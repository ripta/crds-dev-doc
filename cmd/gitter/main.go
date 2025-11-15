/*
Copyright 2020 The CRDS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/crdsdev/doc/pkg/crd"
	"github.com/crdsdev/doc/pkg/models"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
	"gopkg.in/square/go-jose.v2/json"
	yaml "gopkg.in/yaml.v3"
)

const (
	crdArgCount = 6

	userEnv     = "PG_USER"
	passwordEnv = "PG_PASS"
	hostEnv     = "PG_HOST"
	portEnv     = "PG_PORT"
	dbEnv       = "PG_DB"

	listenAddrEnv     = "GITTER_LISTEN_ADDR"
	defaultListenAddr = ":5002"

	maxFileSize = 500 * 1024 // 500 KB
	maxRuntime  = 1 * time.Minute
	maxTagAge   = 4 * 365 * 24 * time.Hour // 4 years

	minRetryInterval = 24 * time.Hour
	maxErrorLength   = 950

	dryRunEnv = "GITTER_DRY_RUN"
)

var (
	ErrNoTagFound         = errors.New("no tag found in repo")
	ErrIndexingInProgress = errors.New("indexing already in progress")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrRecentFailure      = errors.New("recent failure, retry later")
)

func main() {
	dsn := os.Getenv("CRDS_DEV_STORAGE_DSN")
	if dsn == "" {
		dsn = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", os.Getenv(userEnv), os.Getenv(passwordEnv), os.Getenv(hostEnv), os.Getenv(portEnv), os.Getenv(dbEnv))
	}

	conn, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), conn)
	if err != nil {
		panic(err)
	}
	gitter := &Gitter{
		conn:    pool,
		locks:   sync.Map{},
		limiter: rate.NewLimiter(rate.Every(2*time.Minute), 5),
		dryRun:  os.Getenv(dryRunEnv) == "true",
	}
	rpc.Register(gitter)
	rpc.HandleHTTP()

	listenAddr := defaultListenAddr
	if v, ok := os.LookupEnv(listenAddrEnv); ok && v != "" {
		listenAddr = v
	}

	l, e := net.Listen("tcp", listenAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Println("Starting gitter on", listenAddr)
	log.Println("Limiter set to", gitter.limiter.Limit())
	http.Serve(l, nil)
}

// Gitter indexes git repos.
type Gitter struct {
	conn    *pgxpool.Pool
	locks   sync.Map
	limiter *rate.Limiter
	dryRun  bool
}

type tag struct {
	timestamp time.Time
	hash      plumbing.Hash
	name      string
}

func (g *Gitter) Ping(_ struct{}, reply *string) error {
	*reply = "pong"
	return nil
}

// Index indexes a git repo at the specified url.
//
// This method performs fast validation checks synchronously and returns immediately
// with any validation errors. If validation passes, indexing proceeds asynchronously
// in the background. Subsequent calls to Index for the same repo/tag while indexing
// is in progress will return an error.
func (g *Gitter) Index(gRepo models.GitterRepo, reply *string) error {
	key := fmt.Sprintf("%s/%s@%s", gRepo.Org, gRepo.Repo, gRepo.Tag)
	if _, ok := g.locks.LoadOrStore(key, 1); ok {
		*reply = "indexing already in progress"
		return nil
	}

	ctx := context.Background()
	fullRepo := gRepo.FullName()

	// Check for recent failures
	var recentFailureTime time.Time
	var attemptErrorReason string
	checkErr := g.conn.QueryRow(ctx, "SELECT time, error FROM attempts WHERE repo = $1 AND tag = $2 AND time > $3 LIMIT 1", fullRepo, gRepo.Tag, time.Now().Add(-minRetryInterval)).Scan(&recentFailureTime, &attemptErrorReason)
	if checkErr == nil {
		g.locks.Delete(key)
		*reply = fmt.Sprintf("indexing %s failed: %s", fullRepo, attemptErrorReason)
		return nil
	} else if !errors.Is(checkErr, pgx.ErrNoRows) {
		g.locks.Delete(key)
		*reply = "internal error"
		return nil
	}

	// Check for cumulative failures on the same repo, regardless of tag
	var failureCount int
	checkErr = g.conn.QueryRow(ctx, "SELECT COUNT(*) FROM attempts WHERE repo = $1 AND time > $2", fullRepo, time.Now().Add(-minRetryInterval)).Scan(&failureCount)
	if checkErr != nil {
		g.locks.Delete(key)
		return fmt.Errorf("internal error checking failure count: %w", checkErr)
	}
	if failureCount >= 10 {
		g.locks.Delete(key)
		*reply = fmt.Sprintf("indexing %s has had too many failures, and has been blocked", fullRepo)
		return nil
	}

	if rsv := g.limiter.Reserve(); !rsv.OK() {
		g.locks.Delete(key)
		log.Printf("Burst limit exceeded, cannot index %s at this time", key)
		*reply = "no indexing capacity: burst exceeded"
		return nil
	} else if delay := rsv.Delay(); delay > 0 {
		rsv.Cancel()
		g.locks.Delete(key)
		log.Printf("Rate limit exceeded, need to wait %s before indexing %s", delay.String(), key)
		*reply = "low indexing capacity: try again in a few minutes"
		return nil
	}

	// All validation passed - spawn async indexing and return immediately
	go func() {
		defer g.locks.Delete(key)

		ctx, cancel := context.WithTimeout(context.Background(), maxRuntime)
		defer cancel()

		if err := g.doIndex(ctx, gRepo, fullRepo); err != nil {
			if !g.dryRun {
				truncStr := truncateError(err.Error())
				if _, dbErr := g.conn.Exec(context.Background(), "INSERT INTO attempts(repo, tag, time, error) VALUES ($1, $2, NOW(), $3) ON CONFLICT (repo, tag) DO UPDATE SET time = EXCLUDED.time, error = EXCLUDED.error", fullRepo, gRepo.Tag, truncStr); dbErr != nil {
					log.Printf("Failed to record indexing attempt: %v", dbErr)
				}
			}
			log.Printf("Indexing failed for %s: %v", key, err)
		} else {
			log.Printf("Successfully completed indexing for %s", key)
		}
	}()

	*reply = "indexing started; check back in a few minutes"
	return nil
}

func (g *Gitter) doIndex(ctx context.Context, gRepo models.GitterRepo, fullRepo string) error {
	dir, err := os.MkdirTemp(os.TempDir(), "doc-gitter-*")
	if err != nil {
		return fmt.Errorf("unable to create working directory for git checkout: %w", err)
	}
	defer os.RemoveAll(dir)

	if g.dryRun {
		log.Printf("[DRYRUN] Indexing repo %s/%s@%s into %s\n", gRepo.Org, gRepo.Repo, gRepo.Tag, dir)
	} else {
		log.Printf("Indexing repo %s/%s@%s into %s\n", gRepo.Org, gRepo.Repo, gRepo.Tag, dir)
	}
	defer log.Printf("Finished indexing %s/%s\n", gRepo.Org, gRepo.Repo)

	cloneOpts := &git.CloneOptions{
		URL:               fmt.Sprintf("https://%s", fullRepo),
		Depth:             1,
		Progress:          os.Stdout,
		RecurseSubmodules: git.NoRecurseSubmodules,
	}
	if gRepo.Tag != "" {
		cloneOpts.ReferenceName = plumbing.NewTagReferenceName(gRepo.Tag)
		cloneOpts.SingleBranch = true
	}

	repo, err := git.PlainCloneContext(ctx, dir, false, cloneOpts)
	if err != nil {
		return fmt.Errorf("git clone error: %w", err)
	}
	iter, err := repo.Tags()
	if err != nil {
		return fmt.Errorf("git tags error: %w", err)
	}
	w, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git worktree error: %w", err)
	}

	// Get CRDs for each tag
	tags := []tag{}
	if err := iter.ForEach(func(obj *plumbing.Reference) error {
		// Resolve the commit to get the timestamp
		h, err := repo.ResolveRevision(plumbing.Revision(obj.Hash().String()))
		if err != nil || h == nil {
			log.Printf("Unable to resolve revision for tag %s: %s (%v)", obj.Name().Short(), obj.Hash().String(), err)
			return nil
		}
		c, err := repo.CommitObject(*h)
		if err != nil || c == nil {
			log.Printf("Unable to get commit object for tag %s: %s (%v)", obj.Name().Short(), obj.Hash().String(), err)
			return nil
		}

		if c.Committer.When.Before(time.Now().Add(-1 * maxTagAge)) {
			log.Printf("Skipping tag %s: too old (%s)", obj.Name().Short(), c.Committer.When)
			return nil
		}

		if gRepo.Tag == "" {
			tags = append(tags, tag{
				timestamp: c.Committer.When,
				hash:      obj.Hash(),
				name:      obj.Name().Short(),
			})
			return nil
		}
		if obj.Name().Short() == gRepo.Tag {
			tags = append(tags, tag{
				timestamp: c.Committer.When,
				hash:      obj.Hash(),
				name:      obj.Name().Short(),
			})
			iter.Close()
		}
		return nil
	}); err != nil {
		log.Println("Transient error iterating tags:", err)
	}

	if len(tags) == 0 {
		log.Printf("No tags found for repo %s/%s@%s\n", gRepo.Org, gRepo.Repo, gRepo.Tag)
		return ErrNoTagFound
	}

	log.Printf("Found %d tags for repo %s/%s\n", len(tags), gRepo.Org, gRepo.Repo)
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].timestamp.After(tags[j].timestamp)
	})

	for _, t := range tags {
		h, err := repo.ResolveRevision(plumbing.Revision(t.hash.String()))
		if err != nil || h == nil {
			log.Printf("Unable to resolve revision: %s (%v)", t.hash.String(), err)
			continue
		}
		c, err := repo.CommitObject(*h)
		if err != nil || c == nil {
			log.Printf("Unable to resolve revision: %s (%v)", t.hash.String(), err)
			continue
		}

		// Check if another non-alias tag exists with the same hash
		r := g.conn.QueryRow(ctx, "SELECT id, name, repo FROM tags WHERE hash_sha1 = decode($1, 'hex') AND alias_tag_id IS NULL LIMIT 1", h.String())
		var originalTagID *int
		var originalName, originalRepo string
		var origID int
		if err := r.Scan(&origID, &originalName, &originalRepo); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("error querying tags by hash from database: %w", err)
			}
		} else {
			originalTagID = &origID
		}

		// Check if (name, repo) already exists
		r = g.conn.QueryRow(ctx, "SELECT id, encode(hash_sha1, 'hex'), alias_tag_id FROM tags WHERE name=$1 AND repo=$2", t.name, fullRepo)
		var tagID int
		var existingHash string
		var existingAliasID *int
		var isAlias bool

		if err := r.Scan(&tagID, &existingHash, &existingAliasID); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("error querying tags from database: %w", err)
			}
			if !g.dryRun {
				if originalTagID != nil {
					isAlias = true
					log.Printf("Tag %s@%s is an alias of %s@%s (same hash %s), skipping CRD indexing", t.name, fullRepo, originalName, originalRepo, h.String())
				}
				r := g.conn.QueryRow(ctx, "INSERT INTO tags(name, repo, time, hash_sha1, alias_tag_id) VALUES ($1, $2, $3, decode($4, 'hex'), $5) RETURNING id", t.name, fullRepo, c.Committer.When, h.String(), originalTagID)
				if err := r.Scan(&tagID); err != nil {
					return fmt.Errorf("error inserting tag into database: %w", err)
				}
			} else {
				if originalTagID != nil {
					isAlias = true
					log.Printf("[DRYRUN] Tag %s@%s would be an alias of %s@%s (same hash %s), skipping CRD indexing", t.name, fullRepo, originalName, originalRepo, h.String())
				} else {
					log.Printf("[DRYRUN] Would insert new tag %s@%s with hash %s", t.name, fullRepo, h.String())
				}
			}
		} else {
			if existingHash != h.String() {
				log.Printf("Tag %s@%s already exists with different hash (existing: %s, new: %s), skipping", t.name, fullRepo, existingHash, h.String())
				continue
			}
			if existingAliasID != nil {
				isAlias = true
				log.Printf("Tag %s@%s is already an alias (alias_tag_id: %d), skipping CRD indexing", t.name, fullRepo, *existingAliasID)
			}
		}

		// Skip CRD processing if this is an alias
		if isAlias {
			continue
		}

		repoCRDs, err := getCRDsFromTag(dir, t.name, h, w)
		if err != nil {
			log.Printf("Unable to get CRDs: %s@%s (%v)", repo, t.name, err)
			continue
		}
		if len(repoCRDs) == 0 {
			log.Printf("Skipping tag %s: no CRDs found", t.name)
			continue
		}

		allArgs := make([]interface{}, 0, len(repoCRDs)*crdArgCount)
		for _, crd := range repoCRDs {
			allArgs = append(allArgs, crd.Group, crd.Version, crd.Kind, tagID, crd.Filename, crd.CRD)
		}
		if !g.dryRun {
			if _, err := g.conn.Exec(ctx, buildInsert("INSERT INTO crds(\"group\", version, kind, tag_id, filename, data) VALUES ", crdArgCount, len(repoCRDs))+"ON CONFLICT DO NOTHING", allArgs...); err != nil {
				return fmt.Errorf("error inserting CRDs: %s@%s (%v)", repo, t.name, err)
			}
		}
	}

	return nil
}

func truncateError(errMsg string) string {
	if len(errMsg) <= maxErrorLength {
		return errMsg
	}
	suffix := fmt.Sprintf(" [truncated at %d bytes]", maxErrorLength)
	return errMsg[:maxErrorLength] + suffix
}

var (
	reg     = regexp.MustCompile("kind: CustomResourceDefinition")
	regPath = regexp.MustCompile(`^.*\.yaml`)
)

func getCRDsFromTag(dir string, tag string, hash *plumbing.Hash, w *git.Worktree) (map[string]models.RepoCRD, error) {
	err := w.Checkout(&git.CheckoutOptions{
		Hash:  *hash,
		Force: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error during git checkout: %w", err)
	}
	if err := w.Reset(&git.ResetOptions{
		Mode: git.HardReset,
	}); err != nil {
		return nil, fmt.Errorf("error during git reset: %w", err)
	}

	g, err := w.Grep(&git.GrepOptions{
		Patterns:  []*regexp.Regexp{reg},
		PathSpecs: []*regexp.Regexp{regPath},
	})
	if err != nil {
		return nil, fmt.Errorf("error grepping CRDs: %s@%s (%v)", tag, hash.String(), err)
	}
	fmt.Printf("Found %d CRD YAML files for tag %s\n", len(g), tag)

	repoCRDs := map[string]models.RepoCRD{}
	files := getYAMLs(g, dir)
	for file, yamls := range files {
		for _, y := range yamls {
			crder, err := crd.NewCRDer(y, crd.StripLabels(), crd.StripAnnotations(), crd.StripConversion())
			if err != nil || crder.CRD == nil {
				fmt.Printf("Skipping CRD YAML file %s@%s: error parsing: %v\n", file, hash.String(), err)
				continue
			}

			cbytes, err := json.Marshal(crder.CRD)
			if err != nil {
				fmt.Printf("Skipping CRD YAML file %s@%s: error marshaling: %v\n", file, hash.String(), err)
				continue
			}

			fmt.Printf("Processed CRD %s from file %s@%s\n", crd.PrettyGVK(crder.GVK), file, hash.String())
			repoCRDs[crd.PrettyGVK(crder.GVK)] = models.RepoCRD{
				Path:     crd.PrettyGVK(crder.GVK),
				Filename: path.Base(file),
				Group:    crder.GVK.Group,
				Version:  crder.GVK.Version,
				Kind:     crder.GVK.Kind,
				CRD:      cbytes,
			}
		}
	}

	return repoCRDs, nil
}

func getYAMLs(greps []git.GrepResult, dir string) map[string][][]byte {
	allCRDs := map[string][][]byte{}
	for _, res := range greps {
		fi, err := os.Stat(dir + "/" + res.FileName)
		if err != nil {
			log.Printf("failed to stat CRD file %s: %v", res.FileName, err)
			continue
		}

		if fi.Size() > maxFileSize {
			allCRDs[res.FileName] = [][]byte{}
			log.Printf("skipping CRD file %s: file size %d exceeds limit %d", res.FileName, fi.Size(), maxFileSize)
			continue
		}

		b, err := os.ReadFile(dir + "/" + res.FileName)
		if err != nil {
			log.Printf("failed to read CRD file: %s", res.FileName)
			continue
		}

		yamls, err := splitYAML(b, res.FileName)
		if err != nil {
			log.Printf("failed to split/parse CRD file: %s", res.FileName)
			continue
		}

		allCRDs[res.FileName] = yamls
	}

	return allCRDs
}

func splitYAML(file []byte, filename string) ([][]byte, error) {
	errCount := 0
	docIndex := -1

	var yamls [][]byte
	defer func() {
		if err := recover(); err != nil {
			yamls = make([][]byte, 0)
			err = fmt.Errorf("panic while processing yaml file: %v", err)
		}
	}()

	decoder := yaml.NewDecoder(bytes.NewReader(file))
	for {
		if errCount > 10 {
			return nil, fmt.Errorf("encountered too many errors while processing yaml file: %s", filename)
		}

		node := map[string]any{}
		if err := decoder.Decode(&node); err != nil {
			if err == io.EOF {
				break
			}

			log.Printf("error #%d: failed to decode part of CRD file: %s\n%s", errCount, filename, err)
			errCount++
			continue
		}

		docIndex++
		if v, ok := node["kind"].(string); !ok {
			log.Printf("skipping CRD YAML file %s, document index %d: missing kind field", filename, docIndex)
			continue
		} else if v != "CustomResourceDefinition" {
			log.Printf("skipping CRD YAML file %s, document index %d: kind is %s, expecting CustomResourceDefinition", filename, docIndex, v)
			continue
		}

		doc, err := yaml.Marshal(node)
		if err != nil {
			log.Printf("error #%d: failed to reëncode CRD file %s, document index %d: %v", errCount, filename, docIndex, err)
			errCount++
			continue
		}

		yamls = append(yamls, doc)
	}

	return yamls, nil
}

func buildInsert(query string, argsPerInsert, numInsert int) string {
	absArg := 1
	for i := 0; i < numInsert; i++ {
		query += "("
		for j := 0; j < argsPerInsert; j++ {
			query += "$" + fmt.Sprint(absArg)
			if j != argsPerInsert-1 {
				query += ","
			}
			absArg++
		}
		query += ")"
		if i != numInsert-1 {
			query += ","
		}
	}
	return query
}
