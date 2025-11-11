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
	"strings"
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

	maxFileSize = 200 * 1024 // 200 KB
	maxRuntime  = 1 * time.Minute
	maxTagAge   = 4 * 365 * 24 * time.Hour // 4 years
)

var (
	ErrNoTagFound         = errors.New("no tag found")
	ErrIndexingInProgress = errors.New("indexing already in progress")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
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
}

type tag struct {
	timestamp time.Time
	hash      plumbing.Hash
	name      string
}

// Index indexes a git repo at the specified url.
func (g *Gitter) Index(gRepo models.GitterRepo, reply *string) error {
	key := fmt.Sprintf("%s/%s@%s", gRepo.Org, gRepo.Repo, gRepo.Tag)
	if _, ok := g.locks.LoadOrStore(key, 1); ok {
		return fmt.Errorf("%w: %s", ErrIndexingInProgress, key)
	}
	defer g.locks.Delete(key)

	if rsv := g.limiter.Reserve(); !rsv.OK() {
		return fmt.Errorf("%w: burst exceeded", ErrRateLimitExceeded)
	} else if delay := rsv.Delay(); delay > 0 {
		rsv.Cancel()
		return fmt.Errorf("%w: please wait %s", ErrRateLimitExceeded, delay)
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxRuntime)
	defer cancel()

	dir, err := os.MkdirTemp(os.TempDir(), "doc-gitter-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	log.Printf("Indexing repo %s/%s@%s into %s\n", gRepo.Org, gRepo.Repo, gRepo.Tag, dir)
	defer log.Printf("Finished indexing %s/%s\n", gRepo.Org, gRepo.Repo)

	fullRepo := fmt.Sprintf("%s/%s/%s", "github.com", strings.ToLower(gRepo.Org), strings.ToLower(gRepo.Repo))
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
		if gRepo.Tag == "" {
			tags = append(tags, tag{
				hash: obj.Hash(),
				name: obj.Name().Short(),
			})
			return nil
		}
		if obj.Name().Short() == gRepo.Tag {
			tags = append(tags, tag{
				hash: obj.Hash(),
				name: obj.Name().Short(),
			})
			iter.Close()
		}
		return nil
	}); err != nil {
		log.Println("Error iterating tags:", err)
	}

	if len(tags) == 0 {
		log.Printf("No tags found for repo %s/%s@%s\n", gRepo.Org, gRepo.Repo, gRepo.Tag)
		return fmt.Errorf("repo %s/%s@%s: %w", gRepo.Org, gRepo.Repo, gRepo.Tag, ErrNoTagFound)
	}

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
		if c.Committer.When.Before(time.Now().Add(-1 * maxTagAge)) {
			log.Printf("Skipping tag %s: too old (%s)", t.name, c.Committer.When)
			continue
		}
		r := g.conn.QueryRow(ctx, "SELECT id FROM tags WHERE name=$1 AND repo=$2", t.name, fullRepo)
		var tagID int
		if err := r.Scan(&tagID); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("error querying tags from database: %w", err)
			}
			r := g.conn.QueryRow(ctx, "INSERT INTO tags(name, repo, time) VALUES ($1, $2, $3) RETURNING id", t.name, fullRepo, c.Committer.When)
			if err := r.Scan(&tagID); err != nil {
				return fmt.Errorf("error inserting tag into database: %w", err)
			}
		}
		repoCRDs, err := getCRDsFromTag(dir, t.name, h, w)
		if err != nil {
			log.Printf("Unable to get CRDs: %s@%s (%v)", repo, t.name, err)
			continue
		}
		if len(repoCRDs) > 0 {
			allArgs := make([]interface{}, 0, len(repoCRDs)*crdArgCount)
			for _, crd := range repoCRDs {
				allArgs = append(allArgs, crd.Group, crd.Version, crd.Kind, tagID, crd.Filename, crd.CRD)
			}
			if _, err := g.conn.Exec(ctx, buildInsert("INSERT INTO crds(\"group\", version, kind, tag_id, filename, data) VALUES ", crdArgCount, len(repoCRDs))+"ON CONFLICT DO NOTHING", allArgs...); err != nil {
				return fmt.Errorf("error inserting CRDs: %s@%s (%v)", repo, t.name, err)
			}
		}
	}

	return nil
}

func getCRDsFromTag(dir string, tag string, hash *plumbing.Hash, w *git.Worktree) (map[string]models.RepoCRD, error) {
	err := w.Checkout(&git.CheckoutOptions{
		Hash:  *hash,
		Force: true,
	})
	if err != nil {
		return nil, err
	}
	if err := w.Reset(&git.ResetOptions{
		Mode: git.HardReset,
	}); err != nil {
		return nil, err
	}
	reg := regexp.MustCompile("kind: CustomResourceDefinition")
	regPath := regexp.MustCompile(`^.*\.yaml`)
	g, _ := w.Grep(&git.GrepOptions{
		Patterns:  []*regexp.Regexp{reg},
		PathSpecs: []*regexp.Regexp{regPath},
	})
	repoCRDs := map[string]models.RepoCRD{}
	files := getYAMLs(g, dir)
	for file, yamls := range files {
		for _, y := range yamls {
			crder, err := crd.NewCRDer(y, crd.StripLabels(), crd.StripAnnotations(), crd.StripConversion())
			if err != nil || crder.CRD == nil {
				continue
			}
			cbytes, err := json.Marshal(crder.CRD)
			if err != nil {
				continue
			}
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

	var yamls [][]byte
	defer func() {
		if err := recover(); err != nil {
			yamls = make([][]byte, 0)
			err = fmt.Errorf("panic while processing yaml file: %w", err)
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

		doc, err := yaml.Marshal(node)
		if err != nil {
			log.Printf("error #%d: failed to encode part of CRD file: %s\n%s", errCount, filename, err)
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
