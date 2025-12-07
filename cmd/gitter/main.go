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
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"regexp"
	"sort"
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
	envDevelopment = "IS_DEV"

	crdArgCount = 6

	listenAddrEnv     = "GITTER_LISTEN_ADDR"
	defaultListenAddr = ":5002"

	persistReposEnv    = "GITTER_PERSIST_REPOS"
	persistPathEnv     = "GITTER_PERSIST_PATH"
	defaultPersistPath = "/tmp/gitter-repos"

	maxFileSize = 500 * 1024 // 500 KB
	maxRuntime  = 2 * time.Minute
	maxTagAge   = 4 * 365 * 24 * time.Hour // 4 years

	maxCRDsPerTag = 300

	minRetryInterval = 24 * time.Hour
	maxErrorLength   = 950

	dryRunEnv = "GITTER_DRY_RUN"
)

var ErrNoTagFound = errors.New("no tag found in repo")

var logger *slog.Logger

func parseLogLevel() slog.Level {
	level := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func main() {
	logLevel := parseLogLevel()
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))

	dsn := os.Getenv("CRDS_DEV_STORAGE_DSN")
	conn, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Error("failed to parse database config", "err", err)
		os.Exit(1)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), conn)
	if err != nil {
		logger.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}

	limit := rate.Every(2 * time.Minute)
	if os.Getenv(envDevelopment) == "true" {
		limit = rate.Inf
	}

	persistRepos := os.Getenv(persistReposEnv) == "true"
	persistPath := getPersistPath()
	if persistRepos {
		if err := os.MkdirAll(persistPath, 0755); err != nil {
			logger.Error("failed to create persist path", "path", persistPath, "err", err)
			os.Exit(1)
		}
		logger.Info("repository persistence enabled", "path", persistPath)
	}

	gitter := &Gitter{
		conn:             pool,
		locks:            sync.Map{},
		limiter:          rate.NewLimiter(limit, 5),
		dryRun:           os.Getenv(dryRunEnv) == "true",
		persistRepos:     persistRepos,
		persistPath:      persistPath,
		catchUpSemaphore: make(chan struct{}, 2),
	}
	rpc.Register(gitter)
	rpc.HandleHTTP()

	listenAddr := defaultListenAddr
	if v, ok := os.LookupEnv(listenAddrEnv); ok && v != "" {
		listenAddr = v
	}

	l, e := net.Listen("tcp", listenAddr)
	if e != nil {
		logger.Error("failed to listen", "addr", listenAddr, "err", e)
		os.Exit(1)
	}

	logger.Info("starting gitter", "addr", listenAddr, "rate_limit", gitter.limiter.Limit())
	http.Serve(l, nil)
}

func getPersistPath() string {
	if p := os.Getenv(persistPathEnv); p != "" {
		return p
	}
	return defaultPersistPath
}

// Gitter indexes git repos.
type Gitter struct {
	conn    *pgxpool.Pool
	locks   sync.Map
	limiter *rate.Limiter
	dryRun  bool
	// persistRepos indicates whether to persist cloned git repos on disk for
	// debugging, easier development, and faster repeated indexing.
	persistRepos bool
	// persistPath is the base path for persisting repos on disk.
	persistPath string
	// catchUpSemaphore limits concurrent repo processing in CatchUp RPC
	catchUpSemaphore chan struct{}
}

type tag struct {
	timestamp time.Time
	hash      plumbing.Hash
	name      string
}

// repoExists checks if .git is a directory repository at the given path.
func repoExists(dir string) bool {
	gitDir := path.Join(dir, ".git")
	info, err := os.Stat(gitDir)
	if err != nil {
		return false
	}

	return info.IsDir()
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
	key := fmt.Sprintf("github.com/%s/%s", gRepo.Org, gRepo.Repo)
	if _, ok := g.locks.LoadOrStore(key, 1); ok {
		*reply = fmt.Sprintf("indexing %q already in progress", key)
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
		logger.Warn("burst limit exceeded", "repo", fullRepo, "tag", gRepo.Tag)
		*reply = "no indexing capacity: burst exceeded"
		return nil
	} else if delay := rsv.Delay(); delay > 0 {
		rsv.Cancel()
		g.locks.Delete(key)
		logger.Warn("rate limit exceeded", "repo", fullRepo, "tag", gRepo.Tag, "delay", delay.String())
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
					logger.Error("failed to record indexing attempt", "repo", fullRepo, "tag", gRepo.Tag, "err", dbErr)
				}
			}
			logger.Error("indexing failed", "repo", fullRepo, "tag", gRepo.Tag, "err", err)
		} else {
			logger.Info("indexing completed", "repo", fullRepo, "tag", gRepo.Tag)
		}
	}()

	*reply = "indexing started; check back in a few minutes"
	return nil
}

type repoCatchupInfo struct {
	name          string
	newestTag     string
	newestTagTime time.Time
}

type repoCatchupResult struct {
	repo    string
	newTags int
	err     error
}

// CatchUp catches up on new tags for each repo in the database, indexing new tags found.
func (g *Gitter) CatchUp(_ struct{}, reply *string) error {
	ctx := context.Background()

	rows, err := g.conn.Query(ctx, `
		SELECT
			repo,
			MAX(time) as latest_tag_time,
			(SELECT name FROM tags t2 WHERE t2.repo = t1.repo ORDER BY time DESC LIMIT 1) as latest_tag_name
		FROM tags t1
		WHERE alias_tag_id IS NULL
		GROUP BY repo
		ORDER BY repo
	`)
	if err != nil {
		*reply = "failed to query repositories"
		return fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	repos := []repoCatchupInfo{}
	for rows.Next() {
		r := repoCatchupInfo{}
		if err := rows.Scan(&r.name, &r.newestTagTime, &r.newestTag); err != nil {
			logger.Error("failed to scan repo", "err", err)
			continue
		}

		repos = append(repos, r)
	}

	logger.Info("starting catch-up", "repo_count", len(repos))

	wg := sync.WaitGroup{}

	results := make(chan repoCatchupResult, len(repos))
	for _, r := range repos {
		wg.Add(1)
		go func(ri repoCatchupInfo) {
			defer wg.Done()

			g.catchUpSemaphore <- struct{}{}
			defer func() { <-g.catchUpSemaphore }()

			if _, loaded := g.locks.LoadOrStore(ri.name, 1); loaded {
				logger.Info("skipping repo: already being indexed", "repo", ri.name)
				results <- repoCatchupResult{repo: ri.name, newTags: 0, err: fmt.Errorf("already indexing")}
				return
			}
			defer g.locks.Delete(ri.name)

			ctx, cancel := context.WithTimeout(context.Background(), maxRuntime)
			defer cancel()

			newTags, err := g.catchUpRepo(ctx, ri.name, ri.newestTag, ri.newestTagTime)
			results <- repoCatchupResult{repo: ri.name, newTags: newTags, err: err}
		}(r)
	}

	wg.Wait()
	close(results)

	totalRepos := 0
	totalNewTags := 0
	totalErrors := 0
	for r := range results {
		totalRepos++
		totalNewTags += r.newTags
		if r.err != nil {
			totalErrors++
			logger.Error("catch-up failed for repo", "repo", r.repo, "err", r.err)
		} else if r.newTags > 0 {
			logger.Info("catch-up succeeded", "repo", r.repo, "new_tags", r.newTags)
		}
	}

	*reply = fmt.Sprintf("Catch-up completed: %d repos processed, %d new tags indexed, %d errors", totalRepos, totalNewTags, totalErrors)
	return nil
}

func (g *Gitter) doIndex(ctx context.Context, gRepo models.GitterRepo, fullRepo string) error {
	var dir string
	var repo *git.Repository
	var err error

	if g.persistRepos {
		dir = path.Join(g.persistPath, "github.com", strings.ToLower(gRepo.Org), strings.ToLower(gRepo.Repo))
		if repoExists(dir) {
			repo, err = git.PlainOpen(dir)
			if err != nil {
				return fmt.Errorf("unable to open existing repository: %w", err)
			}

			logger.Info("opened existing repository", "org", gRepo.Org, "repo", gRepo.Repo, "dir", dir)

			fetchOpts := &git.FetchOptions{
				Tags:     git.AllTags,
				Progress: os.Stdout,
			}

			if err := repo.FetchContext(ctx, fetchOpts); err != nil && err != git.NoErrAlreadyUpToDate {
				return fmt.Errorf("git fetch error: %w", err)
			}
			logger.Info("fetched latest tags", "org", gRepo.Org, "repo", gRepo.Repo)
		} else {
			parentDir := path.Dir(dir)
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return fmt.Errorf("unable to create parent directory: %w", err)
			}
		}
	} else {
		dir, err = os.MkdirTemp(os.TempDir(), "doc-gitter-*")
		if err != nil {
			return fmt.Errorf("unable to create working directory for git checkout: %w", err)
		}
		defer os.RemoveAll(dir)
	}

	logger.Info("indexing repo", "org", gRepo.Org, "repo", gRepo.Repo, "tag", gRepo.Tag, "dir", dir, "dry_run", g.dryRun)
	defer logger.Info("finished indexing", "org", gRepo.Org, "repo", gRepo.Repo)

	if repo == nil {
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

		repo, err = git.PlainCloneContext(ctx, dir, false, cloneOpts)
		if err != nil {
			return fmt.Errorf("git clone error: %w", err)
		}
		logger.Info("cloned new repository", "org", gRepo.Org, "repo", gRepo.Repo, "dir", dir)
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
			logger.Warn("unable to resolve revision for tag", "tag", obj.Name().Short(), "hash", obj.Hash().String(), "err", err)
			return nil
		}
		c, err := repo.CommitObject(*h)
		if err != nil || c == nil {
			logger.Warn("unable to get commit object for tag", "tag", obj.Name().Short(), "hash", obj.Hash().String(), "err", err)
			return nil
		}

		if c.Committer.When.Before(time.Now().Add(-1 * maxTagAge)) {
			logger.Warn("skipping tag: too old", "tag", obj.Name().Short(), "commit_time", c.Committer.When)
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
		logger.Warn("transient error iterating tags", "err", err)
	}

	if len(tags) == 0 {
		logger.Warn("no tags found for repo", "org", gRepo.Org, "repo", gRepo.Repo, "tag", gRepo.Tag)
		return ErrNoTagFound
	}

	logger.Info("found tags for repo", "org", gRepo.Org, "repo", gRepo.Repo, "count", len(tags))
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].timestamp.After(tags[j].timestamp)
	})

	for _, t := range tags {
		if err := g.indexSingleTag(ctx, dir, t, repo, w, fullRepo); err != nil {
			logger.Error("unable to index tag", "tag", t.name, "hash", t.hash.String(), "err", err)
			continue
		}
	}

	return nil
}

// indexSingleTag indexes a single tag by extracting CRDs and inserting into database.
func (g *Gitter) indexSingleTag(ctx context.Context, dir string, t tag, repo *git.Repository, w *git.Worktree, fullRepo string) error {
	h, err := repo.ResolveRevision(plumbing.Revision(t.hash.String()))
	if err != nil || h == nil {
		return fmt.Errorf("unable to resolve revision: %w", err)
	}

	c, err := repo.CommitObject(*h)
	if err != nil || c == nil {
		return fmt.Errorf("unable to get commit object: %w", err)
	}

	// Check if another non-alias tag exists with the same hash
	r := g.conn.QueryRow(ctx, "SELECT id, name, repo FROM tags WHERE hash_sha1 = decode($1, 'hex') AND alias_tag_id IS NULL LIMIT 1", h.String())
	var originalTagID *int
	var originalName, originalRepo string
	var origID int
	if err := r.Scan(&origID, &originalName, &originalRepo); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("error querying tags by hash: %w", err)
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
			return fmt.Errorf("error querying tags: %w", err)
		}
		if !g.dryRun {
			if originalTagID != nil {
				isAlias = true
				logger.Info("tag is an alias, skipping CRD indexing", "tag", t.name, "repo", fullRepo, "original_tag", originalName, "original_repo", originalRepo, "hash", h.String())
			}
			r := g.conn.QueryRow(ctx, "INSERT INTO tags(name, repo, time, hash_sha1, alias_tag_id) VALUES ($1, $2, $3, decode($4, 'hex'), $5) RETURNING id", t.name, fullRepo, c.Committer.When, h.String(), originalTagID)
			if err := r.Scan(&tagID); err != nil {
				return fmt.Errorf("error inserting tag: %w", err)
			}
		} else {
			if originalTagID != nil {
				isAlias = true
				logger.Info("tag would be an alias, skipping CRD indexing", "tag", t.name, "repo", fullRepo, "dry_run", true)
			}
		}
	} else {
		if existingHash != h.String() {
			logger.Warn("tag already exists with different hash, skipping", "tag", t.name, "repo", fullRepo)
			return nil
		}
		if existingAliasID != nil {
			isAlias = true
			logger.Info("tag is already an alias, skipping CRD indexing", "tag", t.name, "repo", fullRepo)
		}
	}

	// Skip CRD processing if this is an alias
	if isAlias {
		return nil
	}

	repoCRDs, err := getCRDsFromTag(dir, t.name, h, w)
	if err != nil {
		return fmt.Errorf("unable to get CRDs: %w", err)
	}
	if len(repoCRDs) == 0 {
		logger.Info("skipping tag: no CRDs found", "tag", t.name)
		return nil
	}
	if len(repoCRDs) > maxCRDsPerTag {
		return fmt.Errorf("too many CRDs found in tag %s: %d (limit %d)", t.name, len(repoCRDs), maxCRDsPerTag)
	}

	allArgs := make([]interface{}, 0, len(repoCRDs)*crdArgCount)
	for _, crd := range repoCRDs {
		allArgs = append(allArgs, crd.Group, crd.Version, crd.Kind, tagID, crd.Filename, crd.CRD)
	}

	logger.Info("found CRDs for tag", "tag", t.name, "count", len(repoCRDs))
	if !g.dryRun {
		if _, err := g.conn.Exec(ctx, buildInsert("INSERT INTO crds(\"group\", version, kind, tag_id, filename, data) VALUES ", crdArgCount, len(repoCRDs))+"ON CONFLICT DO NOTHING", allArgs...); err != nil {
			return fmt.Errorf("error inserting CRDs: %w", err)
		}
	}

	return nil
}

// catchUpRepo processes a single repository to discover and index new tags.
// Returns stats about new tags indexed and any error encountered.
func (g *Gitter) catchUpRepo(ctx context.Context, repoName string, newestIndexedTag string, newestIndexedTime time.Time) (int, error) {
	// Parse repo name: github.com/org/repo
	parts := strings.Split(strings.TrimPrefix(repoName, "github.com/"), "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid repo format: %s", repoName)
	}
	org, repoShort := parts[0], parts[1]

	// Open or clone repository
	var dir string
	var gitRepo *git.Repository
	var err error

	if g.persistRepos {
		dir = path.Join(g.persistPath, "github.com", strings.ToLower(org), strings.ToLower(repoShort))
		if repoExists(dir) {
			gitRepo, err = git.PlainOpen(dir)
			if err != nil {
				return 0, fmt.Errorf("unable to open existing repository: %w", err)
			}

			// Fetch latest tags
			fetchOpts := &git.FetchOptions{
				Tags:     git.AllTags,
				Progress: os.Stdout,
			}
			if err := gitRepo.FetchContext(ctx, fetchOpts); err != nil && err != git.NoErrAlreadyUpToDate {
				return 0, fmt.Errorf("git fetch error: %w", err)
			}
		} else {
			// Clone if doesn't exist
			parentDir := path.Dir(dir)
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return 0, fmt.Errorf("unable to create parent directory: %w", err)
			}

			cloneOpts := &git.CloneOptions{
				URL:               fmt.Sprintf("https://%s", repoName),
				Depth:             1,
				Progress:          os.Stdout,
				RecurseSubmodules: git.NoRecurseSubmodules,
			}
			gitRepo, err = git.PlainCloneContext(ctx, dir, false, cloneOpts)
			if err != nil {
				return 0, fmt.Errorf("git clone error: %w", err)
			}
		}
	} else {
		// Use temp directory
		dir, err = os.MkdirTemp(os.TempDir(), "doc-gitter-catchup-*")
		if err != nil {
			return 0, fmt.Errorf("unable to create working directory: %w", err)
		}
		defer os.RemoveAll(dir)

		cloneOpts := &git.CloneOptions{
			URL:               fmt.Sprintf("https://%s", repoName),
			Depth:             1,
			Progress:          os.Stdout,
			RecurseSubmodules: git.NoRecurseSubmodules,
		}
		gitRepo, err = git.PlainCloneContext(ctx, dir, false, cloneOpts)
		if err != nil {
			return 0, fmt.Errorf("git clone error: %w", err)
		}
	}

	// Discover tags newer than the last indexed tag
	newTags, err := discoverNewTags(gitRepo, newestIndexedTime)
	if err != nil {
		return 0, fmt.Errorf("error discovering tags: %w", err)
	}

	if len(newTags) == 0 {
		logger.Info("no new tags found", "repo", repoName)
		return 0, nil
	}

	logger.Info("found new tags", "repo", repoName, "count", len(newTags))

	// Get worktree
	w, err := gitRepo.Worktree()
	if err != nil {
		return 0, fmt.Errorf("git worktree error: %w", err)
	}

	// Index each new tag
	successCount := 0
	for _, t := range newTags {
		if err := g.indexSingleTag(ctx, dir, t, gitRepo, w, repoName); err != nil {
			logger.Error("failed to index tag", "repo", repoName, "tag", t.name, "err", err)

			// Record failure in attempts table
			if !g.dryRun {
				truncStr := truncateError(err.Error())
				if _, dbErr := g.conn.Exec(context.Background(),
					"INSERT INTO attempts(repo, tag, time, error) VALUES ($1, $2, NOW(), $3) ON CONFLICT (repo, tag) DO UPDATE SET time = EXCLUDED.time, error = EXCLUDED.error",
					repoName, t.name, truncStr); dbErr != nil {
					logger.Error("failed to record indexing attempt", "repo", repoName, "tag", t.name, "err", dbErr)
				}
			}
			// Continue processing other tags
			continue
		}
		successCount++
	}

	return successCount, nil
}

func truncateError(errMsg string) string {
	if len(errMsg) <= maxErrorLength {
		return errMsg
	}
	suffix := fmt.Sprintf(" [truncated at %d bytes]", maxErrorLength)
	return errMsg[:maxErrorLength] + suffix
}

// discoverNewTags returns all tags in the repo newer than the given timestamp,
// sorted chronologically (oldest first) for indexing in order.
func discoverNewTags(repo *git.Repository, afterTime time.Time) ([]tag, error) {
	iter, err := repo.Tags()
	if err != nil {
		return nil, fmt.Errorf("git tags error: %w", err)
	}
	defer iter.Close()

	var newTags []tag
	if err := iter.ForEach(func(obj *plumbing.Reference) error {
		h, err := repo.ResolveRevision(plumbing.Revision(obj.Hash().String()))
		if err != nil || h == nil {
			return nil
		}
		c, err := repo.CommitObject(*h)
		if err != nil || c == nil {
			return nil
		}

		if c.Committer.When.After(afterTime) && c.Committer.When.After(time.Now().Add(-1*maxTagAge)) {
			newTags = append(newTags, tag{
				timestamp: c.Committer.When,
				hash:      obj.Hash(),
				name:      obj.Name().Short(),
			})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(newTags, func(i, j int) bool {
		return newTags[i].timestamp.Before(newTags[j].timestamp)
	})

	return newTags, nil
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
	logger.Debug("found CRD YAML files for tag", "tag", tag, "count", len(g))

	repoCRDs := map[string]models.RepoCRD{}
	files := getYAMLs(g, dir)
	for file, yamls := range files {
		for _, y := range yamls {
			crder, err := crd.NewCRDer(y, crd.StripLabels(), crd.StripAnnotations(), crd.StripConversion())
			if err != nil || crder.CRD == nil {
				logger.Debug("skipping CRD YAML file: error parsing", "file", file, "hash", hash.String(), "err", err)
				continue
			}

			cbytes, err := json.Marshal(crder.CRD)
			if err != nil {
				logger.Debug("skipping CRD YAML file: error marshaling", "file", file, "hash", hash.String(), "err", err)
				continue
			}

			logger.Debug("processed CRD", "gvk", crd.PrettyGVK(crder.GVK), "file", file, "hash", hash.String())
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
			logger.Warn("failed to stat CRD file", "file", res.FileName, "err", err)
			continue
		}

		if fi.Size() > maxFileSize {
			allCRDs[res.FileName] = [][]byte{}
			logger.Warn("skipping CRD file: file size exceeds limit", "file", res.FileName, "size", fi.Size(), "limit", maxFileSize)
			continue
		}

		b, err := os.ReadFile(dir + "/" + res.FileName)
		if err != nil {
			logger.Warn("failed to read CRD file", "file", res.FileName, "err", err)
			continue
		}

		yamls, err := splitYAML(b, res.FileName)
		if err != nil {
			logger.Warn("failed to split/parse CRD file", "file", res.FileName, "err", err)
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

			logger.Warn("failed to decode part of CRD file", "file", filename, "error_count", errCount, "err", err)
			errCount++
			continue
		}

		docIndex++
		if v, ok := node["kind"].(string); !ok {
			logger.Warn("skipping CRD YAML file: missing kind field", "file", filename, "doc_index", docIndex)
			continue
		} else if v != "CustomResourceDefinition" {
			logger.Warn("skipping CRD YAML file: unexpected kind", "file", filename, "doc_index", docIndex, "kind", v)
			continue
		}

		doc, err := yaml.Marshal(node)
		if err != nil {
			logger.Warn("failed to reëncode CRD file", "file", filename, "doc_index", docIndex, "error_count", errCount, "err", err)
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
