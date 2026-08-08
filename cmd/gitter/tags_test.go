package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// newTestRepo builds an on-disk repo with one commit per named tag, each
// committed at the given time.
func newTestRepo(t *testing.T, tags map[string]time.Time) *git.Repository {
	t.Helper()

	dir := t.TempDir()
	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("failed to init repo: %v", err)
	}

	w, err := repo.Worktree()
	if err != nil {
		t.Fatalf("failed to get worktree: %v", err)
	}

	// Commit in a stable order, independent of map iteration order.
	names := make([]string, 0, len(tags))
	for name := range tags {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		when := tags[name]

		file := filepath.Join(dir, name+".txt")
		if err := os.WriteFile(file, []byte(name), 0o644); err != nil {
			t.Fatalf("failed to write file for %s: %v", name, err)
		}
		if _, err := w.Add(name + ".txt"); err != nil {
			t.Fatalf("failed to add file for %s: %v", name, err)
		}

		sig := &object.Signature{Name: "Test", Email: "test@example.com", When: when}
		hash, err := w.Commit("commit for "+name, &git.CommitOptions{Author: sig, Committer: sig})
		if err != nil {
			t.Fatalf("failed to commit for %s: %v", name, err)
		}
		if _, err := repo.CreateTag(name, hash, nil); err != nil {
			t.Fatalf("failed to tag %s: %v", name, err)
		}
	}

	return repo
}

func tagNames(tags []tag) []string {
	names := make([]string, 0, len(tags))
	for _, t := range tags {
		names = append(names, t.name)
	}
	return names
}

func TestDiscoverNewTags(t *testing.T) {
	now := time.Now()

	t.Run("returns only tags newer than the cutoff", func(t *testing.T) {
		repo := newTestRepo(t, map[string]time.Time{
			"v1.0.0": now.Add(-72 * time.Hour),
			"v1.1.0": now.Add(-48 * time.Hour),
			"v1.2.0": now.Add(-24 * time.Hour),
		})

		got, err := discoverNewTags(repo, now.Add(-60*time.Hour))
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}

		want := []string{"v1.1.0", "v1.2.0"}
		if !slices.Equal(tagNames(got), want) {
			t.Errorf("discoverNewTags() = %v, want %v", tagNames(got), want)
		}
	})

	// The caller indexes in slice order, so a truncated run still leaves the
	// database's newest tag pointing at the newest commit.
	t.Run("sorts oldest first", func(t *testing.T) {
		repo := newTestRepo(t, map[string]time.Time{
			"v1.0.0": now.Add(-72 * time.Hour),
			"v1.1.0": now.Add(-48 * time.Hour),
			"v1.2.0": now.Add(-24 * time.Hour),
		})

		got, err := discoverNewTags(repo, now.Add(-100*time.Hour))
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}

		want := []string{"v1.0.0", "v1.1.0", "v1.2.0"}
		if !slices.Equal(tagNames(got), want) {
			t.Errorf("discoverNewTags() = %v, want %v", tagNames(got), want)
		}
	})

	t.Run("excludes tags older than maxTagAge", func(t *testing.T) {
		repo := newTestRepo(t, map[string]time.Time{
			"ancient": now.Add(-maxTagAge - 24*time.Hour),
			"recent":  now.Add(-24 * time.Hour),
		})

		got, err := discoverNewTags(repo, now.Add(-10*maxTagAge))
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}

		want := []string{"recent"}
		if !slices.Equal(tagNames(got), want) {
			t.Errorf("discoverNewTags() = %v, want %v", tagNames(got), want)
		}
	})

	t.Run("returns nothing when all tags predate the cutoff", func(t *testing.T) {
		repo := newTestRepo(t, map[string]time.Time{
			"v1.0.0": now.Add(-72 * time.Hour),
		})

		got, err := discoverNewTags(repo, now)
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("discoverNewTags() = %v, want no tags", tagNames(got))
		}
	})

	t.Run("returns nothing for a repo with no tags", func(t *testing.T) {
		repo := newTestRepo(t, nil)

		got, err := discoverNewTags(repo, now.Add(-100*time.Hour))
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("discoverNewTags() = %v, want no tags", tagNames(got))
		}
	})

	t.Run("the cutoff is exclusive", func(t *testing.T) {
		at := now.Add(-24 * time.Hour).Truncate(time.Second)
		repo := newTestRepo(t, map[string]time.Time{"v1.0.0": at})

		got, err := discoverNewTags(repo, at)
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("discoverNewTags() = %v, want no tags for a cutoff equal to the commit time", tagNames(got))
		}
	})

	t.Run("populates hash and timestamp", func(t *testing.T) {
		at := now.Add(-24 * time.Hour).Truncate(time.Second)
		repo := newTestRepo(t, map[string]time.Time{"v1.0.0": at})

		got, err := discoverNewTags(repo, now.Add(-100*time.Hour))
		if err != nil {
			t.Fatalf("discoverNewTags() error = %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("discoverNewTags() returned %d tags, want 1", len(got))
		}

		if got[0].hash.IsZero() {
			t.Error("discoverNewTags() left the tag hash zero")
		}
		if !got[0].timestamp.Equal(at) {
			t.Errorf("discoverNewTags() timestamp = %v, want %v", got[0].timestamp, at)
		}
	})
}
