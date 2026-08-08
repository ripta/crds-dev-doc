package main

import (
	"slices"
	"testing"
	"time"
)

// at builds a timestamp offset from a fixed reference point, so ordering does
// not depend on wall-clock time.
func at(hours int) time.Time {
	base := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(hours) * time.Hour)
}

func labelsOf(tags []tagInfo) map[string][]string {
	out := make(map[string][]string, len(tags))
	for _, t := range tags {
		out[t.Name] = t.Labels
	}
	return out
}

func assertLabels(t *testing.T, tags []tagInfo, name string, want ...string) {
	t.Helper()

	got, ok := labelsOf(tags)[name]
	if !ok {
		t.Fatalf("no tag named %q in %v", name, labelsOf(tags))
	}
	if want == nil {
		want = []string{}
	}
	if got == nil {
		got = []string{}
	}
	if !slices.Equal(got, want) {
		t.Errorf("labels for %q = %v, want %v", name, got, want)
	}
}

func TestLabelTags(t *testing.T) {
	t.Run("highest release gets latest, newest commit gets newest", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.2.0", Timestamp: at(3), HashSHA1: "ccc"},
			{Name: "v1.1.0", Timestamp: at(2), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v1.0.0")
		assertLabels(t, got, "v1.1.0")
		assertLabels(t, got, "v1.2.0", labelNewest, labelLatest)
	})

	// "newest" is by commit time, "latest" by version order. Backporting onto
	// an older line separates them.
	t.Run("newest and latest can be different tags", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v2.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.9.1", Timestamp: at(5), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v2.0.0", labelLatest)
		assertLabels(t, got, "v1.9.1", labelNewest)
	})

	t.Run("prerelease above the highest release gets next", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.1.0-rc.1", Timestamp: at(2), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v1.0.0", labelLatest)
		assertLabels(t, got, "v1.1.0-rc.1", labelNewest, labelNext)
	})

	t.Run("prerelease below the highest release gets nothing", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0-rc.1", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.0.0", Timestamp: at(2), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v1.0.0-rc.1")
		assertLabels(t, got, "v1.0.0", labelNewest, labelLatest)
	})

	t.Run("only prereleases means next but no latest", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v0.1.0-alpha.1", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v0.1.0-alpha.2", Timestamp: at(2), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v0.1.0-alpha.1")
		assertLabels(t, got, "v0.1.0-alpha.2", labelNewest, labelNext)
	})

	t.Run("highest prerelease wins among several", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v2.0.0-rc.1", Timestamp: at(3), HashSHA1: "aaa"},
			{Name: "v2.0.0-rc.2", Timestamp: at(1), HashSHA1: "bbb"},
			{Name: "v1.0.0", Timestamp: at(2), HashSHA1: "ccc"},
		})

		assertLabels(t, got, "v2.0.0-rc.1", labelNewest)
		assertLabels(t, got, "v2.0.0-rc.2", labelNext)
		assertLabels(t, got, "v1.0.0", labelLatest)
	})

	t.Run("non-semver tags can still be newest", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "nightly", Timestamp: at(5), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v1.0.0", labelLatest)
		assertLabels(t, got, "nightly", labelNewest)
	})

	t.Run("non-semver tags never get latest or next", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "main", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "release", Timestamp: at(2), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "main")
		assertLabels(t, got, "release", labelNewest)
	})

	t.Run("tolerant parsing accepts unprefixed versions", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "1.2.3", Timestamp: at(1), HashSHA1: "aaa"},
		})

		if !got[0].IsSemver {
			t.Error("IsSemver = false for 1.2.3, want true")
		}
		assertLabels(t, got, "1.2.3", labelNewest, labelLatest)
	})

	// Aliases of one commit all carry "newest".
	t.Run("every tag on the newest commit is labelled", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.2.0", Timestamp: at(3), HashSHA1: "ccc"},
			{Name: "stable", Timestamp: at(3), HashSHA1: "ccc"},
			{Name: "v1.1.0", Timestamp: at(1), HashSHA1: "bbb"},
		})

		assertLabels(t, got, "v1.2.0", labelNewest, labelLatest)
		assertLabels(t, got, "stable", labelNewest)
		assertLabels(t, got, "v1.1.0")
	})

	t.Run("sets IsSemver", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "nightly", Timestamp: at(2), HashSHA1: "bbb"},
		})

		if !got[0].IsSemver {
			t.Error("IsSemver = false for v1.0.0, want true")
		}
		if got[1].IsSemver {
			t.Error("IsSemver = true for nightly, want false")
		}
	})

	t.Run("clears stale IsSemver from the caller", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "nightly", Timestamp: at(1), HashSHA1: "aaa", IsSemver: true},
		})

		if got[0].IsSemver {
			t.Error("IsSemver = true for nightly, want it cleared")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		if got := labelTags(nil); len(got) != 0 {
			t.Errorf("labelTags(nil) = %v, want empty", got)
		}
	})

	t.Run("single tag gets newest and latest", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
		})

		assertLabels(t, got, "v1.0.0", labelNewest, labelLatest)
	})

	t.Run("empty hashes produce no newest label", func(t *testing.T) {
		got := labelTags([]tagInfo{
			{Name: "v1.0.0", Timestamp: at(1)},
			{Name: "v1.1.0", Timestamp: at(2)},
		})

		assertLabels(t, got, "v1.0.0")
		assertLabels(t, got, "v1.1.0", labelLatest)
	})

	t.Run("is idempotent", func(t *testing.T) {
		in := []tagInfo{
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.1.0", Timestamp: at(2), HashSHA1: "bbb"},
		}

		once := labelsOf(labelTags(in))
		twice := labelsOf(labelTags(in))

		for name, want := range once {
			if !slices.Equal(twice[name], want) {
				t.Errorf("labels for %q changed on a second call: %v then %v", name, want, twice[name])
			}
		}
	})
}

// Regression: listGVK computed labels across every repo on the page, so only
// the globally-newest repo could show "newest".
func TestLabelTagsIsPerRepository(t *testing.T) {
	repotags := map[string][]tagInfo{
		"github.com/org/fresh": {
			{Name: "v3.0.0", Timestamp: at(100), HashSHA1: "fff"},
		},
		"github.com/org/stale": {
			{Name: "v1.0.0", Timestamp: at(1), HashSHA1: "aaa"},
			{Name: "v1.1.0", Timestamp: at(2), HashSHA1: "bbb"},
		},
	}

	for repo := range repotags {
		repotags[repo] = labelTags(repotags[repo])
	}

	assertLabels(t, repotags["github.com/org/fresh"], "v3.0.0", labelNewest, labelLatest)
	assertLabels(t, repotags["github.com/org/stale"], "v1.1.0", labelNewest, labelLatest)
	assertLabels(t, repotags["github.com/org/stale"], "v1.0.0")
}
