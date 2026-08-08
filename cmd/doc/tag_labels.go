package main

import (
	"time"

	"github.com/blang/semver/v4"
)

const (
	labelNewest = "newest"
	labelLatest = "latest"
	labelNext   = "next"
)

type tagInfo struct {
	Name       string
	Timestamp  time.Time
	HashSHA1   string
	AliasTagID *int
	DataSize   *int
	IsSemver   bool
	Labels     []string
}

// labelTags fills in IsSemver and Labels for one repository's tags:
// "newest" for every tag on the most recent commit, "latest" for the highest
// release, and "next" for the highest prerelease above that release.
//
// Tags are labelled relative to the slice they arrive in, so callers holding
// several repositories must call this once per repository.
func labelTags(tags []tagInfo) []tagInfo {
	var (
		latestTimestamp time.Time
		latestHash      string

		latestPre     semver.Version
		latestRelease semver.Version
	)

	parsed := make(map[string]semver.Version, len(tags))
	for i := range tags {
		if sv, err := semver.ParseTolerant(tags[i].Name); err == nil {
			tags[i].IsSemver = true
			parsed[tags[i].Name] = sv

			if len(sv.Pre) > 0 && sv.GT(latestPre) {
				latestPre = sv
			}
			if len(sv.Pre) == 0 && sv.GT(latestRelease) {
				latestRelease = sv
			}
		} else {
			tags[i].IsSemver = false
		}

		if tags[i].Timestamp.After(latestTimestamp) {
			latestTimestamp = tags[i].Timestamp
			latestHash = tags[i].HashSHA1
		}
	}

	for i := range tags {
		tags[i].Labels = nil

		if latestHash != "" && tags[i].HashSHA1 == latestHash {
			tags[i].Labels = append(tags[i].Labels, labelNewest)
		}
		if !tags[i].IsSemver {
			continue
		}

		sv := parsed[tags[i].Name]
		switch {
		case latestPre.GT(latestRelease) && sv.Equals(latestPre):
			tags[i].Labels = append(tags[i].Labels, labelNext)
		case sv.Equals(latestRelease):
			tags[i].Labels = append(tags[i].Labels, labelLatest)
		}
	}

	return tags
}
