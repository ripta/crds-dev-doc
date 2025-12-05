package main

import (
	"errors"
	"runtime/debug"
)

func appVersion() (string, string, error) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", "", errors.New("version information not available")
	}

	version := bi.Main.Version
	dirty := false
	if version == "(devel)" {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" {
				version = s.Value
			}
			if s.Key == "vcs.modified" && s.Value == "true" {
				dirty = true
			}
		}
	}

	if dirty {
		version = version + "-dirty"
	}

	return bi.Path, version, nil
}
