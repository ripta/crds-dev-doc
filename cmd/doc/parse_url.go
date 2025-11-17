package main

import (
	"errors"
	"net/url"
	"strings"
)

var ErrInvalidPath = errors.New("invalid path")

// TODO(hasheddan): add testing and more reliable parse
func parseGHURL(uPath string) (org, repo, group, version, kind, tag string, err error) {
	u, err := url.Parse(uPath)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	elements := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(elements) < 6 {
		return "", "", "", "", "", "", ErrInvalidPath
	}

	tagSplit := strings.Split(u.Path, "@")
	if len(tagSplit) > 1 {
		tag = tagSplit[1]
	}

	return elements[1], elements[2], elements[3], elements[4], strings.Split(elements[5], "@")[0], tag, nil
}
