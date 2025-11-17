package main

import (
	"errors"
	"net/url"
	"strings"
)

var ErrInvalidPath = errors.New("invalid path")

func parseGHURL(uPath string) (org, repo, group, version, kind, tag string, err error) {
	u, err := url.Parse(uPath)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	elements := strings.SplitN(strings.Trim(u.Path, "/"), "/", 6)
	if len(elements) != 6 {
		return "", "", "", "", "", "", ErrInvalidPath
	}

	tagSplit := strings.Split(elements[5], "@")
	if len(tagSplit) > 1 {
		tag = tagSplit[1]
	}

	return elements[1], elements[2], elements[3], elements[4], tagSplit[0], tag, nil
}
