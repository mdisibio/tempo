package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/masterminds/semver"
)

var (
	forcePrefix = "v"

	trimPrefixes = []string{
		"release-",
	}

	known = map[string]string{
		"main": "next",
	}
)

func main() {
	tagOrBranchName := os.Args[1]

	if k, ok := known[tagOrBranchName]; ok {
		githubEnvSet("DEST", k)
		return
	}

	for _, p := range trimPrefixes {
		tagOrBranchName = strings.TrimPrefix(tagOrBranchName, p)
	}

	v, err := semver.NewVersion(tagOrBranchName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	githubEnvSet("DEST", fmt.Sprintf("%s%d.%d", forcePrefix, v.Major(), v.Minor()))
}

func githubEnvSet(name, val string) {
	os.WriteFile(os.Getenv("GITHUB_ENV"), []byte(fmt.Sprintf("%s=%s", name, val)), 0600)
	os.Exit(0)
}
