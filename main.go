package main

import (
	"os"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	venncmd "github.com/slackpad/venn/cmd"
)

const (
	appName    = "venn"
	appVersion = "0.0.1"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   appName,
		Level:  hclog.LevelFromString("INFO"),
		Output: os.Stderr,
	})

	c := cli.NewCLI(appName, appVersion)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		// Initialization
		"init": venncmd.DoInit(logger),

		// Index management commands
		"index add-files":                 venncmd.IndexAddFiles(logger),
		"index add-google-photos-takeout": venncmd.IndexAddGooglePhotosTakeout(logger),
		"index cat":                       venncmd.IndexCat(logger),
		"index chunk":                     venncmd.IndexChunk(logger),
		"index ls":                        venncmd.IndexList(logger),
		"index materialize":               venncmd.IndexMaterialize(logger),
		"index rm":                        venncmd.IndexDelete(logger),
		"index stats":                     venncmd.IndexStats(logger),

		// Set operations
		"set difference":   venncmd.SetDifference(logger),
		"set intersection": venncmd.SetIntersection(logger),
		"set union":        venncmd.SetUnion(logger),
	}

	exitStatus, err := c.Run()
	if err != nil {
		logger.Error("command execution failed", "error", err)
	}

	os.Exit(exitStatus)
}
