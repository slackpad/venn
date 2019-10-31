package main

import (
	"os"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	venncmd "github.com/slackpad/venn/cmd"
)

var appName = "venn"
var appVersion = "0.0.1"

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  appName,
		Level: hclog.LevelFromString("INFO"),
	})

	c := cli.NewCLI(appName, appVersion)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"index add-files":   venncmd.IndexAdd(logger),
		"index cat":         venncmd.IndexCat(logger),
		"index delete":      venncmd.IndexDelete(logger),
		"index list":        venncmd.IndexList(logger),
		"index materialize": venncmd.IndexMaterialize(logger),
		"index stats":       venncmd.IndexStats(logger),
		"set difference":    venncmd.SetDifference(logger),
		"set union":         venncmd.SetUnion(logger),
	}

	exitStatus, err := c.Run()
	if err != nil {
		logger.Error(err.Error())
	}

	os.Exit(exitStatus)
}
