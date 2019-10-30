package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexAdd(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexAdd{
			logger: logger,
		}, nil
	}
}

type indexAdd struct {
	logger hclog.Logger
}

func (c *indexAdd) Synopsis() string {
	return "Adds files to an index"
}

func (c *indexAdd) Help() string {
	return `
Recursively scans all of the files in a folder tree and indexes them. The
index will be created if it doesn't exist, or if it does exist then new
files will be added to it.

venn index add <dbPath> <indexName> <rootPath>
	
dbPath:    Path to the database file (will create or append)
indexName: Name of index to use
rootPath:  Path of the root folder to scan`
}

func (c *indexAdd) Run(args []string) int {
	if len(args) != 3 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	rootPath := args[2]
	if err := core.IndexAdd(c.logger, dbPath, indexName, rootPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
