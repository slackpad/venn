package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexAddFiles(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexAddFiles{
			logger: logger,
		}, nil
	}
}

type indexAddFiles struct {
	logger hclog.Logger
}

func (c *indexAddFiles) Synopsis() string {
	return "Adds files to an index"
}

func (c *indexAddFiles) Help() string {
	return `
Recursively scans all of the files in a folder tree and indexes them. The
index will be created if it doesn't exist, or if it does exist then new
files will be added to it.

venn index add-files <indexName> <rootPath>
	
indexName: Name of index to use
rootPath:  Path of the root folder to scan`
}

func (c *indexAddFiles) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	rootPath := args[1]
	if err := core.IndexAdd(c.logger, indexName, rootPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
