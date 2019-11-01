package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexMaterialize(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexMaterialize{
			logger: logger,
		}, nil
	}
}

type indexMaterialize struct {
	logger hclog.Logger
}

func (c *indexMaterialize) Synopsis() string {
	return "Materializes an index into a folder"
}

func (c *indexMaterialize) Help() string {
	return `
Copies without duplicates all indexed files into the target path.

venn materialize <indexName> <rootPath>
	
indexName: Name of index to use
rootPath:  Path of the root folder to target`
}

func (c *indexMaterialize) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	rootPath := args[1]
	if err := core.Materialize(c.logger, indexName, rootPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
