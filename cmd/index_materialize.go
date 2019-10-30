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

venn materialize <dbPath> <indexName> <rootPath>
	
dbPath:   Path to the database file
indexNme: Name of index to use
rootPath: Path of the root folder to target`
}

func (c *indexMaterialize) Run(args []string) int {
	if len(args) != 3 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	rootPath := args[2]
	if err := core.Materialize(c.logger, dbPath, indexName, rootPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
