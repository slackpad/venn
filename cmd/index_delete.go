package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexDelete(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexDelete{
			logger: logger,
		}, nil
	}
}

type indexDelete struct {
	logger hclog.Logger
}

func (c *indexDelete) Synopsis() string {
	return "Deletes an index"
}

func (c *indexDelete) Help() string {
	return `
venn index delete <dbPath> <indexName>
	
dbPath:   Path to the database file
indexNme: Name of index to delete`
}

func (c *indexDelete) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	if err := core.IndexDelete(c.logger, dbPath, indexName); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
