package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexList(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexList{
			logger: logger,
		}, nil
	}
}

type indexList struct {
	logger hclog.Logger
}

func (c *indexList) Synopsis() string {
	return "Lists files in an index"
}

func (c *indexList) Help() string {
	return `
venn index list <dbPath> <indexName>
	
dbPath:   Path to the database file
indexNme: Name of index to use`
}

func (c *indexList) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	if err := core.IndexList(c.logger, dbPath, indexName); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
