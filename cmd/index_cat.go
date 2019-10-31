package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexCat(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexCat{
			logger: logger,
		}, nil
	}
}

type indexCat struct {
	logger hclog.Logger
}

func (c *indexCat) Synopsis() string {
	return "Lists files in an index"
}

func (c *indexCat) Help() string {
	return `
venn index cat <dbPath> <indexName>
	
dbPath:    Path to the database file
indexName: Name of index to use`
}

func (c *indexCat) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	if err := core.IndexCat(c.logger, dbPath, indexName); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
