package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func SetDifference(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setDifference{
			logger: logger,
		}, nil
	}
}

type setDifference struct {
	logger hclog.Logger
}

func (c *setDifference) Synopsis() string {
	return "Makes a new index as A-B"
}

func (c *setDifference) Help() string {
	return `
This creates a new index with all of the files in B removed from A. It
Doesn't modify A or B.

venn set subtract <dbPath> <indexName> <indexNameA> <indexNameA>
	
dbPath:     Path to the database file
indexName:  Name of index to create with the result
indexNameA: First index
indexNameA: Second index
`
}

func (c *setDifference) Run(args []string) int {
	if len(args) != 4 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	a := args[2]
	b := args[3]
	if err := core.SetDifference(c.logger, dbPath, indexName, a, b); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
