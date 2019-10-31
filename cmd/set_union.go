package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func SetUnion(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setUnion{
			logger: logger,
		}, nil
	}
}

type setUnion struct {
	logger hclog.Logger
}

func (c *setUnion) Synopsis() string {
	return "Makes a new index as A+B"
}

func (c *setUnion) Help() string {
	return `
This creates a new index with all of the files in A and B. It
doesn't modify A or B.

venn set subtract <dbPath> <indexName> <indexNameA> <indexNameA>
	
dbPath:     Path to the database file
indexName:  Name of index to create with the result
indexNameA: First index
indexNameA: Second index
`
}

func (c *setUnion) Run(args []string) int {
	if len(args) != 4 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	a := args[2]
	b := args[3]
	if err := core.SetUnion(c.logger, dbPath, indexName, a, b); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
