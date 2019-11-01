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
	return "Makes a new index as A - B"
}

func (c *setDifference) Help() string {
	return `
This creates a new index with all of the files in B removed from A. It
doesn't modify A or B.

venn set difference <indexName> <indexNameA> <indexNameA>
	
indexName:  Name of index to create with the result
indexNameA: First index
indexNameA: Second index
`
}

func (c *setDifference) Run(args []string) int {
	if len(args) != 3 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	a := args[1]
	b := args[2]
	if err := core.SetDifference(c.logger, indexName, a, b); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
