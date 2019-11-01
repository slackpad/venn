package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func SetIntersection(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setIntersection{
			logger: logger,
		}, nil
	}
}

type setIntersection struct {
	logger hclog.Logger
}

func (c *setIntersection) Synopsis() string {
	return "Makes a new index as A ∩ B"
}

func (c *setIntersection) Help() string {
	return `
This creates a new index with all of the files in both A and
B. It doesn't modify A or B.

venn set intersection <indexName> <indexNameA> <indexNameA>
	
indexName:  Name of index to create with the result
indexNameA: First index
indexNameA: Second index
`
}

func (c *setIntersection) Run(args []string) int {
	if len(args) != 3 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	a := args[1]
	b := args[2]
	if err := core.SetIntersection(c.logger, indexName, a, b); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
