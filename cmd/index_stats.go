package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func IndexStats(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexStats{
			logger: logger,
		}, nil
	}
}

type indexStats struct {
	logger hclog.Logger
}

func (c *indexStats) Synopsis() string {
	return "Displays stats about an index"
}

func (c *indexStats) Help() string {
	return `
venn index stats <indexName>
	
indexName: Name of index to use`
}

func (c *indexStats) Run(args []string) int {
	if len(args) != 1 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	if err := core.IndexStats(c.logger, indexName); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
