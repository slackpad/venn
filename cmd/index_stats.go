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
venn index stats <dbPath> <indexName>
	
dbPath:    Path to the database file
indexName: Name of index to use`
}

func (c *indexStats) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	indexName := args[1]
	if err := core.IndexStats(c.logger, dbPath, indexName); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
