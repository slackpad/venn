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
	return "Lists all the indexes in a database"
}

func (c *indexList) Help() string {
	return `
venn index list <dbPath>
	
dbPath:    Path to the database file`
}

func (c *indexList) Run(args []string) int {
	if len(args) != 1 {
		return cli.RunResultHelp
	}
	dbPath := args[0]
	if err := core.IndexList(c.logger, dbPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
