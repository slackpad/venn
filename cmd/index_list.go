package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexList returns a CommandFactory for listing all indexes.
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
	return "List all indexes"
}

func (c *indexList) Help() string {
	return `Usage: venn index ls

List all indexes in the database.

This command displays the names of all indexes that have been created.

Example:
  venn index ls
`
}

func (c *indexList) Run(args []string) int {
	if len(args) != 0 {
		c.logger.Error("index ls command takes no arguments")
		return cli.RunResultHelp
	}

	if err := core.IndexList(c.logger); err != nil {
		c.logger.Error("failed to list indexes", "error", err)
		return 1
	}

	return 0
}
