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
	return "Lists all the indexes"
}

func (c *indexList) Help() string {
	return `
venn index ls`
}

func (c *indexList) Run(args []string) int {
	if len(args) != 0 {
		return cli.RunResultHelp
	}
	if err := core.IndexList(c.logger); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
