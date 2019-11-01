package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

func DoInit(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &doInit{
			logger: logger,
		}, nil
	}
}

type doInit struct {
	logger hclog.Logger
}

func (c *doInit) Synopsis() string {
	return "Initialize venn in the current directory"
}

func (c *doInit) Help() string {
	return `
venn init`
}

func (c *doInit) Run(args []string) int {
	if len(args) != 0 {
		return cli.RunResultHelp
	}
	if err := core.CreateDB(c.logger); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
