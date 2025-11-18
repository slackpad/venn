package cmd

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// DoInit returns a CommandFactory for initializing venn in the current directory.
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
	return `Usage: venn init

Initialize venn in the current directory by creating a new venn.db file.

This command creates a new database for managing file indexes. If a database
already exists, this command will fail.

Example:
  venn init
`
}

func (c *doInit) Run(args []string) int {
	if len(args) != 0 {
		c.logger.Error("init command takes no arguments")
		return cli.RunResultHelp
	}

	if err := core.CreateDB(c.logger); err != nil {
		c.logger.Error("failed to initialize database", "error", err)
		return 1
	}

	fmt.Println("Venn database initialized successfully")
	return 0
}
