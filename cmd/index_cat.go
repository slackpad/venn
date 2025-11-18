package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexCat returns a CommandFactory for listing files in an index.
func IndexCat(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexCat{
			logger: logger,
		}, nil
	}
}

type indexCat struct {
	logger hclog.Logger
}

func (c *indexCat) Synopsis() string {
	return "List files in an index"
}

func (c *indexCat) Help() string {
	return `Usage: venn index cat <indexName>

Display the contents of an index in a table format.

This command shows all files in the specified index, including their SHA-256
hash, size, timestamp, content type, and associated file paths. Files with
the same hash (duplicates) are shown together.

Arguments:
  indexName  Name of the index to display

Example:
  venn index cat photos
`
}

func (c *indexCat) Run(args []string) int {
	if len(args) != 1 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]

	if err := core.IndexCat(c.logger, indexName); err != nil {
		c.logger.Error("failed to display index", "index", indexName, "error", err)
		return 1
	}

	return 0
}
