package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexStats returns a CommandFactory for displaying statistics about an index.
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
	return "Display statistics about an index"
}

func (c *indexStats) Help() string {
	return `Usage: venn index stats <indexName>

Display statistics about an index including file counts, sizes, and types.

This command shows:
- Number of unique hashes
- Number of files (including duplicates)
- Number of hashes with duplicates
- Total size in bytes
- Distribution of file types

Arguments:
  indexName  Name of the index to analyze

Example:
  venn index stats photos
`
}

func (c *indexStats) Run(args []string) int {
	if len(args) != 1 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]

	if err := core.IndexStats(c.logger, indexName); err != nil {
		c.logger.Error("failed to display index statistics", "index", indexName, "error", err)
		return 1
	}

	return 0
}
