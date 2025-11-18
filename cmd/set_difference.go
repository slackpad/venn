package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// SetDifference returns a CommandFactory for computing set difference.
func SetDifference(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setDifference{
			logger: logger,
		}, nil
	}
}

type setDifference struct {
	logger hclog.Logger
}

func (c *setDifference) Synopsis() string {
	return "Create a new index as A - B"
}

func (c *setDifference) Help() string {
	return `Usage: venn set difference <indexName> <indexNameA> <indexNameB>

Create a new index containing all files in A that are not in B.

This performs a set difference operation, creating a new index with all entries
from index A except those that also appear in index B. The operation is based
on file content (SHA-256 hash), not file paths. Original indexes A and B are
not modified.

Arguments:
  indexName   Name of the new index to create with the result
  indexNameA  First index (files to include)
  indexNameB  Second index (files to exclude)

Example:
  venn set difference cleaned_photos all_photos bad_photos
`
}

func (c *setDifference) Run(args []string) int {
	if len(args) != 3 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	indexA := args[1]
	indexB := args[2]

	if err := core.SetDifference(c.logger, indexName, indexA, indexB); err != nil {
		c.logger.Error("failed to compute set difference", "result", indexName, "A", indexA, "B", indexB, "error", err)
		return 1
	}

	c.logger.Info("set difference completed successfully", "result", indexName)
	return 0
}
