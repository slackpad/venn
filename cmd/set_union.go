package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// SetUnion returns a CommandFactory for computing set union.
func SetUnion(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setUnion{
			logger: logger,
		}, nil
	}
}

type setUnion struct {
	logger hclog.Logger
}

func (c *setUnion) Synopsis() string {
	return "Create a new index as A ∪ B"
}

func (c *setUnion) Help() string {
	return `Usage: venn set union <indexName> <indexNameA> <indexNameB>

Create a new index containing all files from both A and B.

This performs a set union operation, creating a new index with all entries from
both index A and index B. When a file appears in both indexes (same SHA-256 hash),
all paths and metadata are merged. Original indexes A and B are not modified.

Arguments:
  indexName   Name of the new index to create with the result
  indexNameA  First index
  indexNameB  Second index

Example:
  venn set union all_photos photos1 photos2
`
}

func (c *setUnion) Run(args []string) int {
	if len(args) != 3 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	indexA := args[1]
	indexB := args[2]

	if err := core.SetUnion(c.logger, indexName, indexA, indexB); err != nil {
		c.logger.Error("failed to compute set union", "result", indexName, "A", indexA, "B", indexB, "error", err)
		return 1
	}

	c.logger.Info("set union completed successfully", "result", indexName)
	return 0
}
