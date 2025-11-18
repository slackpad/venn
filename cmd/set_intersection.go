package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// SetIntersection returns a CommandFactory for computing set intersection.
func SetIntersection(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &setIntersection{
			logger: logger,
		}, nil
	}
}

type setIntersection struct {
	logger hclog.Logger
}

func (c *setIntersection) Synopsis() string {
	return "Create a new index as A ∩ B"
}

func (c *setIntersection) Help() string {
	return `Usage: venn set intersection <indexName> <indexNameA> <indexNameB>

Create a new index containing only files that appear in both A and B.

This performs a set intersection operation, creating a new index with only the
entries that exist in both index A and index B. The operation is based on file
content (SHA-256 hash), not file paths. Original indexes A and B are not modified.

Arguments:
  indexName   Name of the new index to create with the result
  indexNameA  First index
  indexNameB  Second index

Example:
  venn set intersection common_photos photos1 photos2
`
}

func (c *setIntersection) Run(args []string) int {
	if len(args) != 3 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	indexA := args[1]
	indexB := args[2]

	if err := core.SetIntersection(c.logger, indexName, indexA, indexB); err != nil {
		c.logger.Error("failed to compute set intersection", "result", indexName, "A", indexA, "B", indexB, "error", err)
		return 1
	}

	c.logger.Info("set intersection completed successfully", "result", indexName)
	return 0
}
