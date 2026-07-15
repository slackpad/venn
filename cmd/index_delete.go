package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/slackpad/venn/core"
)

// IndexDelete returns a Command for deleting an index.
func IndexDelete(logger hclog.Logger) Command {
	return &indexDelete{
		logger: logger,
	}
}

type indexDelete struct {
	logger hclog.Logger
}

func (c *indexDelete) Synopsis() string {
	return "Delete an index"
}

func (c *indexDelete) Help() string {
	return `Usage: venn index rm <indexName>

Delete an index from the database.

WARNING: This operation cannot be undone. The index and all its file references
will be permanently removed from the database.

Arguments:
  indexName  Name of the index to delete

Example:
  venn index rm old_photos
`
}

func (c *indexDelete) Run(args []string) int {
	if len(args) != 1 {
		c.logger.Error("incorrect number of arguments")
		return RunResultHelp
	}

	indexName := args[0]

	if err := core.IndexDelete(c.logger, indexName); err != nil {
		c.logger.Error("failed to delete index", "index", indexName, "error", err)
		return 1
	}

	c.logger.Info("index deleted successfully", "index", indexName)
	return 0
}
