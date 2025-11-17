package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexMaterialize returns a CommandFactory for materializing an index to a folder.
func IndexMaterialize(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexMaterialize{
			logger: logger,
		}, nil
	}
}

type indexMaterialize struct {
	logger hclog.Logger
}

func (c *indexMaterialize) Synopsis() string {
	return "Materialize an index into a folder"
}

func (c *indexMaterialize) Help() string {
	return `Usage: venn index materialize <indexName> <rootPath>

Copy all indexed files to a target directory without duplicates.

This command creates a content-addressable layout where files are organized by
their SHA-256 hash. Files with the same content (hash) will only be copied once,
regardless of how many times they appear in the index. The directory structure
uses the first bytes of the hash for organization.

Arguments:
  indexName  Name of the index to materialize
  rootPath   Path to the target folder

Example:
  venn index materialize cleaned_photos /backup/photos
`
}

func (c *indexMaterialize) Run(args []string) int {
	if len(args) != 2 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	rootPath := args[1]

	if err := core.Materialize(c.logger, indexName, rootPath); err != nil {
		c.logger.Error("failed to materialize index", "index", indexName, "path", rootPath, "error", err)
		return 1
	}

	c.logger.Info("index materialized successfully", "index", indexName, "path", rootPath)
	return 0
}
