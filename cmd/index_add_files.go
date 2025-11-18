package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexAddFiles returns a CommandFactory for adding files to an index.
func IndexAddFiles(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexAddFiles{
			logger: logger,
		}, nil
	}
}

type indexAddFiles struct {
	logger hclog.Logger
}

func (c *indexAddFiles) Synopsis() string {
	return "Add files to an index"
}

func (c *indexAddFiles) Help() string {
	return `Usage: venn index add-files <indexName> <rootPath>

Recursively scan all files in a folder tree and add them to an index.

The index will be created if it doesn't exist. If it already exists, new files
will be added to it. Files are identified by their SHA-256 hash, so duplicate
files across multiple paths will be tracked efficiently.

Arguments:
  indexName  Name of the index to create or update
  rootPath   Path to the root folder to scan

Example:
  venn index add-files photos /home/user/Pictures
`
}

func (c *indexAddFiles) Run(args []string) int {
	if len(args) != 2 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	rootPath := args[1]

	if err := core.IndexAddFiles(c.logger, indexName, rootPath); err != nil {
		c.logger.Error("failed to add files to index", "index", indexName, "path", rootPath, "error", err)
		return 1
	}

	c.logger.Info("files added successfully", "index", indexName)
	return 0
}
