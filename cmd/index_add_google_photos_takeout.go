package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexAddGooglePhotosTakeout returns a CommandFactory for adding files from a Google Photos Takeout.
func IndexAddGooglePhotosTakeout(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexAddGooglePhotosTakeout{
			logger: logger,
		}, nil
	}
}

type indexAddGooglePhotosTakeout struct {
	logger hclog.Logger
}

func (c *indexAddGooglePhotosTakeout) Synopsis() string {
	return "Add files from a Google Photos Takeout to an index"
}

func (c *indexAddGooglePhotosTakeout) Help() string {
	return `Usage: venn index add-google-photos-takeout <indexName> <rootPath>

Recursively scan files from a Google Photos Takeout and add them to an index.

This command is specifically designed for Google Photos Takeout archives. It
will extract timestamps from the JSON metadata files that accompany photos
and attach those metadata files to the indexed entries for materialization.

The index will be created if it doesn't exist. If it already exists, new files
will be added to it.

Arguments:
  indexName  Name of the index to create or update
  rootPath   Path to the extracted Google Photos Takeout folder

Example:
  venn index add-google-photos-takeout photos ~/Downloads/GooglePhotosTakeout
`
}

func (c *indexAddGooglePhotosTakeout) Run(args []string) int {
	if len(args) != 2 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	rootPath := args[1]

	if err := core.IndexAddGooglePhotosTakeout(c.logger, indexName, rootPath); err != nil {
		c.logger.Error("failed to add Google Photos takeout to index", "index", indexName, "path", rootPath, "error", err)
		return 1
	}

	c.logger.Info("Google Photos takeout added successfully", "index", indexName)
	return 0
}
