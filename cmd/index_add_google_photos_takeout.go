package cmd

import (
	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

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
	return "Adds files to an index from a Google Photos Takeout"
}

func (c *indexAddGooglePhotosTakeout) Help() string {
	return `
Recursively scans all of the files in a folder tree and indexes them. The
index will be created if it doesn't exist, or if it does exist then new
files will be added to it.

This assumes it's scanning an extracted Google Photos Takeout, so it will
use the metadata JSON files for the image time stamps, and it will collect
the metadata files and attach them in any materialized version of the index.

venn index add-google-photos-takeout <indexName> <rootPath>
	
indexName: Name of index to use
rootPath:  Path of the root folder to scan`
}

func (c *indexAddGooglePhotosTakeout) Run(args []string) int {
	if len(args) != 2 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	rootPath := args[1]
	if err := core.IndexAddGooglePhotosTakeout(c.logger, indexName, rootPath); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
