package cmd

import (
	"strconv"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

// IndexChunk returns a CommandFactory for splitting an index into smaller chunks.
func IndexChunk(logger hclog.Logger) cli.CommandFactory {
	return func() (cli.Command, error) {
		return &indexChunk{
			logger: logger,
		}, nil
	}
}

type indexChunk struct {
	logger hclog.Logger
}

func (c *indexChunk) Synopsis() string {
	return "Split an index into fixed-size chunks"
}

func (c *indexChunk) Help() string {
	return `Usage: venn index chunk <indexName> <targetIndexPrefix> <chunkSize>

Split a large index into multiple smaller indexes of fixed size.

This command creates new indexes with names like "prefix-0", "prefix-1", etc.,
each containing up to chunkSize entries from the source index. This is useful
for processing large indexes in smaller batches.

Arguments:
  indexName          Name of the source index to split
  targetIndexPrefix  Prefix for the created chunk indexes
  chunkSize          Maximum number of entries per chunk

Example:
  venn index chunk photos photo_chunk 1000
`
}

func (c *indexChunk) Run(args []string) int {
	if len(args) != 3 {
		c.logger.Error("incorrect number of arguments")
		return cli.RunResultHelp
	}

	indexName := args[0]
	targetIndexPrefix := args[1]
	chunkSize, err := strconv.Atoi(args[2])
	if err != nil {
		c.logger.Error("invalid chunk size", "value", args[2], "error", err)
		return 1
	}

	if err := core.IndexChunk(c.logger, indexName, targetIndexPrefix, chunkSize); err != nil {
		c.logger.Error("failed to chunk index", "index", indexName, "error", err)
		return 1
	}

	c.logger.Info("index chunked successfully", "source", indexName, "prefix", targetIndexPrefix)
	return 0
}
