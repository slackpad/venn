package cmd

import (
	"strconv"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/mitchellh/cli"
	"github.com/slackpad/venn/core"
)

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
	return "Chunk an index into fixed-size parts"
}

func (c *indexChunk) Help() string {
	return `
venn index chunk <indexName> <targetIndexPrefix> <chunkSize>
	
indexName: Name of index to use
targetIndexPrefix: Prefix for the target indexes
chunkSize: Chunk size`
}

func (c *indexChunk) Run(args []string) int {
	if len(args) != 3 {
		return cli.RunResultHelp
	}
	indexName := args[0]
	targetIndexPrefix := args[1]
	chunkSize, err := strconv.Atoi(args[2])
	if err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	if err := core.IndexChunk(c.logger, indexName, targetIndexPrefix, chunkSize); err != nil {
		c.logger.Error(err.Error())
		return 1
	}
	return 0
}
