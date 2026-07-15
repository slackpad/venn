package cmd

// Command is implemented by every venn subcommand.
type Command interface {
	Synopsis() string
	Help() string
	Run(args []string) int
}

// RunResultHelp tells the runner to print the command's help and exit 1.
// The value is kept out of the sane exit-code range so a command can never
// return it as a real status.
const RunResultHelp = -18511
