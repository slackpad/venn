package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
	venncmd "github.com/slackpad/venn/cmd"
)

const (
	appName    = "venn"
	appVersion = "0.0.1"
)

// commands builds the registry of subcommands keyed by their full,
// space-separated name (e.g. "index add-files").
func commands(logger hclog.Logger) map[string]venncmd.Command {
	return map[string]venncmd.Command{
		// Initialization
		"init": venncmd.DoInit(logger),

		// Index management commands
		"index add-files":                 venncmd.IndexAddFiles(logger),
		"index add-google-photos-takeout": venncmd.IndexAddGooglePhotosTakeout(logger),
		"index cat":                       venncmd.IndexCat(logger),
		"index chunk":                     venncmd.IndexChunk(logger),
		"index ls":                        venncmd.IndexList(logger),
		"index materialize":               venncmd.IndexMaterialize(logger),
		"index rm":                        venncmd.IndexDelete(logger),
		"index stats":                     venncmd.IndexStats(logger),

		// Set operations
		"set difference":   venncmd.SetDifference(logger),
		"set intersection": venncmd.SetIntersection(logger),
		"set union":        venncmd.SetUnion(logger),
	}
}

func isHelpFlag(s string) bool {
	return s == "-h" || s == "-help" || s == "--help"
}

// usage prints the top-level command listing.
func usage(w io.Writer, cmds map[string]venncmd.Command) {
	names := make([]string, 0, len(cmds))
	for name := range cmds {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Fprintf(w, "Usage: %s [--version] [--help] <command> [<args>]\n\n", appName)
	fmt.Fprintln(w, "Available commands:")
	for _, name := range names {
		fmt.Fprintf(w, "    %-34s%s\n", name, cmds[name].Synopsis())
	}
}

// printHelp writes a command's help text with exactly one trailing newline.
func printHelp(w io.Writer, c venncmd.Command) {
	fmt.Fprintln(w, strings.TrimRight(c.Help(), "\n"))
}

// run dispatches a single invocation. Writers are injected so behavior can be
// tested; main wires them to stdout/stderr.
func run(args []string, out, errOut io.Writer) int {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   appName,
		Level:  hclog.LevelFromString("INFO"),
		Output: errOut,
	})
	cmds := commands(logger)

	if len(args) == 0 {
		usage(errOut, cmds)
		return 127
	}

	switch args[0] {
	case "-v", "-version", "--version":
		fmt.Fprintf(out, "%s %s\n", appName, appVersion)
		return 0
	case "-h", "-help", "--help", "help":
		usage(out, cmds)
		return 0
	}

	// Longest-prefix match: every command name is one or two tokens, so try a
	// two-token join first, then a single token.
	for n := 2; n >= 1; n-- {
		if len(args) < n {
			continue
		}
		c, ok := cmds[strings.Join(args[:n], " ")]
		if !ok {
			continue
		}
		rest := args[n:]
		for _, a := range rest {
			if isHelpFlag(a) {
				printHelp(out, c)
				return 0
			}
		}
		code := c.Run(rest)
		if code == venncmd.RunResultHelp {
			printHelp(errOut, c)
			return 1
		}
		return code
	}

	fmt.Fprintf(errOut, "Error: unknown command %q\n\n", strings.Join(args, " "))
	usage(errOut, cmds)
	return 127
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
