package cmd

import (
	"strings"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
)

// commandTable enumerates every subcommand: its full name, its constructor, and
// the exact number of positional arguments its Run accepts. The whole cmd
// package shares one wrapper shape, so this table drives the arg-validation and
// help-contract checks for all of them at once.
var commandTable = []struct {
	name string
	new  func(hclog.Logger) Command
	argc int
}{
	{"init", DoInit, 0},
	{"index add-files", IndexAddFiles, 2},
	{"index add-google-photos-takeout", IndexAddGooglePhotosTakeout, 2},
	{"index cat", IndexCat, 1},
	{"index chunk", IndexChunk, 3},
	{"index ls", IndexList, 0},
	{"index materialize", IndexMaterialize, 2},
	{"index rm", IndexDelete, 1},
	{"index stats", IndexStats, 1},
	{"set difference", SetDifference, 3},
	{"set intersection", SetIntersection, 3},
	{"set union", SetUnion, 3},
}

// TestArgValidation checks that every command returns RunResultHelp for any arg
// count other than the one it expects. The wrong-count check runs before any
// core call, so no database is touched here.
func TestArgValidation(t *testing.T) {
	logger := hclog.NewNullLogger()
	for _, tc := range commandTable {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 2, 3, 4} {
				if n == tc.argc {
					continue // the valid count would fall through to a core call
				}
				got := tc.new(logger).Run(make([]string, n))
				if got != RunResultHelp {
					t.Errorf("%s: Run with %d args = %d, want RunResultHelp (%d)",
						tc.name, n, got, RunResultHelp)
				}
			}
		})
	}
}

// TestHelpContract locks in the shape the dispatcher relies on: a non-empty
// synopsis and help text that starts with "Usage: venn ".
func TestHelpContract(t *testing.T) {
	logger := hclog.NewNullLogger()
	for _, tc := range commandTable {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.new(logger)
			if c.Synopsis() == "" {
				t.Error("Synopsis() is empty")
			}
			if !strings.HasPrefix(c.Help(), "Usage: venn ") {
				t.Errorf("Help() does not start with %q:\n%s", "Usage: venn ", c.Help())
			}
		})
	}
}

// TestCoreFailureExit1 checks that a command whose core call fails returns exit
// code 1 (not RunResultHelp, not a panic). A fresh working directory has no
// venn.db, so any command that reads the database hits ErrNotInitialized.
func TestCoreFailureExit1(t *testing.T) {
	t.Chdir(t.TempDir())
	logger := hclog.NewNullLogger()

	cases := []struct {
		name string
		cmd  Command
		args []string
	}{
		{"index ls", IndexList(logger), nil},
		{"index stats", IndexStats(logger), []string{"idx"}},
		{"set union", SetUnion(logger), []string{"result", "a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cmd.Run(tc.args); got != 1 {
				t.Errorf("%s: Run against uninitialized dir = %d, want 1", tc.name, got)
			}
		})
	}
}
