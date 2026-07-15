package main

import (
	"bytes"
	"strings"
	"testing"
)

// exec runs the dispatcher with captured writers.
func exec(args ...string) (code int, out, errOut string) {
	var o, e bytes.Buffer
	code = run(args, &o, &e)
	return code, o.String(), e.String()
}

func TestVersion(t *testing.T) {
	for _, flag := range []string{"-v", "-version", "--version"} {
		code, out, errOut := exec(flag)
		if code != 0 {
			t.Errorf("%s: got exit %d, want 0", flag, code)
		}
		if out != "venn 0.0.1\n" {
			t.Errorf("%s: out = %q, want %q", flag, out, "venn 0.0.1\n")
		}
		if errOut != "" {
			t.Errorf("%s: errOut = %q, want empty", flag, errOut)
		}
	}
}

func TestNoArgsPrintsUsageToStderrExit127(t *testing.T) {
	code, out, errOut := exec()
	if code != 127 {
		t.Errorf("got exit %d, want 127", code)
	}
	if out != "" {
		t.Errorf("out = %q, want empty", out)
	}
	if !strings.Contains(errOut, "Usage: venn") || !strings.Contains(errOut, "index add-files") {
		t.Errorf("errOut missing usage listing:\n%s", errOut)
	}
}

func TestHelpFlagPrintsUsageToStdout(t *testing.T) {
	for _, flag := range []string{"-h", "-help", "--help", "help"} {
		code, out, errOut := exec(flag)
		if code != 0 {
			t.Errorf("%s: got exit %d, want 0", flag, code)
		}
		if !strings.Contains(out, "Available commands:") || !strings.Contains(out, "set union") {
			t.Errorf("%s: out missing usage listing:\n%s", flag, out)
		}
		if errOut != "" {
			t.Errorf("%s: errOut = %q, want empty", flag, errOut)
		}
	}
}

func TestUnknownCommandExit127(t *testing.T) {
	code, out, errOut := exec("bogus")
	if code != 127 {
		t.Errorf("got exit %d, want 127", code)
	}
	if out != "" {
		t.Errorf("out = %q, want empty", out)
	}
	if !strings.Contains(errOut, `unknown command "bogus"`) {
		t.Errorf("errOut missing unknown-command message:\n%s", errOut)
	}
	if !strings.Contains(errOut, "Usage: venn") {
		t.Errorf("errOut missing usage after unknown command:\n%s", errOut)
	}
}

// A bare namespace prefix ("index") is not itself a command, so it falls
// through to unknown-command rather than matching anything.
func TestBareNamespaceIsUnknown(t *testing.T) {
	code, _, errOut := exec("index")
	if code != 127 {
		t.Errorf("got exit %d, want 127", code)
	}
	if !strings.Contains(errOut, "unknown command") {
		t.Errorf("errOut missing unknown-command message:\n%s", errOut)
	}
}

// The two-token name must win over treating "index" as a one-token command:
// "index add-files -h" resolves to the add-files command and prints its help.
func TestTwoWordDispatchHelp(t *testing.T) {
	code, out, errOut := exec("index", "add-files", "-h")
	if code != 0 {
		t.Errorf("got exit %d, want 0", code)
	}
	if !strings.Contains(out, "venn index add-files") {
		t.Errorf("out is not add-files help:\n%s", out)
	}
	if errOut != "" {
		t.Errorf("errOut = %q, want empty", errOut)
	}
}

func TestHelpAfterOneWordCommand(t *testing.T) {
	code, out, _ := exec("init", "-h")
	if code != 0 {
		t.Errorf("got exit %d, want 0", code)
	}
	if !strings.Contains(out, "Usage: venn init") {
		t.Errorf("out is not init help:\n%s", out)
	}
}

// A command returning RunResultHelp (wrong arg count) prints its own help to
// stderr and exits 1. init takes no arguments, so passing some triggers it.
func TestRunResultHelpRoundTrip(t *testing.T) {
	code, out, errOut := exec("init", "x", "y")
	if code != 1 {
		t.Errorf("got exit %d, want 1", code)
	}
	if out != "" {
		t.Errorf("out = %q, want empty", out)
	}
	if !strings.Contains(errOut, "Usage: venn init") {
		t.Errorf("errOut missing init help:\n%s", errOut)
	}
}
