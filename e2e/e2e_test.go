// Package e2e drives the built venn binary as a black box: TestMain compiles it
// once, and each test runs it in its own temporary working directory with
// exec.Command, asserting exit codes, stdout/stderr, and files left on disk.
//
// These tests exist because the commands print success output with fmt.Println
// straight to os.Stdout, so their real behavior can only be observed from a
// separate process. Plain `go test ./...` runs them — the tool is small enough
// that a real build-and-exec per suite stays in single-digit seconds.
package e2e

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// vennBin is the path to the binary built by TestMain.
var vennBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "venn-e2e-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	vennBin = filepath.Join(dir, "venn")
	build := exec.Command("go", "build", "-o", vennBin, ".")
	build.Dir = ".." // module root, where main lives
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build venn binary: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// result captures one invocation of the binary.
type result struct {
	code   int
	stdout string
	stderr string
}

// runVenn runs the binary in workdir with the given args and returns its result.
// A non-exit failure (binary missing, etc.) fails the test.
func runVenn(t *testing.T, workdir string, args ...string) result {
	t.Helper()

	cmd := exec.Command(vennBin, args...)
	cmd.Dir = workdir
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf

	code := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("venn %v: %v", args, err)
		}
		code = exitErr.ExitCode()
	}
	return result{code: code, stdout: out.String(), stderr: errBuf.String()}
}

// writeFile writes content to workdir/rel, creating parent directories.
func writeFile(t *testing.T, workdir, rel, content string) {
	t.Helper()
	path := filepath.Join(workdir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir for %q: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %q: %v", rel, err)
	}
}

func sha256hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", sum[:])
}

// mustInit runs `venn init` and fails the test if it does not succeed.
func mustInit(t *testing.T, workdir string) {
	t.Helper()
	r := runVenn(t, workdir, "init")
	if r.code != 0 {
		t.Fatalf("init: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if !strings.Contains(r.stdout, "Venn database initialized successfully") {
		t.Fatalf("init: stdout missing success message:\n%s", r.stdout)
	}
}

// TestFullWorkflow exercises the documented happy path: index two trees that
// share duplicate content, run every set operation, and materialize the union
// into a content-addressable tree whose files round-trip by hash.
func TestFullWorkflow(t *testing.T) {
	wd := t.TempDir()
	mustInit(t, wd)

	const (
		alpha  = "alpha unique content"
		beta   = "beta unique content"
		shared = "shared duplicate content"
	)
	writeFile(t, wd, "treeA/a.dat", alpha)
	writeFile(t, wd, "treeA/shared.dat", shared)
	writeFile(t, wd, "treeB/b.dat", beta)
	writeFile(t, wd, "treeB/shared.dat", shared) // same content as treeA/shared.dat

	if r := runVenn(t, wd, "index", "add-files", "A", "treeA"); r.code != 0 {
		t.Fatalf("add-files A: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if r := runVenn(t, wd, "index", "add-files", "B", "treeB"); r.code != 0 {
		t.Fatalf("add-files B: exit %d, stderr:\n%s", r.code, r.stderr)
	}

	// index ls shows both indexes.
	if got := indexNames(t, wd); !equalStrings(got, []string{"A", "B"}) {
		t.Errorf("index ls = %v, want [A B]", got)
	}

	// index A: two unique hashes, no duplicates within the index.
	if r := runVenn(t, wd, "index", "stats", "A"); r.code != 0 || !strings.Contains(r.stdout, "2 hashes for 2 files") {
		t.Errorf("stats A: exit %d, stdout:\n%s", r.code, r.stdout)
	}

	// Set operations.
	if r := runVenn(t, wd, "set", "union", "U", "A", "B"); r.code != 0 {
		t.Fatalf("set union: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if r := runVenn(t, wd, "set", "intersection", "I", "A", "B"); r.code != 0 {
		t.Fatalf("set intersection: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if r := runVenn(t, wd, "set", "difference", "D", "A", "B"); r.code != 0 {
		t.Fatalf("set difference: exit %d, stderr:\n%s", r.code, r.stderr)
	}

	// Union: 3 unique hashes across 4 files, with the shared hash carrying two paths.
	if r := runVenn(t, wd, "index", "stats", "U"); r.code != 0 || !strings.Contains(r.stdout, "3 hashes for 4 files (1 hashes with duplicates)") {
		t.Errorf("stats U: exit %d, stdout:\n%s", r.code, r.stdout)
	}
	// Intersection: only the shared hash.
	if r := runVenn(t, wd, "index", "stats", "I"); r.code != 0 || !strings.Contains(r.stdout, "1 hashes for 2 files") {
		t.Errorf("stats I: exit %d, stdout:\n%s", r.code, r.stdout)
	}
	// Difference A-B: only alpha remains.
	if r := runVenn(t, wd, "index", "stats", "D"); r.code != 0 || !strings.Contains(r.stdout, "1 hashes for 1 files") {
		t.Errorf("stats D: exit %d, stdout:\n%s", r.code, r.stdout)
	}

	// index cat U lists all three hashes.
	catU := runVenn(t, wd, "index", "cat", "U")
	if catU.code != 0 {
		t.Fatalf("cat U: exit %d, stderr:\n%s", catU.code, catU.stderr)
	}
	for _, want := range []string{sha256hex(alpha), sha256hex(beta), sha256hex(shared)} {
		if !strings.Contains(catU.stdout, want) {
			t.Errorf("cat U stdout missing hash %s:\n%s", want, catU.stdout)
		}
	}

	// Materialize the union and verify a content-addressable round-trip.
	if r := runVenn(t, wd, "index", "materialize", "U", "out"); r.code != 0 {
		t.Fatalf("materialize U: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	got := materializedHashes(t, filepath.Join(wd, "out"))
	want := map[string]bool{sha256hex(alpha): true, sha256hex(beta): true, sha256hex(shared): true}
	if len(got) != len(want) {
		t.Errorf("materialized %d files, want %d: %v", len(got), len(want), got)
	}
	for h := range got {
		if !want[h] {
			t.Errorf("materialized unexpected hash %s", h)
		}
	}
}

// TestTakeoutFlow indexes a minimal Google Photos Takeout fixture and checks
// that materialize preserves the photo-taken timestamp and the .json sidecar.
func TestTakeoutFlow(t *testing.T) {
	wd := t.TempDir()
	mustInit(t, wd)

	const (
		photo   = "not really a jpeg, but distinct bytes"
		takenTS = 1609459200 // 2021-01-01T00:00:00Z
	)
	metadata := fmt.Sprintf(`{"title":"photo.jpg","photoTakenTime":{"timestamp":"%d"}}`, takenTS)
	writeFile(t, wd, "takeout/photo.jpg", photo)
	writeFile(t, wd, "takeout/photo.jpg.json", metadata)

	if r := runVenn(t, wd, "index", "add-google-photos-takeout", "g", "takeout"); r.code != 0 {
		t.Fatalf("add-google-photos-takeout: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	// One content hash indexed (the .json companion is folded into it).
	if r := runVenn(t, wd, "index", "stats", "g"); r.code != 0 || !strings.Contains(r.stdout, "1 hashes for 1 files") {
		t.Errorf("stats g: exit %d, stdout:\n%s", r.code, r.stdout)
	}

	if r := runVenn(t, wd, "index", "materialize", "g", "out"); r.code != 0 {
		t.Fatalf("materialize g: exit %d, stderr:\n%s", r.code, r.stderr)
	}

	hexPhoto := sha256hex(photo)
	var jpgPath, jsonPath string
	walkFiles(t, filepath.Join(wd, "out"), func(path string) {
		switch filepath.Ext(path) {
		case ".jpg":
			jpgPath = path
		case ".json":
			jsonPath = path
		}
	})

	if jpgPath == "" {
		t.Fatal("materialized tree has no .jpg file")
	}
	if base := strings.TrimSuffix(filepath.Base(jpgPath), ".jpg"); base != hexPhoto {
		t.Errorf("materialized photo name = %s, want hash %s", base, hexPhoto)
	}
	info, err := os.Stat(jpgPath)
	if err != nil {
		t.Fatalf("stat materialized photo: %v", err)
	}
	if info.ModTime().Unix() != takenTS {
		t.Errorf("materialized photo mtime = %d, want %d (taken timestamp)", info.ModTime().Unix(), takenTS)
	}

	if jsonPath == "" {
		t.Fatal("materialized tree has no .json attachment")
	}
	gotJSON, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read materialized attachment: %v", err)
	}
	if string(gotJSON) != metadata {
		t.Errorf("attachment content = %q, want %q", gotJSON, metadata)
	}
}

// TestChunkAndDelete splits an index into fixed-size chunks and then removes the
// source, checking that `index ls` reflects both operations.
func TestChunkAndDelete(t *testing.T) {
	wd := t.TempDir()
	mustInit(t, wd)

	for i := range 5 {
		writeFile(t, wd, fmt.Sprintf("tree/f%d.dat", i), fmt.Sprintf("distinct content number %d", i))
	}
	if r := runVenn(t, wd, "index", "add-files", "big", "tree"); r.code != 0 {
		t.Fatalf("add-files big: exit %d, stderr:\n%s", r.code, r.stderr)
	}

	// 5 entries, chunk size 2 -> part-0, part-1, part-2 (2, 2, 1).
	if r := runVenn(t, wd, "index", "chunk", "big", "part", "2"); r.code != 0 {
		t.Fatalf("chunk: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if got := indexNames(t, wd); !equalStrings(got, []string{"big", "part-0", "part-1", "part-2"}) {
		t.Errorf("after chunk, index ls = %v, want [big part-0 part-1 part-2]", got)
	}

	if r := runVenn(t, wd, "index", "rm", "big"); r.code != 0 {
		t.Fatalf("rm big: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	if got := indexNames(t, wd); !equalStrings(got, []string{"part-0", "part-1", "part-2"}) {
		t.Errorf("after rm, index ls = %v, want [part-0 part-1 part-2]", got)
	}
}

// TestErrorSurfaces covers the failure exit codes a caller relies on.
func TestErrorSurfaces(t *testing.T) {
	t.Run("command before init exits 1", func(t *testing.T) {
		wd := t.TempDir()
		r := runVenn(t, wd, "index", "ls")
		if r.code != 1 {
			t.Errorf("exit = %d, want 1", r.code)
		}
		if !strings.Contains(r.stderr, "venn has not been initialized") {
			t.Errorf("stderr missing not-initialized error:\n%s", r.stderr)
		}
	})

	t.Run("unknown command exits 127", func(t *testing.T) {
		wd := t.TempDir()
		r := runVenn(t, wd, "bogus")
		if r.code != 127 {
			t.Errorf("exit = %d, want 127", r.code)
		}
		if !strings.Contains(r.stderr, "unknown command") {
			t.Errorf("stderr missing unknown-command error:\n%s", r.stderr)
		}
	})

	t.Run("wrong arg count exits 1 and prints usage", func(t *testing.T) {
		wd := t.TempDir()
		mustInit(t, wd)
		r := runVenn(t, wd, "index", "add-files", "onlyonearg")
		if r.code != 1 {
			t.Errorf("exit = %d, want 1", r.code)
		}
		if !strings.Contains(r.stderr, "Usage: venn index add-files") {
			t.Errorf("stderr missing command usage:\n%s", r.stderr)
		}
	})
}

// indexNames returns the sorted output of `index ls`, one name per line.
func indexNames(t *testing.T, workdir string) []string {
	t.Helper()
	r := runVenn(t, workdir, "index", "ls")
	if r.code != 0 {
		t.Fatalf("index ls: exit %d, stderr:\n%s", r.code, r.stderr)
	}
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(r.stdout), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names = append(names, line)
		}
	}
	sort.Strings(names)
	return names
}

// materializedHashes walks a materialized tree and returns, for each content
// file, the sha256 of its bytes — asserting the filename encodes that hash.
func materializedHashes(t *testing.T, root string) map[string]string {
	t.Helper()
	hashes := make(map[string]string)
	walkFiles(t, root, func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read materialized file %q: %v", path, err)
		}
		sum := fmt.Sprintf("%x", sha256.Sum256(data))
		name := filepath.Base(path)
		base := strings.TrimSuffix(name, filepath.Ext(name))
		if base != sum {
			t.Errorf("materialized file %q does not match its content hash %s", name, sum)
		}
		hashes[sum] = path
	})
	return hashes
}

// walkFiles calls fn for every regular file under root.
func walkFiles(t *testing.T, root string, fn func(path string)) {
	t.Helper()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fn(path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", root, err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
