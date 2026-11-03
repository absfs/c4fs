package main

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
	"github.com/absfs/c4fs"
)

// storedTree puts a one-file tree into a new store that the c4 CLI's
// configuration (C4_STORE) points at, and returns the store and the
// ID of its manifest text, as c4 id -s prints it, and the tree's own
// C4 ID (ComputeC4ID), which a changeset names as its base.
func storedTree(t *testing.T) (*store.TreeStore, c4.ID, c4.ID) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.NewTreeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := s.Put(strings.NewReader("package main\n"))
	m := c4m.NewManifest()
	m.AddEntry(&c4m.Entry{Name: "src/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp(), Size: 13})
	m.AddEntry(&c4m.Entry{Name: "main.go", Depth: 1, Mode: 0o644, Size: 13, Timestamp: c4m.NullTimestamp(), C4ID: body})
	text, _ := c4m.Marshal(m)
	id, _ := s.Put(strings.NewReader(string(text)))
	t.Setenv("C4_STORE", dir)
	return s, id, m.ComputeC4ID()
}

func runOK(t *testing.T, args ...string) string {
	t.Helper()
	var out, errOut bytes.Buffer
	if err := run(args, &out, &errOut); err != nil {
		t.Fatalf("run %v: %v\n%s", args, err, errOut.String())
	}
	return out.String()
}

// TestModes runs the command's three modes over a small stored tree and
// checks what each prints.
func TestModes(t *testing.T) {
	s, id, tree := storedTree(t)

	listing := runOK(t, id.String())
	if !strings.Contains(listing, "drwxr-xr-x") || !strings.Contains(listing, " /src\n") {
		t.Errorf("listing has no src directory:\n%s", listing)
	}
	if !strings.Contains(listing, "-rw-r--r--         13 c4") || !strings.HasSuffix(listing, " /src/main.go\n") {
		t.Errorf("listing has no src/main.go line:\n%s", listing)
	}

	if got := runOK(t, id.String(), "src/main.go"); got != "package main\n" {
		t.Errorf("file = %q", got)
	}

	out := runOK(t, "-w", id.String())
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) < 4 {
		t.Fatalf("workspace output:\n%s", out)
	}
	head := strings.Fields(lines[0])
	if len(head) < 2 || head[0] != "captured" {
		t.Fatalf("first line %q", lines[0])
	}
	captured := strings.TrimSuffix(head[1], ":")
	if !strings.Contains(lines[0], "1 file(s), 25 bytes") {
		t.Errorf("first line %q", lines[0])
	}
	// The changeset: the base ID, the patch adding NOTES.txt, the ID.
	if lines[1] != tree.String() {
		t.Errorf("changeset base %q, want the tree's ID %s", lines[1], tree)
	}
	if !strings.Contains(out, " NOTES.txt c4") {
		t.Errorf("patch does not add NOTES.txt:\n%s", out)
	}
	if lines[len(lines)-2] != captured {
		t.Errorf("changeset closes with %q, want %s", lines[len(lines)-2], captured)
	}
	if lines[len(lines)-1] != "view: NOTES.txt absent, as it should be" {
		t.Errorf("last line %q", lines[len(lines)-1])
	}
	// The capture put the new content into the configured store.
	if !s.Has(c4.Identify(strings.NewReader("written in the workspace\n"))) {
		t.Error("NOTES.txt content not in the store")
	}
}

// TestRunErrors covers bad arguments and failures to open or read.
func TestRunErrors(t *testing.T) {
	_, id, _ := storedTree(t)
	var out, errOut bytes.Buffer
	if err := run(nil, &out, &errOut); !errors.Is(err, errUsage) || !strings.Contains(errOut.String(), "usage: example") {
		t.Errorf("no arguments: %v, %q", err, errOut.String())
	}
	if err := run([]string{"-nope"}, &out, &errOut); !errors.Is(err, errUsage) {
		t.Errorf("bad flag: %v", err)
	}
	if err := run([]string{"not-an-id"}, &out, &errOut); err == nil {
		t.Error("parsed a bad ID")
	}
	missing := c4.Identify(strings.NewReader("not stored"))
	if err := run([]string{missing.String()}, &out, &errOut); err == nil {
		t.Error("opened a tree not in the store")
	}
	err := run([]string{id.String(), "nope.txt"}, &out, &errOut)
	if err == nil {
		t.Error("printed a missing file")
	}
	// Exit statuses: 0, 2 for usage, 1 and a message otherwise.
	errOut.Reset()
	if exitCode(nil, &errOut) != 0 || exitCode(errUsage, &errOut) != 2 || errOut.Len() != 0 {
		t.Errorf("exit codes for success and usage, stderr %q", errOut.String())
	}
	if exitCode(err, &errOut) != 1 || !strings.HasPrefix(errOut.String(), "example: ") {
		t.Errorf("exit code for a failure, stderr %q", errOut.String())
	}
}

// failingFS creates files whose writes fail.
type failingFS struct{ absfs.FileSystem }

func (failingFS) Create(string) (absfs.File, error) { return failingFile{}, nil }

type failingFile struct{ absfs.File }

func (failingFile) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func (failingFile) Close() error              { return nil }

// TestErrors covers the helpers' failures: a missing directory to list,
// a file that cannot be created or written, and a store that cannot be
// opened.
func TestErrors(t *testing.T) {
	v, err := c4fs.New(c4m.NewManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := list(os.Stdout, v, "/missing"); err == nil {
		t.Error("listed a missing directory")
	}
	if err := write(v, "/x", "x"); err == nil {
		t.Error("wrote to a read-only view")
	}
	if err := write(failingFS{}, "/x", "x"); err == nil || err.Error() != "disk full" {
		t.Errorf("failed write = %v", err)
	}
	// A changeset that cannot be printed.
	sv, err := c4fs.OpenConfigured(func() c4.ID { _, id, _ := storedTree(t); return id }())
	if err != nil {
		t.Fatal(err)
	}
	if err := demoWorkspace(failingWriter{}, sv); err == nil || err.Error() != "closed pipe" {
		t.Errorf("print to a failing writer = %v", err)
	}
	// A store that cannot take the content.
	if runtime.GOOS != "windows" {
		ro := t.TempDir()
		if _, err := store.NewTreeStore(ro); err != nil {
			t.Fatal(err)
		}
		os.Chmod(ro, 0o555)
		t.Cleanup(func() { os.Chmod(ro, 0o755) })
		t.Setenv("C4_STORE", ro)
		if err := demoWorkspace(io.Discard, sv); err == nil {
			t.Error("captured into a read-only store")
		}
	}
	t.Setenv("C4_STORE", "")
	t.Setenv("HOME", t.TempDir())
	if err := demoWorkspace(os.Stdout, v); err == nil {
		t.Error("captured without a store")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("closed pipe") }
