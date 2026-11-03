package main

import (
	"flag"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/c4fs"
)

// TestModes runs the command's three modes over a small stored tree.
func TestModes(t *testing.T) {
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

	stdout := os.Stdout
	devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	os.Stdout = devnull
	defer func() { os.Stdout = stdout; devnull.Close() }()

	for _, args := range [][]string{
		{id.String()},
		{id.String(), "src/main.go"},
		{"-w", id.String()},
	} {
		flag.CommandLine = flag.NewFlagSet("example", flag.ExitOnError)
		os.Args = append([]string{"example"}, args...)
		main()
	}
}

// TestErrors covers the helpers' failures: a missing directory to list,
// a file that cannot be created, and a store that cannot take content.
func TestErrors(t *testing.T) {
	v, err := c4fs.New(c4m.NewManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := list(v, "/missing"); err == nil {
		t.Error("listed a missing directory")
	}
	if err := write(v, "/x", "x"); err == nil {
		t.Error("wrote to a read-only view")
	}
	t.Setenv("C4_STORE", "")
	t.Setenv("HOME", t.TempDir())
	if err := demoWorkspace(v); err == nil {
		t.Error("captured without a store")
	}
}
