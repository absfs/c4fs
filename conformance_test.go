package c4fs

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/absfs/absfs"
	"github.com/absfs/fstesting"
	"github.com/absfs/memfs"
	"github.com/absfs/unionfs"
)

// conformanceTree has no dangling or looping links, which io/fs's
// TestFS would rightly report as unopenable.
var conformanceTree = map[string]string{
	"README.md":          "hello\n",
	"empty.txt":          "",
	"src/main.go":        "package main\n",
	"src/lib/a.go":       "package lib\n",
	"src/lib/deep/z.txt": strings.Repeat("z", 5000),
	"docs/":              "",
	"tmp/":               "",
}

func conformanceView(t *testing.T) *View {
	t.Helper()
	dir, st := t.TempDir(), newTreeStore(t)
	writeTree(t, dir, conformanceTree)
	_, _, root := snapshot(t, dir, st, false)
	v, err := Open(st, root)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestIOFS runs the standard library's file system checker over the
// View through Sub: directory listings, Stat, Read, ReadAt and Seek.
func TestIOFS(t *testing.T) {
	v := conformanceView(t)
	fsys, err := v.Sub("/")
	if err != nil {
		t.Fatal(err)
	}
	if err := fstest.TestFS(fsys, "README.md", "empty.txt", "src/main.go", "src/lib/a.go", "src/lib/deep/z.txt"); err != nil {
		t.Fatal(err)
	}
	sub, err := v.Sub("src")
	if err != nil {
		t.Fatal(err)
	}
	if err := fstest.TestFS(sub, "main.go", "lib/deep/z.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Sub("/README.md"); err == nil {
		t.Error("Sub of a file succeeded")
	}
}

// TestFstestingReadOnly runs absfs/fstesting's read-only wrapper checks:
// writes are refused and nothing is created.
func TestFstestingReadOnly(t *testing.T) {
	base, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	suite := &fstesting.WrapperSuite{
		Name:     "c4fs",
		BaseFS:   base,
		ReadOnly: true,
		Factory: func(absfs.FileSystem) (absfs.FileSystem, error) {
			// The suite works in TempDir()/wrapper_test, which a
			// read-only view must already hold.
			m := c4m.NewManifest()
			m.AddEntry(&c4m.Entry{Name: "tmp/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp()})
			m.AddEntry(&c4m.Entry{Name: "wrapper_test/", Depth: 1, Timestamp: c4m.NullTimestamp()})
			return New(m, nil)
		},
	}
	suite.Run(t)
}

// TestFstestingAsUnionLower runs the full absfs/fstesting suite against
// a union whose read-only lower layer is a View: everything the suite
// writes lands in the upper, over the View's tree.
func TestFstestingAsUnionLower(t *testing.T) {
	v := conformanceView(t)
	upper, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	u := unionfs.New(unionfs.WithWritableLayer(upper), unionfs.WithReadOnlyLayer(v))
	suite := &fstesting.Suite{
		FS:      u.SymlinkFileSystem(),
		TestDir: "/tmp",
		Features: fstesting.Features{
			Symlinks:      true,
			Permissions:   true,
			Timestamps:    true,
			CaseSensitive: true,
			AtomicRename:  true,
			LargeFiles:    true,
		},
	}
	suite.Run(t)
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("lower changed: %q", got)
	}
}

// FuzzPathTraversal is absfs/fstesting's traversal fuzz target: no path
// may panic the View or reach outside it. Its seeds run with go test.
func FuzzPathTraversal(f *testing.F) {
	dir, st := f.TempDir(), newTreeStore(f)
	writeTree(f, dir, conformanceTree)
	_, _, root := snapshot(f, dir, st, false)
	v, err := Open(st, root)
	if err != nil {
		f.Fatal(err)
	}
	fstesting.FuzzPathTraversal(f, v, "/src")
}
