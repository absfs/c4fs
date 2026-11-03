package c4fs

import (
	"io/fs"
	"runtime"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/absfs/absfs"
	"github.com/absfs/basefs"
	"github.com/absfs/fstesting"
	"github.com/absfs/memfs"
	"github.com/absfs/osfs"
)

// newScratch returns an empty in-memory scratch filesystem.
func newScratch(t testing.TB) absfs.FileSystem {
	t.Helper()
	m, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// folderScratch returns a scratch filesystem in a new host folder, as
// a server would use: basefs over osfs.
func folderScratch(t testing.TB) absfs.FileSystem {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("basefs v1.0.0 takes a Unix-style absolute path, which Windows' filepath.IsAbs refuses")
	}
	host, err := osfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	sc, err := basefs.NewFS(host, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func newWS(t testing.TB, v *View) *Workspace {
	t.Helper()
	w, err := NewWorkspace(v, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

var allFeatures = fstesting.Features{
	Symlinks:      true,
	Permissions:   true,
	Timestamps:    true,
	CaseSensitive: true,
	AtomicRename:  true,
	LargeFiles:    true,
}

// TestFstestingWorkspace runs absfs/fstesting's full suite against a
// workspace over a View: everything it writes lands in the workspace,
// over the View's tree.
func TestFstestingWorkspace(t *testing.T) {
	v := conformanceView(t)
	w := newWS(t, v)
	(&fstesting.Suite{FS: w, TestDir: "/tmp", Features: allFeatures}).Run(t)
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("base changed: %q", got)
	}
}

// TestFstestingWorkspaceFolder runs the suite with scratch in a host
// folder, as a server keeps it.
func TestFstestingWorkspaceFolder(t *testing.T) {
	w, err := NewWorkspace(conformanceView(t), folderScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	(&fstesting.Suite{FS: w, TestDir: "/tmp", Features: allFeatures}).Run(t)
}

// TestFstestingWorkspaceEmpty runs the suite over an empty View, in a
// directory the workspace makes, and again in the View's tree itself.
func TestFstestingWorkspaceEmpty(t *testing.T) {
	v, err := New(c4m.NewManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	(&fstesting.Suite{FS: newWS(t, v), Features: allFeatures}).Run(t)
}

// TestFstestingWorkspaceInBase runs the suite in a directory of the
// View, so its files and the suite's share directories.
func TestFstestingWorkspaceInBase(t *testing.T) {
	(&fstesting.Suite{FS: newWS(t, conformanceView(t)), TestDir: "/src", Features: allFeatures}).Run(t)
}

// TestWorkspaceWrapperSuite runs fstesting's wrapper checks: a
// workspace over an empty View behaves as its scratch-backed writes.
func TestWorkspaceWrapperSuite(t *testing.T) {
	base, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	suite := &fstesting.WrapperSuite{
		Name:   "c4fs.Workspace",
		BaseFS: base,
		Factory: func(absfs.FileSystem) (absfs.FileSystem, error) {
			m := c4m.NewManifest()
			m.AddEntry(&c4m.Entry{Name: "tmp/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp()})
			v, err := New(m, nil)
			if err != nil {
				return nil, err
			}
			return NewWorkspace(v, newScratch(t))
		},
	}
	suite.Run(t)
}

// FuzzWorkspacePathTraversal is fstesting's traversal fuzz target over
// a workspace.
func FuzzWorkspacePathTraversal(f *testing.F) {
	dir, st := f.TempDir(), newTreeStore(f)
	writeTree(f, dir, conformanceTree)
	_, _, root := snapshot(f, dir, st, false)
	v, err := Open(st, root)
	if err != nil {
		f.Fatal(err)
	}
	fstesting.FuzzPathTraversal(f, newWS(f, v), "/src")
}
