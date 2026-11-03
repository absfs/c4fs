package c4fs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// windowsNotDir behaves as syscall.ENOTDIR does on Windows, where it is
// ERROR_PATH_NOT_FOUND: it is ENOTDIR, and it is also fs.ErrNotExist.
type windowsNotDir struct{}

func (windowsNotDir) Error() string { return "not a directory" }

func (windowsNotDir) Is(target error) bool {
	return target == fs.ErrNotExist || target == syscall.ENOTDIR
}

// asOnWindows makes errNotDir match fs.ErrNotExist for the rest of the
// test, as it does on Windows, so every OS exercises what Windows sees.
// Tests that call it must not run in parallel.
func asOnWindows(t *testing.T) {
	t.Helper()
	old := errNotDir
	errNotDir = windowsNotDir{}
	t.Cleanup(func() { errNotDir = old })
	if !errors.Is(errNotDir, fs.ErrNotExist) || !errors.Is(errNotDir, syscall.ENOTDIR) {
		t.Fatal("errNotDir does not behave as on Windows")
	}
}

// throughFile names paths that pass through a file: directly, deeper,
// through a link to a file, and further down the tree.
var throughFile = []string{"/README.md/x", "/README.md/x/y", "/link/x", "/src/lib/a.go/more"}

type namedErr struct {
	op  string
	err error
}

// pathOps runs every operation of fsys that takes a path on name.
func pathOps(fsys interface {
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)
	ReadDir(string) ([]fs.DirEntry, error)
	ReadFile(string) ([]byte, error)
	Readlink(string) (string, error)
	Mkdir(string, os.FileMode) error
	MkdirAll(string, os.FileMode) error
	Remove(string) error
	RemoveAll(string) error
	Rename(string, string) error
	Chmod(string, os.FileMode) error
	Chtimes(string, time.Time, time.Time) error
	Chown(string, int, int) error
	Lchown(string, int, int) error
	Truncate(string, int64) error
	Symlink(string, string) error
	Chdir(string) error
}, open func(string, int) error, name string) []namedErr {
	now := time.Now()
	return []namedErr{
		{"Stat", second(fsys.Stat(name))},
		{"Lstat", second(fsys.Lstat(name))},
		{"Open", open(name, os.O_RDONLY)},
		{"OpenFile O_CREATE", open(name, os.O_CREATE|os.O_WRONLY)},
		{"OpenFile O_CREATE|O_EXCL", open(name, os.O_CREATE|os.O_EXCL|os.O_RDWR)},
		{"ReadDir", second(fsys.ReadDir(name))},
		{"ReadFile", second(fsys.ReadFile(name))},
		{"Readlink", second(fsys.Readlink(name))},
		{"Mkdir", fsys.Mkdir(name, 0o755)},
		{"MkdirAll", fsys.MkdirAll(name, 0o755)},
		{"Remove", fsys.Remove(name)},
		{"RemoveAll", fsys.RemoveAll(name)},
		{"Rename from", fsys.Rename(name, "/renamed")},
		{"Chmod", fsys.Chmod(name, 0o600)},
		{"Chtimes", fsys.Chtimes(name, now, now)},
		{"Chown", fsys.Chown(name, 1, 1)},
		{"Lchown", fsys.Lchown(name, 1, 1)},
		{"Truncate", fsys.Truncate(name, 0)},
		{"Symlink", fsys.Symlink("target", name)},
		{"Chdir", fsys.Chdir(name)},
	}
}

func wantNotDir(t *testing.T, name string, errs []namedErr) {
	t.Helper()
	for _, e := range errs {
		if e.err == nil || !errors.Is(e.err, syscall.ENOTDIR) {
			t.Errorf("%s(%q) = %v, want ENOTDIR", e.op, name, e.err)
		}
	}
}

// TestNotDirAsOnWindows: with ENOTDIR also matching fs.ErrNotExist, as
// on Windows, a path through a file is still ENOTDIR to every
// operation, and none succeeds as though the name were missing
// (RemoveAll, MkdirAll) or creates anything (OpenFile, Mkdir).
func TestNotDirAsOnWindows(t *testing.T) {
	asOnWindows(t)
	f := newFixture(t)
	for how, v := range f.views(t) {
		t.Run("view/"+how, func(t *testing.T) {
			open := func(name string, flag int) error { return second(v.OpenFile(name, flag, 0o644)) }
			for _, name := range throughFile {
				wantNotDir(t, name, pathOps(v, open, name))
			}
			if err := v.RemoveAll("/nope"); err != nil {
				t.Errorf("RemoveAll missing = %v", err)
			}
			if _, err := v.Stat("/nope/x"); !errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				t.Errorf("Stat under missing = %v, want ENOENT only", err)
			}
		})
	}
	t.Run("workspace", func(t *testing.T) {
		f := newWSFixture(t)
		w := f.w
		before := wsNames(t, w, "/")
		open := func(name string, flag int) error {
			h, err := w.OpenFile(name, flag, 0o644)
			if err == nil {
				h.Close()
			}
			return err
		}
		for _, name := range throughFile {
			errs := pathOps(w, open, name)
			errs = append(errs,
				namedErr{"Create", firstErr(w.Create(name))},
				namedErr{"Rename onto", w.Rename("/src/main.go", name)},
				namedErr{"Rename dir onto", w.Rename("/emptydir", name)},
			)
			wantNotDir(t, name, errs)
		}
		// Nothing changed: no name made, moved or removed, nothing
		// copied to scratch.
		if got := wsNames(t, w, "/"); !reflect.DeepEqual(got, before) {
			t.Errorf("root = %v, was %v", got, before)
		}
		for _, name := range []string{"README.md", "src/main.go", "src/lib/a.go"} {
			if got := wsRead(t, w, "/"+name); got != fixtureFiles[name] {
				t.Errorf("%s = %q", name, got)
			}
		}
		if n := f.scratchFiles(t); n != 0 {
			t.Errorf("%d scratch files", n)
		}
		if err := w.RemoveAll("/nope"); err != nil {
			t.Errorf("RemoveAll missing = %v", err)
		}
		if err := w.MkdirAll("/made/deeper", 0o755); err != nil {
			t.Errorf("MkdirAll new = %v", err)
		}
	})
}

// mkdirAllTree has every kind of name os.MkdirAll treats differently.
var mkdirAllTree = map[string]string{
	"README.md":   "hello\n",
	"src/main.go": "package main\n",
	"emptydir/":   "",
	"link":        "->README.md",
	"dirlink":     "->src",
	"dangling":    "->nowhere",
	"loop":        "->loop",
	"through":     "->README.md/x",
}

var mkdirAllPaths = []string{
	"/", "/README.md", "/README.md/x", "/README.md/x/y", "/src", "/src/new",
	"/src/main.go/x", "/new/a/b", "/emptydir/a", "/link", "/link/x",
	"/dirlink", "/dirlink/new", "/dangling", "/dangling/x", "/loop",
	"/loop/x", "/through", "/through/x",
}

// errSummary is the cause and path of err, comparable across the host
// and the package.
func errSummary(err error, root string) string {
	if err == nil {
		return "ok"
	}
	var cause string
	switch {
	case errors.Is(err, syscall.ENOTDIR):
		cause = "ENOTDIR"
	case errors.Is(err, syscall.ELOOP):
		cause = "ELOOP"
	case errors.Is(err, fs.ErrExist):
		cause = "EEXIST"
	case errors.Is(err, fs.ErrNotExist):
		cause = "ENOENT"
	case errors.Is(err, syscall.EROFS):
		cause = "EROFS"
	default:
		cause = err.Error()
	}
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		return cause + " (no PathError)"
	}
	p := pe.Path
	if root != "" {
		p = filepath.ToSlash(strings.TrimPrefix(p, root))
	}
	return pe.Op + " " + p + ": " + cause
}

// TestMkdirAllMatchesOS holds MkdirAll on a Workspace to os.MkdirAll on
// the same tree on disk, and on a View to what os.MkdirAll reports on a
// read-only mount: the same, except EROFS where it would make a
// directory, naming the first one.
func TestMkdirAllMatchesOS(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows reports its own errors for links and paths through files; c4fs answers as Unix does")
	}
	snap := t.TempDir()
	writeTree(t, snap, mkdirAllTree)
	st := newTreeStore(t)
	_, _, root := snapshot(t, snap, st, false)
	v := mustOpen(t, st, root)
	for _, name := range mkdirAllPaths {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeTree(t, dir, mkdirAllTree)
			// The first missing directory, which os.MkdirAll makes first.
			first := ""
			for p := name; p != "/"; p = filepath.Dir(p) {
				if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
					first = p
				}
			}
			host := errSummary(os.MkdirAll(filepath.Join(dir, filepath.FromSlash(name)), 0o755), dir)
			if got := errSummary(newWS(t, v).MkdirAll(name, 0o755), ""); got != host {
				t.Errorf("Workspace.MkdirAll = %s, os.MkdirAll = %s", got, host)
			}
			want := host
			if host == "ok" && first != "" {
				want = "mkdir " + first + ": EROFS"
			}
			if got := errSummary(v.MkdirAll(name, 0o755), ""); got != want {
				t.Errorf("View.MkdirAll = %s, want %s (os.MkdirAll = %s)", got, want, host)
			}
		})
	}
}
