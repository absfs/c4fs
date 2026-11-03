package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/absfs/absfs"
	"github.com/absfs/memfs"
	"github.com/absfs/switchfs"
	"github.com/absfs/unionfs"
)

// router routes every path method through switchfs's router with the
// route's rewrite applied, as C4's NFS workspace prototype does:
// switchfs v1.0.0 neither applies a route's Rewriter nor has the symlink
// methods absnfs needs.
type router struct {
	*switchfs.SwitchFS
	def absfs.SymlinkFileSystem
}

func (r *router) backend(p string) (absfs.SymlinkFileSystem, string) {
	rt, err := r.Router().RouteWithInfo(p, nil)
	if err != nil {
		return r.def, p
	}
	np := p
	if rt.Rewriter != nil {
		np = rt.Rewriter.Rewrite(p)
	}
	if np == "" {
		np = "/"
	}
	return rt.Backend.(absfs.SymlinkFileSystem), np
}

func (r *router) Stat(p string) (os.FileInfo, error) {
	b, np := r.backend(p)
	return b.Stat(np)
}

func (r *router) Lstat(p string) (os.FileInfo, error) {
	b, np := r.backend(p)
	return b.Lstat(np)
}

func (r *router) Readlink(p string) (string, error) {
	b, np := r.backend(p)
	return b.Readlink(np)
}

func (r *router) OpenFile(p string, flag int, perm os.FileMode) (absfs.File, error) {
	b, np := r.backend(p)
	return b.OpenFile(np, flag, perm)
}

func (r *router) Open(p string) (absfs.File, error) { return r.OpenFile(p, os.O_RDONLY, 0) }

func (r *router) Create(p string) (absfs.File, error) {
	return r.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (r *router) ReadDir(p string) ([]fs.DirEntry, error) {
	b, np := r.backend(p)
	return b.ReadDir(np)
}

func (r *router) Remove(p string) error {
	b, np := r.backend(p)
	return b.Remove(np)
}

func (r *router) Mkdir(p string, perm os.FileMode) error {
	b, np := r.backend(p)
	return b.Mkdir(np, perm)
}

func (r *router) Rename(o, n string) error {
	bo, no := r.backend(o)
	bn, nn := r.backend(n)
	if bo != bn {
		return &os.LinkError{Op: "rename", Old: o, New: n, Err: syscall.EXDEV}
	}
	return bo.Rename(no, nn)
}

// workspaces is the prototype's server: one memfs root listing
// workspaces, and per workspace a union of its own upper over one
// shared View.
type workspaces struct {
	mu     sync.Mutex
	root   *memfs.FileSystem
	sw     *router
	lower  *View
	uppers map[string]*memfs.FileSystem
}

func newWorkspaces(t *testing.T, lower *View) *workspaces {
	t.Helper()
	root, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	sw, err := switchfs.New(switchfs.WithDefault(root))
	if err != nil {
		t.Fatal(err)
	}
	return &workspaces{root: root, sw: &router{SwitchFS: sw, def: root}, lower: lower, uppers: map[string]*memfs.FileSystem{}}
}

func (w *workspaces) create(t *testing.T, name string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	upper, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	u := unionfs.New(unionfs.WithWritableLayer(upper), unionfs.WithReadOnlyLayer(w.lower))
	if err := w.root.Mkdir("/"+name, 0o755); err != nil {
		t.Fatal(err)
	}
	err = w.sw.Router().AddRoute(switchfs.Route{
		Pattern:  `^/` + regexp.QuoteMeta(name) + `(/|$)`,
		Type:     switchfs.PatternRegex,
		Backend:  u.SymlinkFileSystem(),
		Priority: 100,
		Rewriter: switchfs.StripPrefix("/" + name),
	})
	if err != nil {
		t.Fatal(err)
	}
	w.uppers[name] = upper
}

func readAll(t *testing.T, fsys interface {
	Open(string) (absfs.File, error)
}, name string) string {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// listing returns a function taking ReadDir's results, so a call reads
// listing(t)(fsys.ReadDir(dir)).
func listing(t *testing.T) func([]fs.DirEntry, error) []string {
	return func(ents []fs.DirEntry, err error) []string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(ents))
		for i, e := range ents {
			out[i] = e.Name()
		}
		sort.Strings(out)
		return out
	}
}

// TestUnionLower proves a View as the shared read-only lower of a union:
// reads come through the union, a write copies the file up, and the View
// is untouched.
func TestUnionLower(t *testing.T) {
	f := newFixture(t)
	cs := newCounting(f.st)
	v, err := Open(cs, f.root)
	if err != nil {
		t.Fatal(err)
	}
	upper, _ := memfs.NewFS()
	u := unionfs.New(unionfs.WithWritableLayer(upper), unionfs.WithReadOnlyLayer(v)).SymlinkFileSystem()

	if got := readAll(t, u, "/src/lib/a.go"); got != fixtureFiles["src/lib/a.go"] {
		t.Errorf("read through union = %q", got)
	}
	if got := listing(t)(u.ReadDir("/src/lib")); !reflect.DeepEqual(got, []string{"a.go", "b.go", "up"}) {
		t.Errorf("union listing = %v", got)
	}
	if fi, err := u.Lstat("/link"); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("union lstat link = %v %v", fi, err)
	}
	if target, err := u.Readlink("/link"); err != nil || target != "src/main.go" {
		t.Errorf("union readlink = %q %v", target, err)
	}

	// Copy-up on write: open an existing lower file for writing.
	w, err := u.OpenFile("/src/main.go", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("PACKAGE"), 0); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if got := readAll(t, u, "/src/main.go"); got != "PACKAGE main\n" {
		t.Errorf("after write = %q", got)
	}
	if got := readAll(t, upper, "/src/main.go"); got != "PACKAGE main\n" {
		t.Errorf("upper = %q", got)
	}
	// The copy keeps the lower file's mode: the scanned host's, which
	// is -rw-r--r-- here but -rw-rw-rw- on Windows.
	lower, err := v.Stat("/src/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && lower.Mode().Perm() != 0o644 {
		t.Fatalf("lower mode = %v", lower.Mode())
	}
	if fi, err := upper.Stat("/src/main.go"); err != nil || fi.Mode().Perm() != lower.Mode().Perm() {
		t.Errorf("copied-up mode = %v %v, lower %v", fi, err, lower.Mode())
	}
	// A new file and a deletion.
	nf, err := u.Create("/src/new.go")
	if err != nil {
		t.Fatal(err)
	}
	nf.Write([]byte("package main // new\n"))
	nf.Close()
	if err := u.Remove("/README.md"); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Stat("/README.md"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("removed file still visible: %v", err)
	}
	if got := listing(t)(u.ReadDir("/src")); !reflect.DeepEqual(got, []string{"lib", "main.go", "new.go"}) {
		t.Errorf("merged listing = %v", got)
	}

	// The lower is untouched.
	if got := mustRead(t, v, "/src/main.go"); got != "package main\n" {
		t.Errorf("lower changed: %q", got)
	}
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("lower lost README: %q", got)
	}
	if _, err := v.Stat("/src/new.go"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("new file leaked into lower: %v", err)
	}
	// Directories the union never visited were never loaded.
	if n := cs.count(f.m.GetEntry("docs/").C4ID); n != 0 {
		t.Errorf("unvisited docs record read %d times", n)
	}
}

// TestSwitchedWorkspaces composes the NFS workspace prototype's tree:
// switchfs routes /<workspace>/... to that workspace's union over one
// shared View, and workspaces do not see each other's writes.
func TestSwitchedWorkspaces(t *testing.T) {
	f := newFixture(t)
	v, err := Open(f.st, f.root)
	if err != nil {
		t.Fatal(err)
	}
	ws := newWorkspaces(t, v)
	ws.create(t, "alpha")
	ws.create(t, "beta")
	fsys := ws.sw

	if got := listing(t)(fsys.ReadDir("/")); !reflect.DeepEqual(got, []string{"alpha", "beta"}) {
		t.Errorf("root = %v", got)
	}
	for _, w := range []string{"alpha", "beta"} {
		if got := readAll(t, fsys, "/"+w+"/src/lib/b.go"); got != fixtureFiles["src/lib/b.go"] {
			t.Errorf("%s read = %q", w, got)
		}
		if got := listing(t)(fsys.ReadDir("/" + w)); len(got) != 10 {
			t.Errorf("%s listing = %v", w, got)
		}
	}

	// alpha edits a file, adds one and renames another.
	af, err := fsys.OpenFile("/alpha/README.md", os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	af.Write([]byte("alpha's readme\n"))
	af.Close()
	nf, err := fsys.Create("/alpha/docs/notes.txt")
	if err != nil {
		t.Fatal(err)
	}
	nf.Write([]byte("notes"))
	nf.Close()
	if err := fsys.Rename("/alpha/empty.txt", "/alpha/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Rename("/alpha/link", "/beta/link"); !errors.Is(err, syscall.EXDEV) {
		t.Errorf("cross-workspace rename = %v", err)
	}

	if got := readAll(t, fsys, "/alpha/README.md"); got != "alpha's readme\n" {
		t.Errorf("alpha readme = %q", got)
	}
	if got := readAll(t, fsys, "/beta/README.md"); got != "hello\n" {
		t.Errorf("beta sees alpha's write: %q", got)
	}
	if _, err := fsys.Stat("/beta/docs/notes.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("beta sees alpha's file: %v", err)
	}
	if _, err := fsys.Stat("/alpha/empty.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("rename left the old name: %v", err)
	}
	if fi, err := fsys.Lstat("/beta/link"); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("beta link = %v %v", fi, err)
	}
	if target, err := fsys.Readlink("/beta/dirlink"); err != nil || target != "src" {
		t.Errorf("readlink through router = %q %v", target, err)
	}
	// Only alpha's upper holds anything, and the View is unchanged.
	if _, err := ws.uppers["beta"].Stat("/README.md"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("beta upper written: %v", err)
	}
	if got := readAll(t, ws.uppers["alpha"], "/docs/notes.txt"); got != "notes" {
		t.Errorf("alpha upper = %q", got)
	}
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("lower changed: %q", got)
	}
	if _, err := v.Stat("/empty.txt"); err != nil {
		t.Errorf("lower lost a renamed file: %v", err)
	}
	if err := fsys.Mkdir("/alpha/newdir", 0o755); err != nil {
		t.Errorf("mkdir in workspace = %v", err)
	}
	if err := fsys.Remove("/alpha/newdir"); err != nil {
		t.Errorf("rmdir in workspace = %v", err)
	}
	if !strings.HasPrefix(v.TempDir(), "/") {
		t.Error("TempDir")
	}
}

// TestConcurrentReaders reads the shared lower from many goroutines
// through many workspaces at once, as an NFS server does.
func TestConcurrentReaders(t *testing.T) {
	f := newFixture(t)
	ram := newCounting(f.st)
	v, err := Open(streamOnly{ram}, f.root, WithCache(newTestCache(t)))
	if err != nil {
		t.Fatal(err)
	}
	ws := newWorkspaces(t, v)
	for _, w := range []string{"a", "b", "c", "d"} {
		ws.create(t, w)
	}
	paths := []string{"/README.md", "/src/main.go", "/src/lib/a.go", "/src/lib/b.go", "/docs/guide.txt", "/link", "/dirlink/lib/up", "/name with space"}
	want := map[string]string{}
	for _, p := range paths {
		want[p] = mustRead(t, mustNew(t, f.m, f.st), p)
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := []string{"a", "b", "c", "d"}[i%4]
			for j := 0; j < 20; j++ {
				p := paths[(i+j)%len(paths)]
				if got := readAll(t, ws.sw, "/"+w+p); got != want[p] {
					t.Errorf("%s%s = %q", w, p, got)
				}
				if _, err := ws.sw.ReadDir("/" + w + "/src/lib"); err != nil {
					t.Error(err)
				}
				if _, err := v.Stat(p); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	for _, p := range []string{"src/lib/", "docs/"} {
		if n := ram.count(f.m.GetEntry(p).C4ID); n != 1 {
			t.Errorf("record %s read %d times", p, n)
		}
	}
}
