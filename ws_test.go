package c4fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/absfs/absfs"
)

// wsFixture is a workspace over the view fixture's tree, with its
// scratch filesystem exposed for inspection.
type wsFixture struct {
	*fixture
	v       *View
	w       *Workspace
	scratch absfs.FileSystem
}

func newWSFixture(t testing.TB) *wsFixture {
	t.Helper()
	f := newFixture(t)
	v, err := Open(f.st, f.root)
	if err != nil {
		t.Fatal(err)
	}
	sc := newScratch(t)
	w, err := NewWorkspace(v, sc)
	if err != nil {
		t.Fatal(err)
	}
	return &wsFixture{fixture: f, v: v, w: w, scratch: sc}
}

// scratchFiles counts the files in the scratch filesystem.
func (f *wsFixture) scratchFiles(t testing.TB) int {
	t.Helper()
	ents, err := f.scratch.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

func wsRead(t testing.TB, w *Workspace, name string) string {
	t.Helper()
	b, err := w.ReadFile(name)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", name, err)
	}
	return string(b)
}

func wsWrite(t testing.TB, w *Workspace, name, body string) {
	t.Helper()
	f, err := w.Create(name)
	if err != nil {
		t.Fatalf("Create(%s): %v", name, err)
	}
	if _, err := f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func wsNames(t testing.TB, w *Workspace, dir string) []string {
	t.Helper()
	ents, err := w.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	out := make([]string, len(ents))
	for i, e := range ents {
		out[i] = e.Name()
	}
	return out
}

func wantErr(t testing.TB, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Errorf("%s: err = %v, want %v", what, err, want)
	}
}

func TestWorkspaceReadsBase(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	for name, body := range fixtureFiles {
		if strings.HasSuffix(name, "/") || strings.HasPrefix(body, "->") {
			continue
		}
		if got := wsRead(t, w, "/"+name); got != body {
			t.Errorf("%s = %q", name, got)
		}
	}
	if got, want := wsNames(t, w, "/"), names(t, f.v, "/"); !reflect.DeepEqual(got, want) {
		t.Errorf("root = %v, want %v", got, want)
	}
	if got := wsRead(t, w, "/link"); got != "package main\n" {
		t.Errorf("link = %q", got)
	}
	if got := wsRead(t, w, "/dirlink/lib/up"); got != "package main\n" {
		t.Errorf("through links = %q", got)
	}
	if target, err := w.Readlink("/abslink"); err != nil || target != "/src/lib/a.go" {
		t.Errorf("readlink = %q %v", target, err)
	}
	fi, err := w.Stat("/src/main.go")
	if err != nil || fi.Size() != 13 || fi.Sys().(*c4m.Entry).C4ID.IsNil() {
		t.Errorf("stat = %v %v", fi, err)
	}
	if f.scratchFiles(t) != 0 {
		t.Error("reading wrote to scratch")
	}
	if w.Base() != f.v {
		t.Error("Base")
	}
	if w.TempDir() != "/tmp" {
		t.Error("TempDir")
	}
}

// TestWorkspaceCopyUp: writing a View file copies it to scratch once,
// appends and truncates work on the copy, and the View is untouched.
func TestWorkspaceCopyUp(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	h, err := w.OpenFile("/README.md", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WriteString("more\n"); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if got := wsRead(t, w, "/README.md"); got != "hello\nmore\n" {
		t.Errorf("appended = %q", got)
	}
	if fi, _ := w.Stat("/README.md"); fi.Size() != 11 || !fi.Sys().(*c4m.Entry).C4ID.IsNil() {
		t.Errorf("stat after write = %d %v", fi.Size(), fi.Sys())
	}
	// A second write-open does not copy again.
	h, _ = w.OpenFile("/README.md", os.O_RDWR, 0)
	h.WriteAt([]byte("J"), 0)
	h.Close()
	if got := wsRead(t, w, "/README.md"); got != "Jello\nmore\n" {
		t.Errorf("rewritten = %q", got)
	}
	if n := f.scratchFiles(t); n != 1 {
		t.Errorf("%d scratch files", n)
	}
	if got := mustRead(t, f.v, "/README.md"); got != "hello\n" {
		t.Errorf("view changed: %q", got)
	}
	// O_TRUNC copies nothing and empties the file.
	h, _ = w.OpenFile("/docs/guide.txt", os.O_WRONLY|os.O_TRUNC, 0)
	h.Close()
	if fi, _ := w.Stat("/docs/guide.txt"); fi.Size() != 0 {
		t.Errorf("truncated size %d", fi.Size())
	}
	// Truncate copies at most what it keeps.
	if err := w.Truncate("/src/lib/a.go", 7); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/src/lib/a.go"); got != "package" {
		t.Errorf("truncated = %q", got)
	}
	if err := w.Truncate("/src/lib/b.go", 20); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/src/lib/b.go"); got != "package lib // b\n\x00\x00\x00" {
		t.Errorf("extended = %q", got)
	}
	if err := w.Truncate("/src/main.go", 0); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/src/main.go"); got != "" {
		t.Errorf("emptied = %q", got)
	}
	if err := w.Truncate("/src", 0); !errors.Is(err, syscall.EISDIR) {
		t.Errorf("truncate dir = %v", err)
	}
	if err := w.Truncate("/nope", 0); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("truncate missing = %v", err)
	}
	if err := w.Truncate("/README.md", -1); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("truncate negative = %v", err)
	}
	// Writing through a link writes the target.
	h, err = w.OpenFile("/link", os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	h.Write([]byte("via link"))
	h.Close()
	if got := wsRead(t, w, "/src/main.go"); got != "via link" {
		t.Errorf("through link = %q", got)
	}
}

// TestWorkspaceMetadataOnly: chmod and chtimes of a View file record
// the change without copying content.
func TestWorkspaceMetadataOnly(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Chmod("/src/main.go", 0o600); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := w.Chtimes("/docs/guide.txt", when, when); err != nil {
		t.Fatal(err)
	}
	if err := w.Chmod("/src", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := w.Chmod("/link", 0o640); err != nil { // follows the link
		t.Fatal(err)
	}
	if f.scratchFiles(t) != 0 {
		t.Error("metadata change copied content")
	}
	fi, _ := w.Stat("/src/main.go")
	if fi.Mode() != 0o640 {
		t.Errorf("mode = %v", fi.Mode())
	}
	if fi, _ := w.Stat("/docs/guide.txt"); !fi.ModTime().Equal(when) || fi.Size() != 6000 {
		t.Errorf("chtimes = %v %d", fi.ModTime(), fi.Size())
	}
	if fi, _ := w.Stat("/src"); fi.Mode() != fs.ModeDir|0o700 || !fi.IsDir() {
		t.Errorf("dir mode = %v", fi.Mode())
	}
	if got := wsRead(t, w, "/src/main.go"); got != "package main\n" {
		t.Errorf("content = %q", got)
	}
	h, _ := w.Open("/src/main.go")
	if fi, _ := h.Stat(); fi.Mode() != 0o640 {
		t.Errorf("handle stat mode = %v", fi.Mode())
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Errorf("write to read handle = %v", err)
	}
	h.Close()
	if err := w.Chown("/src/main.go", 1, 2); err != nil {
		t.Errorf("chown = %v", err)
	}
	if err := w.Lchown("/dangling", 1, 2); err != nil {
		t.Errorf("lchown = %v", err)
	}
	wantErr(t, "chown missing", w.Chown("/nope", 1, 2), fs.ErrNotExist)
	wantErr(t, "chmod missing", w.Chmod("/nope", 0), fs.ErrNotExist)
	wantErr(t, "chown dangling", w.Chown("/dangling", 1, 2), fs.ErrNotExist)
}

func TestWorkspaceCreate(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/src/new.go", "package new\n")
	if got := wsRead(t, w, "/src/new.go"); got != "package new\n" {
		t.Errorf("new = %q", got)
	}
	if got := wsNames(t, w, "/src"); !reflect.DeepEqual(got, []string{"lib", "main.go", "new.go"}) {
		t.Errorf("src = %v", got)
	}
	_, err := w.OpenFile("/src/new.go", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	wantErr(t, "O_EXCL existing", err, fs.ErrExist)
	_, err = w.OpenFile("/src/main.go", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	wantErr(t, "O_EXCL in view", err, fs.ErrExist)
	_, err = w.OpenFile("/nodir/x", os.O_CREATE|os.O_WRONLY, 0o644)
	wantErr(t, "create in missing dir", err, fs.ErrNotExist)
	_, err = w.OpenFile("/README.md/x", os.O_CREATE|os.O_WRONLY, 0o644)
	wantErr(t, "create under a file", err, syscall.ENOTDIR)
	_, err = w.OpenFile("/", os.O_CREATE|os.O_WRONLY, 0o644)
	wantErr(t, "create root", err, syscall.EISDIR)
	_, err = w.OpenFile("/src", os.O_WRONLY, 0)
	wantErr(t, "write a dir", err, syscall.EISDIR)

	// O_CREATE through a dangling link creates its target; with O_EXCL
	// it fails.
	_, err = w.OpenFile("/dangling", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	wantErr(t, "O_EXCL on a link", err, fs.ErrExist)
	h, err := w.OpenFile("/dangling", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	h.Write([]byte("somewhere"))
	h.Close()
	if got := wsRead(t, w, "/nowhere"); got != "somewhere" {
		t.Errorf("link target = %q", got)
	}
	// A loop of links cannot be created through.
	w.Symlink("loop2", "/loop1")
	w.Symlink("loop1", "/loop2")
	_, err = w.OpenFile("/loop1", os.O_CREATE|os.O_WRONLY, 0o644)
	wantErr(t, "create through a loop", err, syscall.ELOOP)

	// A file opened O_RDONLY|O_CREATE exists and is empty.
	h, err = w.OpenFile("/ro", os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Errorf("write read-only new file = %v", err)
	}
	h.Close()
	if fi, err := w.Stat("/ro"); err != nil || fi.Mode() != 0o600 || fi.Size() != 0 {
		t.Errorf("ro = %v %v", fi, err)
	}
}

// TestWorkspaceRemove covers removing files, links and directories,
// and recreating what was removed.
func TestWorkspaceRemove(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Remove("/README.md"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "removed", firstErr(w.Stat("/README.md")), fs.ErrNotExist)
	wantErr(t, "remove again", w.Remove("/README.md"), fs.ErrNotExist)
	// Delete, then recreate: the new file, not the View's.
	wsWrite(t, w, "/README.md", "new readme")
	if got := wsRead(t, w, "/README.md"); got != "new readme" {
		t.Errorf("recreated = %q", got)
	}
	if err := w.Remove("/link"); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/src/main.go"); got != "package main\n" {
		t.Error("removing a link removed its target")
	}
	// rmdir only when the merged directory is empty.
	wantErr(t, "rmdir non-empty", w.Remove("/src/lib"), syscall.ENOTEMPTY)
	for _, n := range []string{"a.go", "b.go", "up"} {
		if err := w.Remove("/src/lib/" + n); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Remove("/src/lib"); err != nil {
		t.Fatalf("rmdir emptied = %v", err)
	}
	if err := w.Remove("/emptydir"); err != nil {
		t.Fatalf("rmdir empty view dir = %v", err)
	}
	w.Mkdir("/made", 0o755)
	wsWrite(t, w, "/made/x", "x")
	wantErr(t, "rmdir new non-empty", w.Remove("/made"), syscall.ENOTEMPTY)
	w.Remove("/made/x")
	if err := w.Remove("/made"); err != nil {
		t.Errorf("rmdir new empty = %v", err)
	}
	wantErr(t, "remove root", w.Remove("/"), syscall.EBUSY)
	wantErr(t, "remove under a file", w.Remove("/empty.txt/x"), syscall.ENOTDIR)
	wantErr(t, "remove missing parent", w.Remove("/nope/x"), fs.ErrNotExist)
	if got := wsNames(t, w, "/src"); !reflect.DeepEqual(got, []string{"main.go"}) {
		t.Errorf("src = %v", got)
	}
	if f.scratchFiles(t) != 1 {
		t.Errorf("scratch holds %d files, want the new README only", f.scratchFiles(t))
	}
}

func firstErr(_ any, err error) error { return err }

// TestWorkspaceOpaque: a View directory removed and made again starts
// empty; its View children do not show through.
func TestWorkspaceOpaque(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.RemoveAll("/src"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "removed tree", firstErr(w.Stat("/src/lib/a.go")), fs.ErrNotExist)
	if err := w.Mkdir("/src", 0o750); err != nil {
		t.Fatal(err)
	}
	if got := wsNames(t, w, "/src"); len(got) != 0 {
		t.Errorf("recreated dir lists %v", got)
	}
	wsWrite(t, w, "/src/main.go", "fresh")
	if got := wsNames(t, w, "/src"); !reflect.DeepEqual(got, []string{"main.go"}) {
		t.Errorf("src = %v", got)
	}
	if got := wsRead(t, w, "/dirlink/main.go"); got != "fresh" {
		t.Errorf("through link = %q", got)
	}
	// RemoveAll of a missing name, of a file, and of the root.
	if err := w.RemoveAll("/nope"); err != nil {
		t.Errorf("removeall missing = %v", err)
	}
	if err := w.RemoveAll("/nope/deeper"); err != nil {
		t.Errorf("removeall missing parent = %v", err)
	}
	if err := w.RemoveAll(""); err != nil {
		t.Errorf("removeall empty = %v", err)
	}
	if err := w.RemoveAll("/empty.txt"); err != nil {
		t.Errorf("removeall file = %v", err)
	}
	wantErr(t, "removeall root", w.RemoveAll("/"), syscall.EBUSY)
	wantErr(t, "removeall under a file", w.RemoveAll("/README.md/x"), syscall.ENOTDIR)
	if got := mustRead(t, f.v, "/src/lib/a.go"); got != fixtureFiles["src/lib/a.go"] {
		t.Error("view changed")
	}
}

// TestWorkspaceRename covers files, links, and whole directories,
// within and across directories, over existing names.
func TestWorkspaceRename(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	// A View directory with a change inside, moved: the subtree moves
	// with its change and the old path is gone.
	wsWrite(t, w, "/src/lib/a.go", "changed a")
	if err := w.Rename("/src/lib", "/lib"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "old dir", firstErr(w.Stat("/src/lib")), fs.ErrNotExist)
	wantErr(t, "old file", firstErr(w.Stat("/src/lib/b.go")), fs.ErrNotExist)
	if got := wsRead(t, w, "/lib/a.go"); got != "changed a" {
		t.Errorf("moved change = %q", got)
	}
	if got := wsRead(t, w, "/lib/b.go"); got != fixtureFiles["src/lib/b.go"] {
		t.Errorf("moved view file = %q", got)
	}
	// up -> ../main.go now resolves against /lib, where there is none.
	wantErr(t, "moved relative link", firstErr(w.Stat("/lib/up")), fs.ErrNotExist)
	// Rename a file over another; the replaced one is gone.
	if err := w.Rename("/lib/b.go", "/README.md"); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/README.md"); got != fixtureFiles["src/lib/b.go"] {
		t.Errorf("replaced = %q", got)
	}
	// Rename a link: it is renamed, not its target.
	if err := w.Rename("/link", "/link2"); err != nil {
		t.Fatal(err)
	}
	if target, _ := w.Readlink("/link2"); target != "src/main.go" {
		t.Errorf("renamed link -> %q", target)
	}
	// A directory over an empty directory, and back.
	if err := w.Rename("/lib", "/emptydir"); err != nil {
		t.Fatalf("dir over empty dir: %v", err)
	}
	if got := wsRead(t, w, "/emptydir/a.go"); got != "changed a" {
		t.Errorf("after dir over dir = %q", got)
	}
	// Renaming to itself does nothing.
	if err := w.Rename("/emptydir", "/emptydir"); err != nil {
		t.Errorf("rename to self = %v", err)
	}
	// Errors.
	w.Mkdir("/full", 0o755)
	wsWrite(t, w, "/full/x", "x")
	check := func(what string, err, want error) {
		t.Helper()
		var le *os.LinkError
		if !errors.As(err, &le) {
			t.Errorf("%s: %T is not a *os.LinkError", what, err)
		}
		wantErr(t, what, err, want)
	}
	check("dir over non-empty dir", w.Rename("/emptydir", "/full"), syscall.ENOTEMPTY)
	check("dir over file", w.Rename("/emptydir", "/README.md"), syscall.ENOTDIR)
	check("file over dir", w.Rename("/README.md", "/full"), syscall.EISDIR)
	check("dir into itself", w.Rename("/emptydir", "/emptydir/sub"), syscall.EINVAL)
	check("missing", w.Rename("/nope", "/x"), fs.ErrNotExist)
	check("missing parent", w.Rename("/README.md", "/nope/x"), fs.ErrNotExist)
	check("bad old parent", w.Rename("/nope/x", "/y"), fs.ErrNotExist)
	check("root", w.Rename("/", "/x"), syscall.EBUSY)
	// The View is untouched.
	if names(t, f.v, "/src/lib") == nil {
		t.Error("view lost a directory")
	}
}

// TestWorkspaceRenameOpenFile: a handle follows its file across a
// rename, and a removed file stays readable through an open handle
// until it is closed.
func TestWorkspaceRenameOpenFile(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	h, err := w.OpenFile("/README.md", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Rename("/README.md", "/moved.md"); err != nil {
		t.Fatal(err)
	}
	h.WriteAt([]byte("J"), 0)
	if got := wsRead(t, w, "/moved.md"); got != "Jello\n" {
		t.Errorf("write after rename = %q", got)
	}
	if err := w.Remove("/moved.md"); err != nil {
		t.Fatal(err)
	}
	if f.scratchFiles(t) != 1 {
		t.Error("scratch dropped while open")
	}
	buf := make([]byte, 6)
	if n, err := h.ReadAt(buf, 0); n != 6 || string(buf) != "Jello\n" {
		t.Errorf("read removed = %q %v", buf[:n], err)
	}
	h.Close()
	if f.scratchFiles(t) != 0 {
		t.Error("scratch kept after last close")
	}
	if err := h.Close(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("second close = %v", err)
	}
	if _, err := h.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("stat closed = %v", err)
	}
	if _, err := h.Write([]byte("x")); err == nil {
		t.Error("write after close")
	}
}

func TestWorkspaceSymlinks(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Symlink("lib/a.go", "/src/alink"); err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, w, "/src/alink"); got != fixtureFiles["src/lib/a.go"] {
		t.Errorf("new link reads %q", got)
	}
	fi, err := w.Lstat("/src/alink")
	if err != nil || fi.Mode()&fs.ModeSymlink == 0 || fi.Size() != 8 {
		t.Errorf("lstat = %v %v", fi, err)
	}
	if e := fi.Sys().(*c4m.Entry); e.Target != "lib/a.go" || e.Name != "alink" {
		t.Errorf("sys = %+v", e)
	}
	var le *os.LinkError
	if err := w.Symlink("x", "/src/alink"); !errors.As(err, &le) || !errors.Is(err, fs.ErrExist) {
		t.Errorf("symlink over existing = %v", err)
	}
	wantErr(t, "symlink at root", w.Symlink("x", "/"), fs.ErrExist)
	wantErr(t, "symlink in missing dir", w.Symlink("x", "/nope/l"), fs.ErrNotExist)
	if _, err := w.Readlink("/src/main.go"); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("readlink of a file = %v", err)
	}
	if _, err := w.Readlink("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("readlink missing = %v", err)
	}
	// A loop, and an empty target.
	w.Symlink("/self", "/self")
	wantErr(t, "loop", firstErr(w.Stat("/self")), syscall.ELOOP)
	w.Symlink("", "/empty-link")
	wantErr(t, "empty target", firstErr(w.Stat("/empty-link")), fs.ErrNotExist)
	// Walking through a file fails.
	wantErr(t, "through a file", firstErr(w.Stat("/README.md/x")), syscall.ENOTDIR)
	// A link to a directory lists it.
	if got := wsNames(t, w, "/dirlink"); !reflect.DeepEqual(got, []string{"alink", "lib", "main.go"}) {
		t.Errorf("dirlink = %v", got)
	}
}

// TestWorkspaceMkdir covers Mkdir, MkdirAll and deep trees.
func TestWorkspaceMkdir(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wantErr(t, "mkdir existing", w.Mkdir("/src", 0o755), fs.ErrExist)
	wantErr(t, "mkdir root", w.Mkdir("/", 0o755), fs.ErrExist)
	wantErr(t, "mkdir over link", w.Mkdir("/dangling", 0o755), fs.ErrExist)
	wantErr(t, "mkdir in missing", w.Mkdir("/a/b", 0o755), fs.ErrNotExist)
	deep := "/d"
	for i := 0; i < 64; i++ {
		deep += "/level"
	}
	if err := w.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	wsWrite(t, w, deep+"/leaf.txt", "leaf")
	if got := wsRead(t, w, deep+"/leaf.txt"); got != "leaf" {
		t.Errorf("deep = %q", got)
	}
	if err := w.MkdirAll("/src/lib", 0o755); err != nil {
		t.Errorf("mkdirall existing = %v", err)
	}
	if err := w.MkdirAll("/dirlink/lib/x", 0o755); err != nil {
		t.Errorf("mkdirall through link = %v", err)
	}
	if _, err := w.Stat("/src/lib/x"); err != nil {
		t.Errorf("made through link: %v", err)
	}
	wantErr(t, "mkdirall through a file", w.MkdirAll("/README.md/x", 0o755), syscall.ENOTDIR)
	wantErr(t, "mkdirall empty", w.MkdirAll("", 0o755), fs.ErrNotExist)
	w.Symlink("loop", "/loop")
	// The loop exists, so os.MkdirAll's Mkdir of it reports EEXIST.
	wantErr(t, "mkdirall through a loop", w.MkdirAll("/loop/x", 0o755), fs.ErrExist)
	if err := w.RemoveAll("/d"); err != nil {
		t.Fatal(err)
	}
	if f.scratchFiles(t) != 0 {
		t.Error("RemoveAll left scratch files")
	}
}

// TestWorkspaceDirHandles reads directories through handles, with
// os.File's pagination.
func TestWorkspaceDirHandles(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/src/z.go", "z")
	w.Remove("/src/main.go")
	for _, dir := range []string{"/src", "/src/lib", "/"} {
		h, err := w.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		all, err := h.Readdirnames(-1)
		if err != nil {
			t.Fatal(err)
		}
		if want := wsNames(t, w, dir); !reflect.DeepEqual(all, want) {
			t.Errorf("%s: %v, want %v", dir, all, want)
		}
		h.Seek(0, io.SeekStart)
		var paged []string
		for {
			ents, err := h.ReadDir(1)
			if err == io.EOF {
				break
			}
			if err != nil || len(ents) != 1 {
				t.Fatal(ents, err)
			}
			paged = append(paged, ents[0].Name())
		}
		if !reflect.DeepEqual(paged, all) {
			t.Errorf("%s paged %v", dir, paged)
		}
		h.Seek(0, io.SeekStart)
		infos, err := h.Readdir(0)
		if err != nil || len(infos) != len(all) {
			t.Errorf("Readdir = %d %v", len(infos), err)
		}
		if fi, err := h.Stat(); err != nil || !fi.IsDir() {
			t.Errorf("dir stat = %v %v", fi, err)
		}
		if _, err := h.Read(make([]byte, 1)); !errors.Is(err, syscall.EISDIR) {
			t.Errorf("read dir = %v", err)
		}
		if _, err := h.ReadAt(make([]byte, 1), 0); !errors.Is(err, syscall.EISDIR) {
			t.Errorf("readat dir = %v", err)
		}
		if _, err := h.Write([]byte("x")); !errors.Is(err, syscall.EISDIR) {
			t.Errorf("write dir = %v", err)
		}
		if _, err := h.WriteAt([]byte("x"), 0); !errors.Is(err, syscall.EISDIR) {
			t.Errorf("writeat dir = %v", err)
		}
		if err := h.Truncate(0); !errors.Is(err, syscall.EISDIR) {
			t.Errorf("truncate dir = %v", err)
		}
		if _, err := h.Seek(1, io.SeekStart); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("seek dir = %v", err)
		}
		if err := h.Sync(); err != nil {
			t.Errorf("sync dir = %v", err)
		}
		if h.Name() != dir {
			t.Errorf("name %q", h.Name())
		}
		h.Close()
		if _, err := h.ReadDir(-1); !errors.Is(err, os.ErrClosed) {
			t.Errorf("readdir closed = %v", err)
		}
		if _, err := h.Seek(0, io.SeekStart); !errors.Is(err, os.ErrClosed) {
			t.Errorf("seek closed = %v", err)
		}
		if err := h.Sync(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("sync closed = %v", err)
		}
		if _, err := h.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("stat closed = %v", err)
		}
		if _, err := h.Read(nil); !errors.Is(err, os.ErrClosed) {
			t.Errorf("read closed = %v", err)
		}
	}
	h, _ := w.Open("/README.md")
	if _, err := h.ReadDir(-1); !errors.Is(err, syscall.ENOTDIR) {
		t.Errorf("readdir of a file = %v", err)
	}
	if err := h.Sync(); err != nil {
		t.Errorf("sync file = %v", err)
	}
	if _, err := h.Seek(2, io.SeekStart); err != nil {
		t.Errorf("seek file = %v", err)
	}
	if err := h.Truncate(0); !errors.Is(err, syscall.EBADF) {
		t.Errorf("truncate read handle = %v", err)
	}
	h.Close()
	if err := h.Truncate(0); !errors.Is(err, os.ErrClosed) {
		t.Errorf("truncate closed read handle = %v", err)
	}
	wantErr(t, "readdir of a file", firstErr(w.ReadDir("/README.md")), syscall.ENOTDIR)
	wantErr(t, "readdir missing", firstErr(w.ReadDir("/nope")), fs.ErrNotExist)
	// A changed file read through a handle, its Stat, and Sync.
	h, _ = w.OpenFile("/src/z.go", os.O_RDWR, 0)
	if fi, err := h.Stat(); err != nil || fi.Size() != 1 || fi.Name() != "z.go" {
		t.Errorf("changed stat = %v %v", fi, err)
	}
	if err := h.Sync(); err != nil {
		t.Errorf("sync = %v", err)
	}
	if err := h.Truncate(3); err != nil {
		t.Errorf("truncate = %v", err)
	}
	h.Close()
	if fi, _ := w.Stat("/src/z.go"); fi.Size() != 3 {
		t.Errorf("size after handle truncate = %d", fi.Size())
	}
}

// TestWorkspaceIOFS runs the standard library's checker over a changed
// workspace through Sub.
func TestWorkspaceIOFS(t *testing.T) {
	w := newWS(t, conformanceView(t))
	wsWrite(t, w, "/src/new.go", "package main\n")
	w.Remove("/src/lib/a.go")
	w.Rename("/src/lib/deep", "/deep")
	w.Chmod("/README.md", 0o600)
	fsys, err := w.Sub("/")
	if err != nil {
		t.Fatal(err)
	}
	if err := fstest.TestFS(fsys, "README.md", "src/new.go", "src/main.go", "deep/z.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceRelativePaths(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Chdir("/src"); err != nil {
		t.Fatal(err)
	}
	if wd, _ := w.Getwd(); wd != "/src" {
		t.Errorf("wd = %q", wd)
	}
	wsWrite(t, w, "rel.txt", "relative")
	if got := wsRead(t, w, "/src/rel.txt"); got != "relative" {
		t.Errorf("rel = %q", got)
	}
	wantErr(t, "chdir to a file", w.Chdir("/README.md"), syscall.ENOTDIR)
	wantErr(t, "chdir missing", w.Chdir("/nope"), fs.ErrNotExist)
	wantErr(t, "stat empty", firstErr(w.Stat("")), fs.ErrNotExist)
	wantErr(t, "mkdir empty", w.Mkdir("", 0o755), fs.ErrNotExist)
	wantErr(t, "rename empty", w.Rename("", "x"), fs.ErrNotExist)
	wantErr(t, "rename to empty", w.Rename("rel.txt", ""), fs.ErrNotExist)
}

// TestWorkspaceClose: Close drops the scratch files and makes every
// operation fail; open handles keep working until closed.
func TestWorkspaceClose(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/a", "a")
	h, _ := w.OpenFile("/b", os.O_CREATE|os.O_RDWR, 0o644)
	h.Write([]byte("b"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("second close = %v", err)
	}
	if f.scratchFiles(t) != 1 {
		t.Errorf("%d scratch files, want the open one", f.scratchFiles(t))
	}
	if _, err := h.WriteAt([]byte("B"), 0); err != nil {
		t.Errorf("open handle after Close = %v", err)
	}
	h.Close()
	if f.scratchFiles(t) != 0 {
		t.Error("scratch kept")
	}
	for what, err := range map[string]error{
		"stat":      firstErr(w.Stat("/a")),
		"open":      firstErr(w.Open("/README.md")),
		"create":    firstErr(w.Create("/c")),
		"mkdir":     w.Mkdir("/d", 0o755),
		"mkdirall":  w.MkdirAll("/d/e", 0o755),
		"remove":    w.Remove("/README.md"),
		"removeall": w.RemoveAll("/src"),
		"rename":    w.Rename("/README.md", "/x"),
		"symlink":   w.Symlink("x", "/y"),
		"chmod":     w.Chmod("/README.md", 0),
		"capture":   firstErr(w.Capture(f.st)),
	} {
		wantErr(t, what, err, os.ErrClosed)
	}
}

func TestNewWorkspaceArgs(t *testing.T) {
	if _, err := NewWorkspace(nil, newScratch(t)); err == nil {
		t.Error("nil view accepted")
	}
	v, _ := New(c4m.NewManifest(), nil)
	if _, err := NewWorkspace(v, nil); err == nil {
		t.Error("nil scratch accepted")
	}
}

// TestWorkspaceMissingContent: a View file whose content is not in the
// store lists and stats, fails to open for writing (there is nothing to
// copy), and can still be truncated or replaced.
func TestWorkspaceMissingContent(t *testing.T) {
	m := parse(t, "-rw-r--r-- 2025-01-01T00:00:00Z 5 gone.txt "+idOf("gone!").String()+"\n")
	v, err := New(m, newTreeStore(t))
	if err != nil {
		t.Fatal(err)
	}
	w := newWS(t, v)
	if fi, err := w.Stat("/gone.txt"); err != nil || fi.Size() != 5 {
		t.Errorf("stat = %v %v", fi, err)
	}
	_, err = w.OpenFile("/gone.txt", os.O_WRONLY, 0)
	wantErr(t, "copy-up of missing content", err, ErrContentMissing)
	if err := w.Truncate("/gone.txt", 0); err != nil {
		t.Errorf("truncate = %v", err)
	}
	wsWrite(t, w, "/gone.txt", "back!")
	if got := wsRead(t, w, "/gone.txt"); got != "back!" {
		t.Errorf("replaced = %q", got)
	}
}

// failingScratch is a scratch filesystem whose file creation fails.
type failingScratch struct {
	absfs.FileSystem
	err error
}

func (f failingScratch) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	if flag&os.O_CREATE != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: f.err}
	}
	return f.FileSystem.OpenFile(name, flag, perm)
}

func TestWorkspaceScratchFailure(t *testing.T) {
	f := newFixture(t)
	v, _ := Open(f.st, f.root)
	full := errors.New("scratch full")
	w, _ := NewWorkspace(v, failingScratch{newScratch(t), full})
	_, err := w.Create("/new")
	wantErr(t, "create", err, full)
	_, err = w.OpenFile("/README.md", os.O_WRONLY, 0)
	wantErr(t, "copy-up", err, full)
	if _, err := w.Stat("/new"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("failed create left a name: %v", err)
	}
	// A metadata change needs no scratch.
	if err := w.Chmod("/README.md", 0o600); err != nil {
		t.Error(err)
	}
}

// TestWorkspaceSysEntry: Stat's Sys is a c4m entry, with the C4 ID of
// unchanged content and a null ID for content not yet hashed.
func TestWorkspaceSysEntry(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	w.Chmod("/README.md", 0o600)
	fi, _ := w.Stat("/README.md")
	if e := fi.Sys().(*c4m.Entry); e.C4ID != idOf("hello\n") || e.Mode != 0o600 || e.Name != "README.md" {
		t.Errorf("metadata-only sys = %+v", e)
	}
	wsWrite(t, w, "/README.md", "changed")
	fi, _ = w.Stat("/README.md")
	if e := fi.Sys().(*c4m.Entry); !e.C4ID.IsNil() || e.Size != 7 {
		t.Errorf("dirty sys = %+v", e)
	}
	if _, err := w.Capture(f.st); err != nil {
		t.Fatal(err)
	}
	fi, _ = w.Stat("/README.md")
	if e := fi.Sys().(*c4m.Entry); e.C4ID != idOf("changed") {
		t.Errorf("captured sys = %+v", e)
	}
	w.Mkdir("/newdir", 0o755)
	fi, _ = w.Stat("/newdir")
	if e := fi.Sys().(*c4m.Entry); e.Name != "newdir/" || !fi.IsDir() || fi.Size() != 0 {
		t.Errorf("dir sys = %+v", e)
	}
	ents, _ := w.ReadDir("/")
	for _, e := range ents {
		if e.Name() == "newdir" {
			if s := e.(interface{ String() string }).String(); !strings.Contains(s, "newdir") {
				t.Errorf("String = %q", s)
			}
			if fi, err := e.Info(); err != nil || fi.Name() != "newdir" || e.Type() != fs.ModeDir {
				t.Errorf("info = %v %v", fi, err)
			}
		}
	}
}

var _ = bytes.NewReader
var _ c4.ID
