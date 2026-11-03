package c4fs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// TestCaptureSequenceCopiedMember: a sequence member opened for writing
// but never written, then given a new mode, is captured with its size,
// exactly as c4 id identifies the same tree on disk.
func TestCaptureSequenceCopiedMember(t *testing.T) {
	dir, st := t.TempDir(), newTreeStore(t)
	files := map[string]string{"notes.txt": "n"}
	for i := 1; i <= 4; i++ {
		files[fmt.Sprintf("plates/shot.%04d.exr", i)] = strings.Repeat("p", i)
	}
	writeTree(t, dir, files)
	m, self, _ := snapshot(t, dir, st, true)
	w := newWS(t, mustOpen(t, st, self))
	h, err := w.OpenFile("/plates/shot.0002.exr", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Chmod("/plates/shot.0002.exr", 0o600); err != nil {
		t.Fatal(err)
	}
	cs := capture(t, w, st)
	got := applyAll(t, m, cs)
	for _, e := range got.Entries {
		if e.Size < 0 {
			t.Errorf("%s has a null size", e.Name)
		}
	}
	// The same change on disk, identified as c4 id does.
	if err := os.Chmod(filepath.Join(dir, "plates", "shot.0002.exr"), 0o600); err != nil {
		t.Fatal(err)
	}
	if want := scanTree(t, dir).ComputeC4ID(); cs.ID != want {
		t.Errorf("captured %s, c4 id gives %s\n%s", cs.ID, want, cs.Text())
	}
}

// TestCaptureNullDirectoryMoved: a View directory with a null C4 ID
// moved over another with the same name, mode and time. The entries are
// identical, which in a patch would mean removal; the patch must instead
// change the directory and keep the moved children.
func TestCaptureNullDirectoryMoved(t *testing.T) {
	for _, to := range []string{"dir001/sub00", "sub00", "made/sub00"} {
		t.Run(to, func(t *testing.T) {
			m := synthetic(2, 1, 2)
			// A top-level twin of the subdirectories, with a file of its
			// own: the same name, mode and time, and a null C4 ID.
			ts := m.Entries[1].Timestamp
			m.Entries = append(m.Entries,
				&c4m.Entry{Name: "sub00/", Mode: fs.ModeDir | 0o755, Timestamp: ts, Size: -1},
				&c4m.Entry{Name: "other.go", Depth: 1, Mode: 0o644, Timestamp: ts, Size: 5, C4ID: idOf("other")})
			w := newWS(t, mustNew(t, m, nil))
			if err := w.RemoveAll("/" + to); err != nil {
				t.Fatal(err)
			}
			// Into a directory the workspace made: one with no View node.
			if err := w.MkdirAll("/made", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := w.Rename("/dir000/sub00", "/"+to); err != nil {
				t.Fatal(err)
			}
			cs := capture(t, w, newTreeStore(t))
			got := applyAll(t, m, cs) // checks the closing ID too
			for _, name := range []string{"file000.go", "file001.go"} {
				moved := m.GetEntry("dir000/sub00/" + name)
				if e := got.GetEntry(to + "/" + name); e == nil || e.C4ID != moved.C4ID {
					t.Errorf("%s/%s = %+v, want the moved file\n%s", to, name, e, cs.Text())
				}
			}
			if got.GetEntry("dir000/sub00/") != nil {
				t.Errorf("dir000/sub00 still there:\n%s", mustMarshal(t, got))
			}
			if to == "sub00" && got.GetEntry("sub00/other.go") != nil {
				t.Errorf("the replaced directory's file survived:\n%s", mustMarshal(t, got))
			}
			full, err := cs.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			if full.ComputeC4ID() != cs.ID {
				t.Errorf("captured manifest is %s, changeset says %s", full.ComputeC4ID(), cs.ID)
			}
		})
	}
}

// TestWorkspaceChtimesZero: a zero mtime leaves the time alone, as
// os.Chtimes does since Go 1.23.
func TestWorkspaceChtimesZero(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	before, _ := w.Stat("/README.md")
	if err := w.Chtimes("/README.md", time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := w.Chtimes("/README.md", time.Now(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if after, _ := w.Stat("/README.md"); !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("mtime %v, was %v", after.ModTime(), before.ModTime())
	}
	if cs := capture(t, w, f.st); !cs.Empty() {
		t.Errorf("zero chtimes changed the tree:\n%s", cs.Text())
	}
	wantErr(t, "chtimes zero, missing", w.Chtimes("/nope", time.Time{}, time.Time{}), fs.ErrNotExist)
}

// TestWorkspaceHandleOffsets: negative offsets are EINVAL on every kind
// of handle, as for an *os.File, whatever the scratch filesystem does.
func TestWorkspaceHandleOffsets(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	for _, tc := range []struct {
		name string
		open func() (absfs.File, error)
	}{
		{"scratch", func() (absfs.File, error) { return w.OpenFile("/README.md", os.O_RDWR, 0) }},
		{"view", func() (absfs.File, error) { return w.Open("/docs/guide.txt") }},
	} {
		h, err := tc.open()
		if err != nil {
			t.Fatal(err)
		}
		size := int64(len(wsRead(t, w, h.Name())))
		if _, err := h.ReadAt(make([]byte, 1), -5); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("%s: ReadAt(-5) = %v", tc.name, err)
		}
		if tc.name == "scratch" {
			if _, err := h.WriteAt([]byte("x"), -5); !errors.Is(err, syscall.EINVAL) {
				t.Errorf("%s: WriteAt(-5) = %v", tc.name, err)
			}
		}
		for _, s := range []struct {
			off    int64
			whence int
		}{{-5, io.SeekStart}, {-size - 1, io.SeekEnd}, {-1, io.SeekCurrent}, {math.MaxInt64, io.SeekEnd}, {0, 7}} {
			if _, err := h.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if pos, err := h.Seek(s.off, s.whence); !errors.Is(err, syscall.EINVAL) {
				t.Errorf("%s: Seek(%d, %d) = %d %v", tc.name, s.off, s.whence, pos, err)
			}
		}
		if pos, err := h.Seek(3, io.SeekStart); pos != 3 || err != nil {
			t.Errorf("%s: Seek(3) = %d %v", tc.name, pos, err)
		}
		if pos, err := h.Seek(-1, io.SeekCurrent); pos != 2 || err != nil {
			t.Errorf("%s: Seek(-1, current) = %d %v", tc.name, pos, err)
		}
		if pos, err := h.Seek(-1, io.SeekEnd); pos != size-1 || err != nil {
			t.Errorf("%s: Seek(-1, end) = %d %v", tc.name, pos, err)
		}
		b := make([]byte, 1)
		if n, err := h.Read(b); n != 1 || err != nil {
			t.Errorf("%s: read at end-1 = %d %v", tc.name, n, err)
		}
		if _, err := h.Read(b); err != io.EOF {
			t.Errorf("%s: read at end = %v, want io.EOF itself", tc.name, err)
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Seek(0, io.SeekCurrent); err == nil {
			t.Errorf("%s: seek on a closed handle", tc.name)
		}
		if _, err := h.Seek(0, io.SeekEnd); err == nil {
			t.Errorf("%s: seek to end on a closed handle", tc.name)
		}
	}
}

// ioFailing is a scratch filesystem whose files fail every write, sync
// and truncate with a path error naming the scratch file.
type ioFailing struct {
	absfs.FileSystem
	err error
}

func (f ioFailing) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	h, err := f.FileSystem.OpenFile(name, flag, perm)
	if err != nil || flag&os.O_CREATE != 0 {
		return h, err
	}
	return ioFailingFile{h, f.err}, nil
}

type ioFailingFile struct {
	absfs.File
	err error
}

func (f ioFailingFile) fail(op string) error {
	return &fs.PathError{Op: op, Path: f.File.Name(), Err: f.err}
}
func (f ioFailingFile) Write([]byte) (int, error)          { return 0, f.fail("write") }
func (f ioFailingFile) WriteAt([]byte, int64) (int, error) { return 0, f.fail("write") }
func (f ioFailingFile) Read([]byte) (int, error)           { return 0, f.fail("read") }
func (f ioFailingFile) ReadAt([]byte, int64) (int, error)  { return 0, f.fail("read") }
func (f ioFailingFile) Sync() error                        { return f.fail("sync") }
func (f ioFailingFile) Truncate(int64) error               { return f.fail("truncate") }
func (f ioFailingFile) Close() error                       { f.File.Close(); return f.fail("close") }

// TestWorkspaceHandleErrors: an error from a scratch file names the
// workspace path, not the scratch file, and keeps its cause.
func TestWorkspaceHandleErrors(t *testing.T) {
	f := newFixture(t)
	disk := errors.New("disk error")
	w, err := NewWorkspace(mustOpen(t, f.st, f.root), ioFailing{newScratch(t), disk})
	if err != nil {
		t.Fatal(err)
	}
	h, err := w.OpenFile("/README.md", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	for op, err := range map[string]error{
		"write":    firstErr(h.Write(b)),
		"writeat":  firstErr(h.WriteAt(b, 0)),
		"read":     firstErr(h.Read(b)),
		"readat":   firstErr(h.ReadAt(b, 0)),
		"sync":     h.Sync(),
		"truncate": h.Truncate(1),
		"close":    h.Close(),
	} {
		var pe *fs.PathError
		if !errors.As(err, &pe) || pe.Path != "/README.md" || !errors.Is(err, disk) {
			t.Errorf("%s: %v, want a path error on /README.md caused by %v", op, err, disk)
		}
	}
	// A failed write is no change.
	if cs := capture(t, w, f.st); !cs.Empty() {
		t.Errorf("failed writes changed the tree:\n%s", cs.Text())
	}
}

// TestWorkspaceHandleSeesChanges: a handle opened before a change sees
// it, as fstat and rewinddir do on a real filesystem, even when the
// handle was opened on a name the workspace had not touched yet.
func TestWorkspaceHandleSeesChanges(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	d, err := w.Open("/docs")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	file, err := w.Open("/README.md")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	wsWrite(t, w, "/docs/newfile", "n")
	if err := w.Chmod("/docs", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := w.Chmod("/README.md", 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Readdirnames(-1); err != nil || !reflect.DeepEqual(got, []string{"guide.txt", "newfile"}) {
		t.Errorf("listing after open = %v %v", got, err)
	}
	if fi, err := d.Stat(); err != nil || fi.Mode() != fs.ModeDir|0o700 {
		t.Errorf("dir fstat = %v %v", fi, err)
	}
	if fi, err := file.Stat(); err != nil || fi.Mode() != 0o600 {
		t.Errorf("file fstat = %v %v", fi, err)
	}
	// A pass in progress is a snapshot; a rewind lists afresh.
	src, err := w.Open("/src")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	first, err := src.Readdirnames(1)
	if err != nil {
		t.Fatal(err)
	}
	wsWrite(t, w, "/src/zz", "z")
	rest, err := src.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	if got := append(first, rest...); !reflect.DeepEqual(got, []string{"lib", "main.go"}) {
		t.Errorf("one pass = %v", got)
	}
	src.Seek(0, io.SeekStart)
	if got, _ := src.Readdirnames(-1); !reflect.DeepEqual(got, []string{"lib", "main.go", "zz"}) {
		t.Errorf("after rewind = %v", got)
	}
	// A directory renamed after it was opened: the handle follows it.
	lib, err := w.Open("/src/lib")
	if err != nil {
		t.Fatal(err)
	}
	defer lib.Close()
	if err := w.Rename("/src/lib", "/lib"); err != nil {
		t.Fatal(err)
	}
	wsWrite(t, w, "/lib/c.go", "c")
	if got, _ := lib.Readdirnames(-1); !reflect.DeepEqual(got, []string{"a.go", "b.go", "c.go", "up"}) {
		t.Errorf("renamed dir via old handle = %v", got)
	}
}

// TestWorkspaceReadOnlyOpenFlags: O_CREATE or O_APPEND with a read-only
// access mode cannot write, so they copy nothing.
func TestWorkspaceReadOnlyOpenFlags(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	for _, flag := range []int{os.O_RDONLY | os.O_CREATE, os.O_APPEND, os.O_RDONLY | os.O_CREATE | os.O_APPEND} {
		h, err := w.OpenFile("/README.md", flag, 0o644)
		if err != nil {
			t.Fatalf("flag %#x: %v", flag, err)
		}
		if b, err := io.ReadAll(h); err != nil || string(b) != "hello\n" {
			t.Errorf("flag %#x: read %q %v", flag, b, err)
		}
		_, err = h.Write([]byte("x"))
		var pe *fs.PathError
		if !errors.Is(err, syscall.EBADF) || !errors.As(err, &pe) || pe.Path != "/README.md" {
			t.Errorf("flag %#x: write = %v, want EBADF on /README.md", flag, err)
		}
		h.Close()
	}
	if n := f.scratchFiles(t); n != 0 {
		t.Errorf("read-only opens made %d scratch files", n)
	}
	// On a changed file, the scratch content is read.
	wsWrite(t, w, "/src/main.go", "changed")
	h, err := w.OpenFile("/src/main.go", os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(h); string(b) != "changed" {
		t.Errorf("read = %q", b)
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Errorf("write = %v", err)
	}
	h.Close()
	// A missing name is created, empty, by a read-only O_CREATE.
	h, err = w.OpenFile("/fresh", os.O_RDONLY|os.O_CREATE, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	if fi, err := w.Stat("/fresh"); err != nil || fi.Size() != 0 || fi.Mode() != 0o640 {
		t.Errorf("created = %v %v", fi, err)
	}
	_, err = w.OpenFile("/README.md", os.O_RDONLY|os.O_CREATE|os.O_EXCL, 0o644)
	wantErr(t, "excl", err, fs.ErrExist)
	_, err = w.OpenFile("/docs", os.O_RDONLY|os.O_CREATE, 0o644)
	wantErr(t, "create on a directory", err, syscall.EISDIR)
}

// TestWorkspaceModeZero: a file's mode set to 0 reads back as 0 in the
// workspace; c4m records it as its null mode, read as 0644.
func TestWorkspaceModeZero(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Chmod("/README.md", 0); err != nil {
		t.Fatal(err)
	}
	h, err := w.OpenFile("/locked", os.O_CREATE|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	for _, name := range []string{"/README.md", "/locked"} {
		if fi, err := w.Stat(name); err != nil || fi.Mode() != 0 {
			t.Errorf("%s: mode %v %v, want ----------", name, fi.Mode(), err)
		}
	}
	if err := w.Chmod("/docs", 0); err != nil {
		t.Fatal(err)
	}
	if fi, _ := w.Stat("/docs"); fi.Mode() != fs.ModeDir {
		t.Errorf("dir mode %v", fi.Mode())
	}
	cs := capture(t, w, f.st)
	m, err := cs.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if e := m.GetEntry("README.md"); e == nil || e.Mode != 0 {
		t.Errorf("captured README.md = %+v, want the null mode", e)
	}
	if fi, _ := cs.View().Stat("/README.md"); fi.Mode() != 0o644 {
		t.Errorf("captured view reads mode %v", fi.Mode())
	}
	if fi, _ := w.Stat("/README.md"); fi.Mode() != 0 {
		t.Errorf("after capture the workspace reads %v", fi.Mode())
	}
	w.Chmod("/README.md", 0o640)
	if fi, _ := w.Stat("/README.md"); fi.Mode() != 0o640 {
		t.Errorf("mode set again = %v", fi.Mode())
	}
}

// TestWorkspaceRmdirPartlyRemoved: a View directory the workspace has
// taken over, with some but not all of its View children removed, is
// not empty.
func TestWorkspaceRmdirPartlyRemoved(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Remove("/src/lib/a.go"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "rmdir with b.go and up left", w.Remove("/src/lib"), syscall.ENOTEMPTY)
	if err := w.Chmod("/docs", 0o700); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "rmdir a changed dir with View children", w.Remove("/docs"), syscall.ENOTEMPTY)
	wantErr(t, "rename over it", w.Rename("/emptydir", "/docs"), syscall.ENOTEMPTY)
	w.Remove("/docs/guide.txt")
	if err := w.Remove("/docs"); err != nil {
		t.Errorf("rmdir emptied = %v", err)
	}
	wantErr(t, "removed", firstErr(w.Stat("/docs")), fs.ErrNotExist)
}

// gatedSource blocks Open of one ID until released, so a test can act
// while a copy-up is in flight.
type gatedSource struct {
	src     store.Source
	id      c4.ID
	started chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (g *gatedSource) Open(id c4.ID) (io.ReadCloser, error) {
	if id == g.id {
		g.once.Do(func() { close(g.started) })
		<-g.gate
	}
	return g.src.Open(id)
}

// TestCopyUpRaces pins what happens while a copy-up runs without the
// lock: a second opener waits for it, and a remove or rename meanwhile
// wins, without leaking the copy.
func TestCopyUpRaces(t *testing.T) {
	for _, tc := range []struct {
		name    string
		act     func(w *Workspace) error
		scratch int
	}{
		{"remove", func(w *Workspace) error { return w.Remove("/README.md") }, 0},
		{"rename", func(w *Workspace) error { return w.Rename("/README.md", "/moved.md") }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			src := &gatedSource{src: f.st, id: idOf("hello\n"), started: make(chan struct{}), gate: make(chan struct{})}
			v, err := Open(src, f.root, WithCache(newTestCache(t)))
			if err != nil {
				t.Fatal(err)
			}
			sc := newScratch(t)
			w, _ := NewWorkspace(v, sc)
			waiting := make(chan struct{})
			var once sync.Once
			testHookCopyWait = func() { once.Do(func() { close(waiting) }) }
			defer func() { testHookCopyWait = nil }()

			open := func() <-chan error {
				done := make(chan error, 1)
				go func() {
					h, err := w.OpenFile("/README.md", os.O_RDWR, 0)
					if err == nil {
						h.Close()
					}
					done <- err
				}()
				return done
			}
			first := open()
			<-src.started // the copy is in flight, unlocked
			second := open()
			<-waiting // the second opener waits for the copy
			if err := tc.act(w); err != nil {
				t.Fatal(err)
			}
			close(src.gate)
			wantErr(t, "copying opener", <-first, fs.ErrNotExist)
			wantErr(t, "waiting opener", <-second, fs.ErrNotExist)
			if ents, _ := sc.ReadDir("/"); len(ents) != tc.scratch {
				t.Errorf("scratch holds %d files, want %d", len(ents), tc.scratch)
			}
			wantErr(t, "old name", firstErr(w.Stat("/README.md")), fs.ErrNotExist)
			if tc.name == "rename" {
				if got := wsRead(t, w, "/moved.md"); got != "hello\n" {
					t.Errorf("moved = %q", got)
				}
				if cs := capture(t, w, f.st); cs.Files != 0 {
					t.Errorf("an unwritten copy was hashed: %d files", cs.Files)
				}
			}
		})
	}
}

// TestCaptureHardLinkMark: a file that carries a c4m hard-link mark
// loses it when its content changes; the others in its group keep it.
func TestCaptureHardLinkMark(t *testing.T) {
	st := newTreeStore(t)
	data := putBytes(t, st, []byte("data"))
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	m := &c4m.Manifest{Version: "1.0", Entries: []*c4m.Entry{
		{Name: "a", Mode: 0o644, Timestamp: ts, Size: 4, C4ID: data, HardLink: 1},
		{Name: "b", Mode: 0o644, Timestamp: ts, Size: 4, C4ID: data, HardLink: 1},
	}}
	w := newWS(t, mustNew(t, m, st))
	wsWrite(t, w, "/a", "new!")
	cs := capture(t, w, st)
	got := applyAll(t, m, cs)
	if e := got.GetEntry("a"); e == nil || e.HardLink != 0 {
		t.Errorf("a = %+v, want no hard-link mark", e)
	}
	if e := got.GetEntry("b"); e == nil || e.HardLink != 1 {
		t.Errorf("b = %+v, want its mark kept", e)
	}
	if b := mustRead(t, cs.View(), "/b"); b != "data" {
		t.Errorf("b reads %q", b)
	}
}
