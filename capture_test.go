package c4fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/scan"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// contents describes every name in a tree: its type, permissions, time
// in whole seconds, and its content or link target.
func contents(t testing.TB, fsys absfs.SymlinkFileSystem) map[string]string {
	t.Helper()
	out := map[string]string{}
	var visit func(dir string)
	visit = func(dir string) {
		ents, err := fsys.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		for _, e := range ents {
			p := path.Join(dir, e.Name())
			fi, err := fsys.Lstat(p)
			if err != nil {
				t.Fatalf("Lstat(%s): %v", p, err)
			}
			desc := fmt.Sprintf("%v %d", fi.Mode(), fi.ModTime().Unix())
			switch {
			case fi.IsDir():
				visit(p)
			case fi.Mode()&fs.ModeSymlink != 0:
				target, _ := fsys.Readlink(p)
				desc += " -> " + target
			default:
				f, err := fsys.Open(p)
				if err != nil {
					t.Fatalf("Open(%s): %v", p, err)
				}
				b, err := io.ReadAll(f)
				f.Close()
				if err != nil {
					t.Fatalf("Read(%s): %v", p, err)
				}
				desc += " " + string(b)
			}
			out[p] = desc
		}
	}
	visit("/")
	return out
}

func capture(t testing.TB, w *Workspace, st store.Store) *Changeset {
	t.Helper()
	cs, err := w.Capture(st)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	return cs
}

// applyAll applies changesets in turn with c4m's own patch code,
// checking each closing ID, and returns the final manifest.
func applyAll(t testing.TB, m *c4m.Manifest, css ...*Changeset) *c4m.Manifest {
	t.Helper()
	for i, cs := range css {
		if got := m.ComputeC4ID(); got != cs.Base {
			t.Fatalf("changeset %d: base %s, manifest is %s", i, cs.Base, got)
		}
		// Through text, as a stored changeset is read.
		sections, err := c4m.DecodePatchChain(bytes.NewReader(cs.Text()))
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sections {
			m = c4m.ApplyPatch(m, &c4m.Manifest{Version: "1.0", Entries: s.Entries})
		}
		if got := m.ComputeC4ID(); got != cs.ID {
			t.Fatalf("changeset %d: applied gives %s, changeset says %s\n%s", i, got, cs.ID, cs.Text())
		}
	}
	return m
}

// fsOps applies one script of changes to a directory on disk and to a
// workspace alike.
type fsOps interface {
	write(name, body string)
	appendTo(name, body string)
	remove(name string)
	removeAll(name string)
	rename(o, n string)
	mkdir(name string)
	chmod(name string, mode fs.FileMode)
	truncate(name string, size int64)
	chtimes(name string, t time.Time)
}

type diskOps struct {
	t   testing.TB
	dir string
}

func (d diskOps) p(name string) string { return filepath.Join(d.dir, filepath.FromSlash(name)) }
func (d diskOps) check(err error) {
	d.t.Helper()
	if err != nil {
		d.t.Fatal(err)
	}
}
func (d diskOps) write(name, body string) { d.check(os.WriteFile(d.p(name), []byte(body), 0o644)) }
func (d diskOps) appendTo(name, body string) {
	f, err := os.OpenFile(d.p(name), os.O_WRONLY|os.O_APPEND, 0)
	d.check(err)
	_, err = f.WriteString(body)
	d.check(err)
	d.check(f.Close())
}
func (d diskOps) remove(name string)                  { d.check(os.Remove(d.p(name))) }
func (d diskOps) removeAll(name string)               { d.check(os.RemoveAll(d.p(name))) }
func (d diskOps) rename(o, n string)                  { d.check(os.Rename(d.p(o), d.p(n))) }
func (d diskOps) mkdir(name string)                   { d.check(os.Mkdir(d.p(name), 0o755)) }
func (d diskOps) chmod(name string, mode fs.FileMode) { d.check(os.Chmod(d.p(name), mode)) }
func (d diskOps) truncate(name string, size int64)    { d.check(os.Truncate(d.p(name), size)) }
func (d diskOps) chtimes(name string, t time.Time)    { d.check(os.Chtimes(d.p(name), t, t)) }

type wsOps struct {
	t testing.TB
	w *Workspace
}

func (o wsOps) check(err error) {
	o.t.Helper()
	if err != nil {
		o.t.Fatal(err)
	}
}
func (o wsOps) write(name, body string) {
	f, err := o.w.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	o.check(err)
	_, err = f.Write([]byte(body))
	o.check(err)
	o.check(f.Close())
}
func (o wsOps) appendTo(name, body string) {
	f, err := o.w.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
	o.check(err)
	_, err = f.WriteString(body)
	o.check(err)
	o.check(f.Close())
}
func (o wsOps) remove(name string)                  { o.check(o.w.Remove(name)) }
func (o wsOps) removeAll(name string)               { o.check(o.w.RemoveAll(name)) }
func (o wsOps) rename(a, b string)                  { o.check(o.w.Rename(a, b)) }
func (o wsOps) mkdir(name string)                   { o.check(o.w.Mkdir(name, 0o755)) }
func (o wsOps) chmod(name string, mode fs.FileMode) { o.check(o.w.Chmod(name, mode)) }
func (o wsOps) truncate(name string, size int64)    { o.check(o.w.Truncate(name, size)) }
func (o wsOps) chtimes(name string, t time.Time)    { o.check(o.w.Chtimes(name, t, t)) }

// scanTree scans a directory as c4 id does and returns its manifest
// after a round trip through c4m text.
func scanTree(t testing.TB, dir string) *c4m.Manifest {
	t.Helper()
	m, err := scan.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return parse(t, string(mustMarshal(t, m)))
}

// captureTree is the scan comparison's starting tree: no symbolic
// links, whose IDs c4 takes from their targets.
var captureTree = map[string]string{
	"README.md":          "hello\n",
	"empty.txt":          "",
	"src/main.go":        "package main\n",
	"src/lib/a.go":       "package lib // a\n",
	"src/lib/b.go":       "package lib // b\n",
	"src/lib/deep/z.txt": strings.Repeat("z", 5000),
	"docs/guide.txt":     strings.Repeat("guide ", 1000),
	"docs/old/notes.txt": "old notes",
	"emptydir/":          "",
	"gone/x/y.txt":       "y",
}

// TestCaptureMatchesScan applies one script of changes to a real
// directory and to a workspace over its snapshot. The workspace's
// capture must be exactly what c4 id makes of the directory: the same
// ID, the same manifest; and its patch must take the old manifest to it
// under c4m's own patch code.
func TestCaptureMatchesScan(t *testing.T) {
	dir, st := t.TempDir(), newTreeStore(t)
	writeTree(t, dir, captureTree)
	base, _, root := snapshot(t, dir, st, false)
	v := mustOpen(t, st, root)
	w := newWS(t, v)
	script := func(o fsOps) {
		o.write("src/new.go", "package main // new\n")
		o.appendTo("README.md", "more\n")
		o.write("src/main.go", "package main // v2\n")
		o.truncate("docs/guide.txt", 12)
		o.chmod("src/lib/b.go", 0o600)
		o.rename("src/lib", "lib")
		o.rename("docs/old", "lib/old")
		o.remove("empty.txt")
		o.write("empty.txt", "") // deleted, then recreated
		o.removeAll("gone")
		o.mkdir("gone") // replaces the removed directory
		o.write("gone/fresh.txt", "fresh")
		o.mkdir("made")
		o.mkdir("made/deeper")
		o.write("made/deeper/leaf.txt", "leaf")
		o.rename("emptydir", "made/was-empty")
		o.remove("lib/deep/z.txt")
	}
	script(diskOps{t, dir})
	script(wsOps{t, w})

	// Give every name the same time on both sides: the script's own
	// times are the clock's, which differ.
	when := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(p, dir))
		if rel == "" {
			return nil
		}
		wsOps{t, w}.chtimes(rel, when)
		// Chmod follows symbolic links and there is no Lchmod: a link's
		// own mode is fixed, and chmodding it would change its target.
		if runtime.GOOS == "windows" && info.Mode()&os.ModeSymlink == 0 {
			// Windows keeps no Unix permission bits, only a read-only
			// attribute: files scan as -rw-rw-rw- or -r--r--r--, and
			// directories as drwxrwxrwx, whatever mode they were made
			// or chmodded with. Compare what Windows does keep, the
			// owner's write bit, then give the workspace the host's
			// mode so the rest of the comparison can be exact.
			fi, err := w.Lstat(rel)
			if err != nil {
				return err
			}
			if fi.Mode().Perm()&0o200 != info.Mode().Perm()&0o200 {
				t.Errorf("%s: workspace mode %v, disk %v", rel, fi.Mode(), info.Mode())
			}
			wsOps{t, w}.chmod(rel, info.Mode().Perm())
		}
		return os.Chtimes(p, when, when)
	})
	if err != nil {
		t.Fatal(err)
	}
	cs := capture(t, w, st)
	want := scanTree(t, dir)
	if cs.ID != want.ComputeC4ID() {
		t.Errorf("captured %s, c4 id gives %s", cs.ID, want.ComputeC4ID())
	}
	got, err := cs.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if g, wnt := string(mustMarshal(t, got)), string(mustMarshal(t, want)); g != wnt {
		t.Errorf("manifest differs:\n--- captured\n%s--- scanned\n%s", g, wnt)
	}
	applyAll(t, base, cs)
	if cs.Base != root {
		t.Errorf("base = %s, want the root record %s", cs.Base, root)
	}
	// Every file's content is in the store, under its ID.
	for _, e := range got.Entries {
		if !e.IsDir() && !e.C4ID.IsNil() && e.Size > 0 && !st.Has(e.C4ID) {
			t.Errorf("%s not stored", e.Name)
		}
	}
	// The captured View reads the same as the workspace and the disk.
	if a, b := contents(t, cs.View()), contents(t, w); !reflect.DeepEqual(a, b) {
		t.Errorf("captured view differs from the workspace:\n%v\n%v", sorted(a), sorted(b))
	}
}

// TestCaptureIncremental: each capture hashes only what was written
// since the last, and its patch applies to the last.
func TestCaptureIncremental(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	head, err := w.Head()
	if err != nil || head != f.root {
		t.Fatalf("head = %s %v, want %s", head, err, f.root)
	}
	if cs := capture(t, w, f.st); !cs.Empty() || cs.Base != f.root || cs.ID != f.root || cs.Text() != nil {
		t.Errorf("nothing changed, yet %+v", cs)
	}
	wsWrite(t, w, "/a.txt", "a")
	wsWrite(t, w, "/src/b.txt", "b")
	wsWrite(t, w, "/README.md", "readme")
	cs1 := capture(t, w, f.st)
	if cs1.Files != 3 || cs1.Bytes != 8 {
		t.Errorf("first capture hashed %d files, %d bytes", cs1.Files, cs1.Bytes)
	}
	// Times are kept to the second, as c4m text keeps them, so the
	// patch in memory and the patch as text agree.
	for _, e := range cs1.Patch.Entries {
		if e.Timestamp.Nanosecond() != 0 || e.Timestamp.Location() != time.UTC {
			t.Errorf("%s: time %v", e.Name, e.Timestamp)
		}
	}
	wsWrite(t, w, "/src/b.txt", "bb")
	w.Chmod("/a.txt", 0o600)
	w.Chmod("/docs/guide.txt", 0o600)
	cs2 := capture(t, w, f.st)
	if cs2.Files != 1 || cs2.Base != cs1.ID {
		t.Errorf("second capture hashed %d, base %s", cs2.Files, cs2.Base)
	}
	cs3 := capture(t, w, f.st)
	if !cs3.Empty() || cs3.Files != 0 || cs3.ID != cs2.ID {
		t.Errorf("third capture = %+v", cs3)
	}
	// Opening for writing without writing hashes, but changes nothing.
	h, _ := w.OpenFile("/src/main.go", os.O_RDWR, 0)
	h.Close()
	if cs := capture(t, w, f.st); !cs.Empty() {
		t.Errorf("open for write changed:\n%s", cs.Text())
	}
	if head, _ := w.Head(); head != cs2.ID {
		t.Errorf("head = %s", head)
	}
	applyAll(t, f.m, cs1, cs2, cs3)
}

// TestCapturePatchIsMinimal: a change deep in the tree is patched as
// the path to it and nothing else.
func TestCapturePatchIsMinimal(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/src/lib/a.go", "changed")
	cs := capture(t, w, f.st)
	var got []string
	for _, e := range cs.Patch.Entries {
		got = append(got, fmt.Sprintf("%d %s", e.Depth, e.Name))
	}
	// src/ and lib/ change ID and size; their times do not change.
	if want := []string{"0 src/", "1 lib/", "2 a.go"}; !reflect.DeepEqual(got, want) {
		t.Errorf("patch = %v, want %v", got, want)
	}
	if !strings.HasPrefix(string(cs.Text()), f.root.String()+"\n") ||
		!strings.HasSuffix(string(cs.Text()), cs.ID.String()+"\n") {
		t.Errorf("text is not framed by the IDs:\n%s", cs.Text())
	}
	var buf bytes.Buffer
	if n, err := cs.WriteTo(&buf); err != nil || n != int64(buf.Len()) || !bytes.Equal(buf.Bytes(), cs.Text()) {
		t.Errorf("WriteTo = %d %v", n, err)
	}
	applyAll(t, f.m, cs)
}

// TestCaptureRestored: a file removed and recreated as it was, and a
// directory whose time is put back, are no change at all.
func TestCaptureRestored(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	ok := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	orig, err := w.Stat("/src/main.go")
	ok(err)
	dirInfo, err := w.Stat("/src")
	ok(err)
	ok(w.Remove("/src/main.go"))
	wantErr(t, "removed", firstErr(w.Stat("/src/main.go")), fs.ErrNotExist)
	// Recreated with O_EXCL: the name really was gone.
	h, err := w.OpenFile("/src/main.go", os.O_CREATE|os.O_EXCL|os.O_WRONLY, orig.Mode())
	ok(err)
	_, err = h.Write([]byte("package main\n"))
	ok(err)
	ok(h.Close())
	ok(w.Chtimes("/src/main.go", orig.ModTime(), orig.ModTime()))
	ok(w.Chtimes("/src", dirInfo.ModTime(), dirInfo.ModTime()))
	// A metadata change undone: back to the scanned mode, which is the
	// host's (-rw-r--r-- here, -rw-rw-rw- on Windows).
	readme, err := w.Stat("/README.md")
	ok(err)
	ok(w.Chmod("/README.md", 0o600))
	ok(w.Chmod("/README.md", readme.Mode()))
	cs := capture(t, w, f.st)
	if cs.Files != 1 {
		t.Errorf("recreated file not hashed: %d files", cs.Files)
	}
	if !cs.Empty() || cs.ID != f.root {
		t.Errorf("restored tree changed:\n%s", cs.Text())
	}
}

// TestCaptureRenamedDirectory: a directory loaded lazily from its
// record, renamed, is patched as its removal and its addition, whole.
func TestCaptureRenamedDirectory(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	if err := w.Rename("/src", "/source"); err != nil {
		t.Fatal(err)
	}
	cs := capture(t, w, f.st)
	m := applyAll(t, f.m, cs)
	if m.GetEntry("source/lib/a.go") == nil || m.GetEntry("src/") != nil {
		t.Errorf("applied:\n%s", mustMarshal(t, m))
	}
	if cs.Files != 0 {
		t.Errorf("a rename hashed %d files", cs.Files)
	}
	// Renaming back is the original tree, apart from the root's time.
	w.Rename("/source", "/src")
	cs2 := capture(t, w, f.st)
	if cs2.ID != f.root {
		t.Errorf("renamed back: %s, want %s\n%s", cs2.ID, f.root, cs2.Text())
	}
	applyAll(t, f.m, cs, cs2)
}

// TestCaptureSymlinks: links made, changed and removed are patched.
// A new link's C4 ID is null.
func TestCaptureSymlinks(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	w.Symlink("src/lib", "/newlink")
	w.Remove("/link")
	w.Symlink("README.md", "/link")
	w.Rename("/dangling", "/src/dangling")
	cs := capture(t, w, f.st)
	m := applyAll(t, f.m, cs)
	if e := m.GetEntry("newlink"); e == nil || e.Target != "src/lib" || !e.C4ID.IsNil() || e.Mode&fs.ModeSymlink == 0 {
		t.Errorf("newlink = %+v", e)
	}
	if e := m.GetEntry("link"); e == nil || e.Target != "README.md" {
		t.Errorf("link = %+v", e)
	}
	if got := mustRead(t, cs.View(), "/newlink/a.go"); got != fixtureFiles["src/lib/a.go"] {
		t.Errorf("through captured link = %q", got)
	}
}

// TestCaptureSequences: a sequence folded in the base stays folded
// while whole, and unfolds into its members when one changes.
func TestCaptureSequences(t *testing.T) {
	dir, st := t.TempDir(), newTreeStore(t)
	files := map[string]string{"notes.txt": "n"}
	for i := 1; i <= 4; i++ {
		files[fmt.Sprintf("plates/shot.%04d.exr", i)] = strings.Repeat("p", i)
	}
	writeTree(t, dir, files)
	m, self, _ := snapshot(t, dir, st, true)
	if !strings.Contains(string(mustMarshal(t, m)), "[0001-0004]") {
		t.Fatalf("not folded:\n%s", mustMarshal(t, m))
	}
	v := mustOpen(t, st, self)
	w := newWS(t, v)
	orig3, _ := w.Stat("/plates/shot.0003.exr")
	orig2, err := w.Stat("/plates/shot.0002.exr") // the scanned host's mode
	if err != nil {
		t.Fatal(err)
	}
	// A change elsewhere: the sequence stays folded.
	wsWrite(t, w, "/notes.txt", "changed")
	w.Chmod("/plates/shot.0002.exr", orig2.Mode()) // the same mode
	cs1 := capture(t, w, st)
	full, err := cs1.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if text := string(mustMarshal(t, full)); !strings.Contains(text, "[0001-0004]") {
		t.Errorf("unfolded by an unrelated change:\n%s", text)
	}
	if len(full.RangeData) != 1 {
		t.Errorf("range data = %v", full.RangeData)
	}
	m1 := applyAll(t, m, cs1)
	// A member changes: the sequence is replaced by its members.
	wsWrite(t, w, "/plates/shot.0003.exr", "changed")
	cs2 := capture(t, w, st)
	m2 := applyAll(t, m1, cs2)
	if text := string(mustMarshal(t, m2)); strings.Contains(text, "[0001-0004]") || !strings.Contains(text, "shot.0004.exr") {
		t.Errorf("not unfolded:\n%s", text)
	}
	for _, e := range m2.Entries {
		if strings.HasPrefix(e.Name, "shot.") && e.Size < 0 {
			t.Errorf("%s has a null size", e.Name)
		}
	}
	// A member moved away also breaks the sequence.
	w.Rename("/plates/shot.0001.exr", "/shot.0001.exr")
	cs3 := capture(t, w, st)
	m3 := applyAll(t, m2, cs3)
	if got := mustRead(t, cs3.View(), "/shot.0001.exr"); got != "p" {
		t.Errorf("moved member = %q", got)
	}
	// Put back as they were, the members fold up again.
	w.Rename("/shot.0001.exr", "/plates/shot.0001.exr")
	wsWrite(t, w, "/plates/shot.0003.exr", "ppp")
	w.Chmod("/plates/shot.0003.exr", orig3.Mode())
	w.Chtimes("/plates/shot.0003.exr", orig3.ModTime(), orig3.ModTime())
	cs4 := capture(t, w, st)
	m4 := applyAll(t, m3, cs4)
	if text := string(mustMarshal(t, m4)); !strings.Contains(text, "[0001-0004]") {
		t.Errorf("not refolded:\n%s", text)
	}
	if len(cs4.Patch.RangeData) != 1 {
		t.Errorf("patch range data = %v", cs4.Patch.RangeData)
	}
	// The resumed tree reads the same.
	r, err := Resume(v, bytes.NewReader(append(append(append(cs1.Text(), cs2.Text()...), cs3.Text()...), cs4.Text()...)), st, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	if a, b := contents(t, r), contents(t, w); !reflect.DeepEqual(a, b) {
		t.Errorf("resumed differs:\n%v\n%v", sorted(a), sorted(b))
	}
}

// TestCaptureNullMetadata: over a base whose directories have null
// sizes and times, IDs still agree with c4m's.
func TestCaptureNullMetadata(t *testing.T) {
	m := synthetic(3, 2, 4)
	v := mustNew(t, m, nil)
	if id, err := v.ID(); err != nil || id != m.ComputeC4ID() {
		t.Fatalf("view ID %s %v, want %s", id, err, m.ComputeC4ID())
	}
	w := newWS(t, v)
	st := newTreeStore(t)
	wsWrite(t, w, "/dir001/sub01/new.go", "new")
	w.Remove("/dir002/sub00/file000.go")
	w.Chmod("/dir000/sub01/file003.go", 0o600)
	cs := capture(t, w, st)
	applyAll(t, m, cs)
	full, err := cs.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if full.ComputeC4ID() != cs.ID {
		t.Errorf("manifest ID %s, changeset %s", full.ComputeC4ID(), cs.ID)
	}
}

// TestResume reconstructs a workspace from its base and its chain of
// changesets, and carries on the chain.
func TestResume(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	var chain bytes.Buffer
	var css []*Changeset
	step := func(do func()) {
		do()
		cs := capture(t, w, f.st)
		cs.WriteTo(&chain)
		css = append(css, cs)
	}
	step(func() { wsWrite(t, w, "/a.txt", "a"); w.Mkdir("/newdir", 0o700) })
	step(func() { w.Rename("/src/lib", "/newdir/lib"); w.Remove("/README.md") })
	step(func() {})
	step(func() { w.Symlink("newdir/lib/a.go", "/alink"); w.Truncate("/docs/guide.txt", 3) })
	applyAll(t, f.m, css...)

	r, err := Resume(f.v, bytes.NewReader(chain.Bytes()), f.st, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	if a, b := contents(t, r), contents(t, w); !reflect.DeepEqual(a, b) {
		t.Errorf("resumed differs:\n%v\n%v", sorted(a), sorted(b))
	}
	head, _ := r.Head()
	if head != css[len(css)-1].ID {
		t.Errorf("resumed head %s", head)
	}
	// The chain continues from the resumed workspace.
	wsWrite(t, r, "/after.txt", "after")
	cs := capture(t, r, f.st)
	if cs.Base != head {
		t.Errorf("continued from %s", cs.Base)
	}
	applyAll(t, f.m, append(css, cs)...)

	// Stored, the chain opens as the tree it ends at: its first line
	// names the base, which is in the store.
	id := putBytes(t, f.st, chain.Bytes())
	if a, b := walk(t, mustOpen(t, f.st, id)), walk(t, r.Base()); !reflect.DeepEqual(a, b) {
		t.Errorf("opened chain differs:\n%v\n%v", sorted(a), sorted(b))
	}
	// An empty chain resumes the base.
	r2, err := Resume(f.v, bytes.NewReader(nil), nil, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := r2.Head(); id != f.root {
		t.Errorf("empty resume head %s", id)
	}
}

func TestApplyErrors(t *testing.T) {
	f := newWSFixture(t)
	wsWrite(t, f.w, "/a.txt", "a")
	cs := capture(t, f.w, f.st)
	text := string(cs.Text())

	other := mustNew(t, parse(t, "-rw-r--r-- 2025-01-01T00:00:00Z 1 x "+idOf("x").String()+"\n"), nil)
	_, err := other.Apply(strings.NewReader(text), f.st)
	wantErr(t, "wrong base", err, ErrBaseMismatch)

	bad := strings.Replace(text, cs.ID.String(), idOf("not it").String(), 1)
	_, err = f.v.Apply(strings.NewReader(bad), f.st)
	wantErr(t, "wrong closing ID", err, ErrIDMismatch)

	_, err = f.v.Apply(bytes.NewReader(mustMarshal(t, f.m)), f.st)
	wantErr(t, "a manifest, not a patch", err, ErrNotManifest)

	_, err = f.v.Apply(strings.NewReader("not c4m at all\n"), f.st)
	wantErr(t, "garbage", err, ErrNotManifest)

	_, err = f.v.Apply(strings.NewReader(f.root.String()+"\n-rw-r--r-- 2025-01-01T00:00:00Z 1 x -\n    -rw-r--r-- 2025-01-01T00:00:00Z 1 y -\n"), nil)
	wantErr(t, "bad depth", err, ErrInvalidManifest)

	_, err = f.v.Apply(errReader{}, nil)
	if err == nil {
		t.Error("read error ignored")
	}
	_, err = Resume(other, strings.NewReader(text), f.st, newScratch(t))
	wantErr(t, "resume on the wrong base", err, ErrBaseMismatch)

	// Without its closing line the patch still applies.
	open := strings.TrimSuffix(text, cs.ID.String()+"\n")
	nv, err := f.v.Apply(strings.NewReader(open), f.st)
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := nv.ID(); id != cs.ID {
		t.Errorf("unclosed patch gives %s", id)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

// failingPut is a store whose Put fails.
type failingPut struct {
	store.Store
	err error
}

func (s failingPut) Put(io.Reader) (c4.ID, error)           { return c4.ID{}, s.err }
func (s failingPut) Create(c4.ID) (io.WriteCloser, error)   { return nil, s.err }
func (s failingWrite) Create(c4.ID) (io.WriteCloser, error) { return failingWriter{s.err}, nil }
func (w failingWriter) Write([]byte) (int, error)           { return 0, w.err }
func (w failingWriter) Close() error                        { return nil }
func (w closeFailing) Write(b []byte) (int, error)          { return len(b), nil }
func (w closeFailing) Close() error                         { return w.err }
func (s failingClose) Create(c4.ID) (io.WriteCloser, error) { return closeFailing{s.err}, nil }
func (s existing) Create(id c4.ID) (io.WriteCloser, error) {
	return nil, &fs.PathError{Op: "create", Path: id.String(), Err: fs.ErrExist}
}

// failingWrite, failingClose and existing are stores whose Create
// writer fails to write, fails to close, or finds the object there.
type failingWrite struct {
	store.Store
	err error
}
type failingClose struct {
	store.Store
	err error
}
type existing struct{ store.Store }
type failingWriter struct{ err error }
type closeFailing struct{ err error }

// TestCaptureFailure: a capture that cannot store fails, loses nothing,
// and the next one records everything.
func TestCaptureFailure(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/a.txt", "a")
	wsWrite(t, w, "/src/b.txt", "b")
	full := errors.New("store full")
	if _, err := w.Capture(failingPut{f.st, full}); !errors.Is(err, full) {
		t.Errorf("capture = %v", err)
	}
	if _, err := w.Capture(nil); !errors.Is(err, ErrNoStore) {
		t.Errorf("nil store = %v", err)
	}
	if _, err := w.Capture(failingWrite{f.st, full}); !errors.Is(err, full) {
		t.Errorf("write failure = %v", err)
	}
	if _, err := w.Capture(failingClose{f.st, full}); !errors.Is(err, full) {
		t.Errorf("close failure = %v", err)
	}
	cs := capture(t, w, f.st)
	if cs.Files != 2 || cs.Base != f.root {
		t.Errorf("after failure: %d files, base %s", cs.Files, cs.Base)
	}
	applyAll(t, f.m, cs)
	// A scratch file that vanished cannot be captured.
	wsWrite(t, w, "/c.txt", "c")
	ents, _ := f.scratch.ReadDir("/")
	for _, e := range ents {
		f.scratch.Remove("/" + e.Name())
	}
	if _, err := w.Capture(f.st); err == nil {
		t.Error("captured a missing scratch file")
	}
}

// TestCaptureLargeFile: a file too large to read whole streams through
// the store's Put; one the store already has is not written again.
func TestCaptureLargeFile(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	big := strings.Repeat("0123456789abcdef", smallFile/16+1000)
	wsWrite(t, w, "/big.bin", big)
	wsWrite(t, w, "/dup.txt", "hello\n") // README.md's content: stored already
	cs := capture(t, w, f.st)
	if cs.Files != 2 || cs.Bytes != int64(len(big)+6) {
		t.Errorf("captured %d files, %d bytes", cs.Files, cs.Bytes)
	}
	if !f.st.Has(idOf(big)) {
		t.Error("large file not stored")
	}
	if got := mustRead(t, cs.View(), "/big.bin"); got != big {
		t.Error("large file reads back wrong")
	}
	// A store that reports the object exists when asked to create it.
	wsWrite(t, w, "/again.txt", "again")
	if _, err := w.Capture(existing{f.st}); err != nil {
		t.Errorf("existing = %v", err)
	}
	wsWrite(t, w, "/big.bin", big+"!")
	if _, err := w.Capture(failingPut{f.st, errors.New("full")}); err == nil {
		t.Error("large put failure ignored")
	}
}

// TestChangesetViewStores: the captured View reads captured content
// from the capture store and the rest from the base's.
func TestChangesetViewStores(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	wsWrite(t, w, "/new.txt", "new content")
	other := newTreeStore(t)
	cs := capture(t, w, other)
	if f.st.Has(idOf("new content")) || !other.Has(idOf("new content")) {
		t.Error("content went to the wrong store")
	}
	v := cs.View()
	if got := mustRead(t, v, "/new.txt"); got != "new content" {
		t.Errorf("captured = %q", got)
	}
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("base = %q", got)
	}
	if id, _ := v.ID(); id != cs.ID {
		t.Error("view ID")
	}
	// Resuming needs the capture store too.
	r, err := Resume(f.v, bytes.NewReader(cs.Text()), other, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := wsRead(t, r, "/new.txt"); got != "new content" {
		t.Errorf("resumed = %q", got)
	}
	// And the next capture into a third store reads through all three.
	wsWrite(t, w, "/third.txt", "third")
	cs2 := capture(t, w, newTreeStore(t))
	for _, name := range []string{"/third.txt", "/new.txt", "/README.md"} {
		if _, err := cs2.View().ReadFile(name); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestCaptureMetadataOnly: chmod and chtimes over a View file record
// the View's C4 ID without reading content.
func TestCaptureMetadataOnly(t *testing.T) {
	f := newFixture(t)
	cs := newCounting(f.st)
	v := mustOpen(t, cs, f.root)
	w := newWS(t, v)
	opens := cs.total()
	w.Chmod("/docs/guide.txt", 0o600)
	when := time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)
	w.Chtimes("/src/lib/a.go", when, when)
	got := capture(t, w, f.st)
	if got.Files != 0 {
		t.Errorf("hashed %d files", got.Files)
	}
	if n := cs.count(idOf(fixtureFiles["docs/guide.txt"])); n != 0 {
		t.Errorf("guide read %d times", n)
	}
	m := applyAll(t, f.m, got)
	if e := m.GetEntry("docs/guide.txt"); e.Mode != 0o600 || e.C4ID != idOf(fixtureFiles["docs/guide.txt"]) {
		t.Errorf("guide = %+v", e)
	}
	if e := m.GetEntry("src/lib/a.go"); !e.Timestamp.Equal(when) {
		t.Errorf("a.go time = %v", e.Timestamp)
	}
	t.Logf("store opens: %d before, %d after", opens, cs.total())
}
