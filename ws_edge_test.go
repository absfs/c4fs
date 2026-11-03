package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// TestNullDirectoryTimes: directories whose times and sizes are null
// resolve from their children for identity, as c4m's do, including
// when a capture changes them.
func TestNullDirectoryTimes(t *testing.T) {
	ts := "2025-01-01T00:00:00Z"
	text := "-rw-r--r-- " + ts + " 1 top.txt " + idOf("t").String() + "\n" +
		"drwxr-xr-x - - a/ -\n" +
		"  -rw-r--r-- " + ts + " 1 x " + idOf("x").String() + "\n" +
		"  drwxr-xr-x - - empty/ -\n" +
		"  drwxr-xr-x - - b/ -\n" +
		"    -rw-r--r-- - 1 y " + idOf("y").String() + "\n"
	m := parse(t, text)
	v := mustNew(t, m, nil)
	if id, err := v.ID(); err != nil || id != m.ComputeC4ID() {
		t.Fatalf("view ID %s %v, want %s", id, err, m.ComputeC4ID())
	}
	st := newTreeStore(t)
	w := newWS(t, v)
	wsWrite(t, w, "/a/new.txt", "n")
	cs := capture(t, w, st)
	applyAll(t, m, cs)
	full, err := cs.Manifest()
	if err != nil || full.ComputeC4ID() != cs.ID {
		t.Errorf("manifest %v", err)
	}
}

// TestWorkspaceMissingRecord: a tree opened by records, one of which is gone
// from the store, fails where it is needed, and only there.
func TestWorkspaceMissingRecord(t *testing.T) {
	gone := idOf("a record that was never stored")
	m := parse(t, "-rw-r--r-- 2025-01-01T00:00:00Z 1 x "+idOf("x").String()+"\n"+
		"drwxr-xr-x 2025-01-01T00:00:00Z - sub/ "+gone.String()+"\n")
	st := newTreeStore(t)
	v := mustNew(t, m, st)
	if _, err := v.ID(); !errors.Is(err, ErrContentMissing) {
		t.Errorf("ID = %v", err)
	}
	if _, err := v.Manifest(); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Manifest = %v", err)
	}
	w := newWS(t, v)
	if _, err := w.Head(); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Head = %v", err)
	}
	if _, err := w.Capture(st); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Capture = %v", err)
	}
	if _, err := v.Apply(strings.NewReader(""), nil); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Apply = %v", err)
	}
	if _, err := w.ReadDir("/sub"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("ReadDir = %v", err)
	}
	if _, err := w.Stat("/sub/y"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Stat below = %v", err)
	}
	if err := w.Remove("/sub"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Remove = %v", err)
	}
	h, err := w.Open("/sub")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Readdirnames(-1); !errors.Is(err, ErrContentMissing) {
		t.Errorf("Readdirnames = %v", err)
	}
	h.Close()
	// Removing it whole needs no record, and leaves a tree that does
	// not need it either.
	if err := w.RemoveAll("/sub"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Capture(st); !errors.Is(err, ErrContentMissing) {
		// The base's own ID still needs the record (its size is null).
		t.Logf("capture after removal = %v", err)
	}
	// A directory adopted for a change below it cannot list its View
	// children either.
	w2 := newWS(t, v)
	if err := w2.Chmod("/sub", 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.ReadDir("/sub"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("adopted ReadDir = %v", err)
	}
	// Whether a name is free there cannot be known either.
	if _, err := w2.Create("/sub/new"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("create in an unlisted dir = %v", err)
	}
	if err := w2.Mkdir("/sub/new", 0o755); !errors.Is(err, ErrContentMissing) {
		t.Errorf("mkdir in an unlisted dir = %v", err)
	}
	if err := w2.Symlink("x", "/sub/new"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("symlink in an unlisted dir = %v", err)
	}
	if err := w2.Rename("/x", "/sub/x"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("rename into an unlisted dir = %v", err)
	}
	if err := w2.RemoveAll("/sub/x"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("removeall in an unlisted dir = %v", err)
	}
	if err := w2.Remove("/sub"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("removed a directory that may hold names: %v", err)
	}
	if err := w2.Rename("/sub", "/x"); err == nil {
		t.Error("renamed a directory over a file")
	}
	if err := w2.Rename("/sub", "/sub2"); err != nil {
		t.Errorf("rename an unlisted dir = %v", err)
	}
}

// localless hides a store's local paths: its content comes only from
// Open, as a remote store's would.
type localless struct{ store.Store }

// TestSourcesWithoutLocalPaths: a captured View reads through stores
// with no local paths, falling through from one to the next, and tells
// absent content from a failing store.
func TestSourcesWithoutLocalPaths(t *testing.T) {
	f := newFixture(t)
	base := localless{f.st}
	v := mustOpen(t, base, f.root)
	w := newWS(t, v)
	wsWrite(t, w, "/new.txt", "captured")
	capStore := localless{newTreeStore(t)}
	cs := capture(t, w, capStore)
	cv := cs.View()
	if got := mustRead(t, cv, "/new.txt"); got != "captured" {
		t.Errorf("captured = %q", got)
	}
	if got := mustRead(t, cv, "/src/main.go"); got != "package main\n" {
		t.Errorf("base = %q", got)
	}
	src := cv.src.(sources)
	if !src.Has(idOf("captured")) || !src.Has(idOf("hello\n")) || src.Has(idOf("nothing")) {
		t.Error("Has")
	}
	if _, err := src.Open(idOf("nothing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("open absent = %v", err)
	}
	if _, ok := src.ContentPath(idOf("captured")); ok {
		t.Error("ContentPath through localless stores")
	}
	// A store that fails, rather than lacks, is reported as failing.
	boom := errors.New("network down")
	s2 := joinSources(failingSource{boom}, base).(sources)
	if _, err := s2.Open(idOf("nothing")); !errors.Is(err, boom) {
		t.Errorf("failing store = %v", err)
	}
	if !s2.Has(idOf("hello\n")) {
		t.Error("Has through a failing store")
	}
	// A source with neither Has nor local paths is asked to open.
	s3 := joinSources(readerAtSource{idOf("r"): []byte("r")}, f.st).(sources)
	if !s3.Has(idOf("r")) || s3.Has(idOf("nothing")) {
		t.Error("Has by opening")
	}
	if p, ok := s3.ContentPath(idOf("hello\n")); !ok || p == "" {
		t.Error("ContentPath from the second store")
	}
	if joinSources(nil, nil) != nil {
		t.Error("joining nothing")
	}
	if joinSources(f.st, f.st) != store.Source(f.st) {
		t.Error("a store joined with itself")
	}
	if got := joinSources(s3, f.st).(sources); len(got) != 2 {
		t.Errorf("flattened %d", len(got))
	}
	// Uncomparable stores are kept apart, not compared.
	if got := joinSources(readerAtSource{}, readerAtSource{}).(sources); len(got) != 2 {
		t.Errorf("uncomparable %d", len(got))
	}
}

// TestListItemsPartial: a listing with a sequence missing members lists
// the members it has one by one.
func TestListItemsPartial(t *testing.T) {
	seq := &c4m.Entry{Name: "f.[1-3].txt", IsSequence: true}
	bad := &c4m.Entry{Name: "g.[x].txt", IsSequence: true}
	kids := []*node{
		{name: "a", entry: &c4m.Entry{Name: "a"}},
		{name: "f.1.txt", entry: &c4m.Entry{Name: "f.1.txt"}, seq: seq},
		{name: "f.3.txt", entry: &c4m.Entry{Name: "f.3.txt"}, seq: seq},
		{name: "g", entry: &c4m.Entry{Name: "g"}, seq: bad},
	}
	var got []string
	for _, it := range listItems(kids) {
		got = append(got, it.e.Name)
	}
	if strings.Join(got, ",") != "a,f.1.txt,f.3.txt,g" {
		t.Errorf("listing = %v", got)
	}
	if seqLen(bad) != 0 {
		t.Error("seqLen of a bad pattern")
	}
	if compareEntries(&c4m.Entry{Name: "a"}, &c4m.Entry{Name: "a"}) != 0 {
		t.Error("compare equal")
	}
	if compareEntries(&c4m.Entry{Name: "01"}, &c4m.Entry{Name: "1"}) == 0 {
		t.Error("natural ties are broken by bytes")
	}
}

func TestErrorHelpers(t *testing.T) {
	plain := errors.New("plain")
	if scratchErr(plain) != plain || relabel(plain, "x") != plain {
		t.Error("plain errors pass through")
	}
	if err := relabel(pathErr("open", "/p", plain), "truncate"); err.(*fs.PathError).Op != "truncate" {
		t.Error("relabel")
	}
	if scratchErr(pathErr("open", "/scratch", plain)) != plain {
		t.Error("scratchErr")
	}
}

// openFailing is a scratch filesystem that creates files but cannot
// open them again.
type openFailing struct {
	absfs.FileSystem
	err error
}

func (f openFailing) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	if flag&os.O_CREATE == 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: f.err}
	}
	return f.FileSystem.OpenFile(name, flag, perm)
}

func TestScratchOpenFailure(t *testing.T) {
	f := newFixture(t)
	v := mustOpen(t, f.st, f.root)
	broken := errors.New("broken scratch")
	w, _ := NewWorkspace(v, openFailing{newScratch(t), broken})
	_, err := w.Create("/new")
	wantErr(t, "open after create", err, broken)
	_, err = w.OpenFile("/README.md", os.O_WRONLY, 0)
	wantErr(t, "open after copy-up", err, broken)
}

// TestCopyUpReadFailure: a View file whose content cannot be read fails
// its copy-up and leaves nothing behind.
func TestCopyUpReadFailure(t *testing.T) {
	m := parse(t, "-rw-r--r-- 2025-01-01T00:00:00Z 4 f "+idOf("data").String()+"\n")
	v, err := New(m, erroringSource{}, WithCache(newTestCache(t)))
	if err != nil {
		t.Fatal(err)
	}
	sc := newScratch(t)
	w, _ := NewWorkspace(v, sc)
	if _, err := w.OpenFile("/f", os.O_WRONLY, 0); err == nil {
		t.Fatal("copied up unreadable content")
	}
	if ents, _ := sc.ReadDir("/"); len(ents) != 0 {
		t.Errorf("left %d scratch files", len(ents))
	}
}

// TestHandleTruncateError: a handle's failing truncate records nothing.
func TestHandleTruncateError(t *testing.T) {
	w := newWS(t, conformanceView(t))
	h, err := w.OpenFile("/README.md", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.Truncate(-1); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("negative truncate = %v", err)
	}
	if _, err := h.Seek(0, io.SeekEnd); err != nil {
		t.Error(err)
	}
}

// TestCaptureEmptyBase: the first capture of an empty View.
func TestCaptureEmptyBase(t *testing.T) {
	v := mustNew(t, c4m.NewManifest(), nil)
	w := newWS(t, v)
	st := newTreeStore(t)
	w.MkdirAll("/a/b", 0o755)
	wsWrite(t, w, "/a/b/c", "c")
	cs := capture(t, w, st)
	if cs.Base != (c4.Identify(strings.NewReader(""))) {
		t.Errorf("empty base = %s", cs.Base)
	}
	applyAll(t, c4m.NewManifest(), cs)
	w.RemoveAll("/a")
	cs2 := capture(t, w, st)
	if cs2.ID != cs.Base {
		t.Errorf("emptied again = %s", cs2.ID)
	}
}

// TestDiffNullDirectoryID: two directories with the same entry, whose
// null C4 ID cannot tell their children apart. c4m.PatchDiff writes the
// directory's entry unchanged, which c4m.ApplyPatch takes as removing
// it and everything below; the capture's diff fills in the directory's
// C4 ID instead, so the patch changes it and applies the children.
func TestDiffNullDirectoryID(t *testing.T) {
	v := mustNew(t, c4m.NewManifest(), nil)
	dirEntry := &c4m.Entry{Name: "d/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp(), Size: -1}
	file := func(name string) *node {
		return &node{name: name, entry: &c4m.Entry{Name: name, Mode: 0o644, Timestamp: c4m.NullTimestamp(), Size: 1, C4ID: idOf(name)}}
	}
	same := file("same")
	old := newRoot([]*node{{name: "d", entry: dirEntry, dir: loadedDir([]*node{file("a"), same})}})
	nw := newRoot([]*node{{name: "d", entry: dirEntry, dir: loadedDir([]*node{file("b"), same})}})
	c := &capturer{w: &Workspace{v: v}}
	patch := c4m.NewManifest()
	if err := c.diff(old, nw, 0, patch); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range patch.Entries {
		got = append(got, e.Name)
	}
	if strings.Join(got, ",") != "d/,a,b" || patch.Entries[0].C4ID.IsNil() {
		t.Errorf("patch = %v, directory ID %s", got, patch.Entries[0].C4ID)
	}
	// Applied by c4m itself, it gives the new tree.
	asManifest := func(kids ...string) *c4m.Manifest {
		m := c4m.NewManifest()
		d := *dirEntry
		m.AddEntry(&d)
		for _, k := range kids {
			e := *file(k).entry
			e.Depth = 1
			m.AddEntry(&e)
		}
		return m
	}
	applied := c4m.ApplyPatch(asManifest("a", "same"), patch)
	var names []string
	for _, e := range applied.Entries {
		names = append(names, e.Name)
	}
	if strings.Join(names, ",") != "d/,b,same" {
		t.Errorf("applied = %v", names)
	}
	if id, _ := v.nodeID(nw.dir.kids[0]); applied.Entries[0].C4ID != id {
		t.Errorf("applied directory ID %s, want its listing's %s", applied.Entries[0].C4ID, id)
	}
	// Identical children: nothing.
	patch = c4m.NewManifest()
	nw2 := newRoot([]*node{{name: "d", entry: dirEntry, dir: loadedDir([]*node{file("a"), same})}})
	if err := c.diff(old, nw2, 0, patch); err != nil || len(patch.Entries) != 0 {
		t.Errorf("identical = %v %v", patch.Entries, err)
	}
}

// TestIDString: idString agrees with c4.ID.String everywhere, and
// canonLine with c4m's Canonical.
func TestIDString(t *testing.T) {
	var max c4.ID
	for i := range max {
		max[i] = 0xff
	}
	ids := []c4.ID{{}, max, idOf(""), idOf("x")}
	for i := 0; i < 2000; i++ {
		ids = append(ids, idOf(strings.Repeat("y", i)))
	}
	var one c4.ID
	one[63] = 1
	ids = append(ids, one)
	for _, id := range ids {
		if got, want := idString(id), id.String(); got != want {
			t.Fatalf("idString = %s, want %s", got, want)
		}
	}
	for _, e := range []*c4m.Entry{
		{Name: "f.txt", Mode: 0o644, Size: 3, Timestamp: c4m.NullTimestamp(), C4ID: idOf("abc")},
		{Name: "d/", Mode: fs.ModeDir | 0o755, Size: -1, Timestamp: c4m.NullTimestamp()},
		{Name: "l", Mode: fs.ModeSymlink | 0o777, Target: "a b", C4ID: idOf("t")},
		{Name: "name with space", C4ID: max},
	} {
		if got, want := canonLine(e), e.Canonical(); got != want {
			t.Errorf("canonLine = %q, want %q", got, want)
		}
	}
}

// TestNaturalLess: naturalLess agrees with c4m.NaturalLess.
func TestNaturalLess(t *testing.T) {
	names := []string{"", "a", "b", "a1", "a01", "a001", "a2", "a10", "a1b", "a1b2", "1", "01", "10", "1a",
		"file.1.exr", "file.01.exr", "file.10.exr", "file.2.exr", "x" + strings.Repeat("9", 30), "x" + strings.Repeat("9", 31),
		"日本1", "日本10", "日本", "é1", "e1", "\xff1", "\xfe1", "a\xff", "0", "00", "000a", "a0", "a00", "Z", "z"}
	rng := uint64(1)
	for i := 0; i < 3000; i++ {
		var b strings.Builder
		for k := 0; k < 1+int(rng%7); k++ {
			rng = rng*6364136223846793005 + 1442695040888963407
			frags := []string{"0", "1", "2", "9", "a", "b", ".", "Z", "_", "é", "日", "00"}
			b.WriteString(frags[rng>>59%uint64(len(frags))])
		}
		names = append(names, b.String())
	}
	for _, a := range names {
		for _, b := range names[:200] {
			if got, want := naturalLess(a, b), c4m.NaturalLess(a, b); got != want {
				t.Fatalf("naturalLess(%q, %q) = %v, c4m says %v", a, b, got, want)
			}
			if got, want := naturalLess(b, a), c4m.NaturalLess(b, a); got != want {
				t.Fatalf("naturalLess(%q, %q) = %v, c4m says %v", b, a, got, want)
			}
		}
	}
}

// TestCopyUpTruncateIsChange: a copy made for O_TRUNC or Truncate is a
// change the moment it exists, even if its opener goes away: the next
// capture must not record the View's ID for the emptied file.
func TestCopyUpTruncateIsChange(t *testing.T) {
	f := newWSFixture(t)
	w := f.w
	// Copy up for O_TRUNC, then lose the name before the opener looks
	// again: the copy is installed with nothing to mark it.
	n := w.materialize("/README.md")
	w.mu.Lock()
	if err := w.copyUp(n, true, -1); err != nil {
		t.Fatal(err)
	}
	w.mu.Unlock()
	if !n.dirty {
		t.Error("an emptied copy is not dirty")
	}
	n2 := w.materialize("/src/main.go")
	w.mu.Lock()
	w.copyUp(n2, false, 4)
	w.mu.Unlock()
	cs := capture(t, w, f.st)
	m := applyAll(t, f.m, cs)
	if e := m.GetEntry("README.md"); e.C4ID != idOf("") {
		t.Errorf("README.md recorded as %s", e.C4ID)
	}
	if e := m.GetEntry("src/main.go"); e.C4ID != idOf("pack") {
		t.Errorf("main.go recorded as %s", e.C4ID)
	}
}
