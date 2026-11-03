package c4fs

import (
	"bytes"
	"errors"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

func TestOpenRecordIsLazy(t *testing.T) {
	f := newFixture(t)
	cs := newCounting(f.st)
	v, err := Open(cs, f.root, WithCache(f.cache))
	if err != nil {
		t.Fatal(err)
	}
	if cs.total() != 1 || cs.count(f.root) != 1 {
		t.Fatalf("Open read %d objects, want only the root record", cs.total())
	}
	src := f.m.GetEntry("src/")
	lib := f.m.GetEntry("src/lib/")
	docs := f.m.GetEntry("docs/")
	if _, err := v.Stat("/src/lib/a.go"); err != nil {
		t.Fatal(err)
	}
	if cs.count(src.C4ID) != 1 || cs.count(lib.C4ID) != 1 || cs.count(docs.C4ID) != 0 {
		t.Errorf("stat loaded src=%d lib=%d docs=%d", cs.count(src.C4ID), cs.count(lib.C4ID), cs.count(docs.C4ID))
	}
	// Nothing about a file's bytes is read by stat, open or close.
	a := idOf(fixtureFiles["src/lib/a.go"])
	file, err := v.Open("/src/lib/a.go")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if cs.count(a) != 0 {
		t.Errorf("open/stat/close read content %d times", cs.count(a))
	}
	// Records load once, however often they are used.
	for i := 0; i < 3; i++ {
		names(t, v, "/src/lib")
	}
	if cs.count(lib.C4ID) != 1 {
		t.Errorf("lib record read %d times", cs.count(lib.C4ID))
	}
	// The empty directory is never fetched: its ID says it is empty.
	names(t, v, "/emptydir")
	if cs.count(emptyID) != 0 {
		t.Errorf("empty dir fetched %d times", cs.count(emptyID))
	}
}

func TestMissingRecord(t *testing.T) {
	f := newFixture(t)
	lib := f.m.GetEntry("src/lib/")
	if err := f.st.Remove(lib.C4ID); err != nil {
		t.Fatal(err)
	}
	v, err := Open(f.st, f.root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.ReadDir("/src/lib"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("ReadDir = %v", err)
	}
	if _, err := v.Stat("/src/lib/a.go"); !errors.Is(err, ErrContentMissing) || errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat = %v", err)
	}
	if _, err := v.Stat("/src/lib"); err != nil {
		t.Errorf("the directory itself still stats: %v", err)
	}
	// A failed load is retried: put the record back and it heals.
	storeRecord(t, f.st, f.m, lib)
	if got := names(t, v, "/src/lib"); len(got) != 3 {
		t.Errorf("after repair = %v", got)
	}
	// Without a store a record cannot load at all.
	nv, err := New(parse(t, "drwxr-xr-x - - d/ "+lib.C4ID.String()+"\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nv.ReadDir("/d"); !errors.Is(err, ErrNoStore) {
		t.Errorf("no store = %v", err)
	}
}

func TestOpenErrors(t *testing.T) {
	st := newTreeStore(t)
	if _, err := Open(nil, emptyID); !errors.Is(err, ErrNoStore) {
		t.Errorf("nil store = %v", err)
	}
	if _, err := Open(st, idOf("missing")); !errors.Is(err, ErrContentMissing) {
		t.Errorf("missing = %v", err)
	}
	notC4m := putBytes(t, st, []byte("@directive\n"))
	if _, err := Open(st, notC4m); !errors.Is(err, ErrNotManifest) {
		t.Errorf("not c4m = %v", err)
	}
	if _, err := Open(lyingSource{[]byte("x")}, idOf("y")); !errors.Is(err, ErrIDMismatch) {
		t.Errorf("lying = %v", err)
	}
	boom := errors.New("network down")
	if _, err := Open(failingSource{boom}, idOf("y")); !errors.Is(err, boom) || errors.Is(err, ErrContentMissing) {
		t.Errorf("failing = %v", err)
	}
	ram := store.NewRAM()
	if _, err := Open(ram, idOf("absent")); !errors.Is(err, ErrContentMissing) {
		t.Errorf("ram missing = %v", err)
	}
	if _, err := Open(erroringSource{}, idOf("y")); err == nil {
		t.Error("read error not reported")
	}
}

func TestOpenConfigured(t *testing.T) {
	f := newFixture(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("C4_STORE", "")
	if _, err := OpenConfigured(f.root); !errors.Is(err, ErrNoStore) {
		t.Errorf("unconfigured = %v", err)
	}
	t.Setenv("C4_STORE", f.st.Root())
	v, err := OpenConfigured(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("read = %q", got)
	}
	t.Setenv("C4_STORE", "s3://bucket/prefix")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	if _, err := OpenConfigured(f.root); err == nil {
		t.Error("bad store config not reported")
	}
}

// walk lists every path in a View with its entry, for comparisons.
func walk(t testing.TB, v *View) map[string]string {
	t.Helper()
	out := map[string]string{}
	var visit func(dir string)
	visit = func(dir string) {
		ents, err := v.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		for _, e := range ents {
			p := strings.TrimSuffix(dir, "/") + "/" + e.Name()
			fi, _ := v.Lstat(p)
			ent := fi.Sys().(*c4m.Entry)
			out[p] = ent.Mode.String() + " " + ent.C4ID.String() + " " + ent.Target
			if e.IsDir() {
				visit(p)
			}
		}
	}
	visit("/")
	return out
}

// patchFixture builds two trees, stores both like the CLI, and returns
// them with their patch.
func patchFixture(t *testing.T) (st *store.TreeStore, oldM, newM *c4m.Manifest, oldSelf, oldRoot, newRoot c4.ID) {
	st = newTreeStore(t)
	oldDir, newDir := t.TempDir(), t.TempDir()
	writeTree(t, oldDir, map[string]string{
		"keep.txt":      "same",
		"change.txt":    "before",
		"gone.txt":      "bye",
		"a/b/deep.txt":  "deep before",
		"a/b/other.txt": "untouched",
		"a/x.txt":       "x",
		"wasfile":       "file",
		"wasdir/f":      "in dir",
		"unvisited/u":   "never loaded",
	})
	writeTree(t, newDir, map[string]string{
		"keep.txt":      "same",
		"change.txt":    "after",
		"new.txt":       "hello",
		"a/b/deep.txt":  "deep after",
		"a/b/other.txt": "untouched",
		"a/x.txt":       "x",
		"a/c/added.txt": "added dir",
		"wasfile/f":     "now a dir",
		"wasdir":        "now a file",
		"unvisited/u":   "never loaded",
	})
	oldM, oldSelf, oldRoot = snapshot(t, oldDir, st, false)
	newM, _, newRoot = snapshot(t, newDir, st, false)
	return
}

func TestExternalBasePatch(t *testing.T) {
	st, oldM, newM, oldSelf, oldRoot, newRoot := patchFixture(t)
	want := walk(t, mustNew(t, newM, st))
	pr := c4m.PatchDiff(oldM, newM)
	if pr.IsEmpty() {
		t.Fatal("empty patch")
	}
	for name, base := range map[string]c4.ID{"self": oldSelf, "record": oldRoot} {
		t.Run(name, func(t *testing.T) {
			text := append([]byte(base.String()+"\n"), mustMarshal(t, pr.Patch)...)
			pid := putBytes(t, st, text)
			cs := newCounting(st)
			v, err := Open(cs, pid)
			if err != nil {
				t.Fatal(err)
			}
			if name == "record" {
				// Only directories the patch passes through were read.
				if n := cs.count(oldM.GetEntry("unvisited/").C4ID); n != 0 {
					t.Errorf("unvisited record read %d times while patching", n)
				}
			}
			if got := walk(t, v); !reflect.DeepEqual(got, want) {
				t.Errorf("patched tree differs:\n got %v\nwant %v", sorted(got), sorted(want))
			}
			if got := mustRead(t, v, "/a/b/deep.txt"); got != "deep after" {
				t.Errorf("deep = %q", got)
			}
			if _, err := v.Stat("/gone.txt"); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("gone = %v", err)
			}
		})
	}
	_ = newRoot
}

func TestPatchMatchesC4m(t *testing.T) {
	_, oldM, newM, _, _, _ := patchFixture(t)
	pr := c4m.PatchDiff(oldM, newM)
	applied := c4m.ApplyPatch(oldM, pr.Patch)
	want := walk(t, mustNew(t, applied, nil))
	v, err := New(&c4m.Manifest{Base: oldM.ComputeC4ID(), Entries: pr.Patch.Entries}, nil)
	if err == nil {
		t.Fatalf("patch over an unloadable base succeeded: %v", walk(t, v))
	}
	if !errors.Is(err, ErrNoStore) {
		t.Errorf("err = %v", err)
	}
	st := newTreeStore(t)
	base := putBytes(t, st, mustMarshal(t, oldM))
	v, err = New(&c4m.Manifest{Base: base, Entries: pr.Patch.Entries}, st)
	if err != nil {
		t.Fatal(err)
	}
	if got := walk(t, v); !reflect.DeepEqual(got, want) {
		t.Errorf("differs from c4m.ApplyPatch:\n got %v\nwant %v", sorted(got), sorted(want))
	}
}

func TestInlinePatchChain(t *testing.T) {
	_, oldM, newM, _, _, _ := patchFixture(t)
	pr := c4m.PatchDiff(oldM, newM)
	var buf bytes.Buffer
	buf.Write(mustMarshal(t, oldM))
	buf.WriteString(oldM.ComputeC4ID().String() + "\n")
	buf.Write(mustMarshal(t, pr.Patch))
	st := newTreeStore(t)
	id := putBytes(t, st, buf.Bytes())
	v, err := Open(st, id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := walk(t, v), walk(t, mustNew(t, newM, nil)); !reflect.DeepEqual(got, want) {
		t.Errorf("chain differs:\n got %v\nwant %v", sorted(got), sorted(want))
	}
}

func TestPatchEdgeCases(t *testing.T) {
	st := newTreeStore(t)
	base := parse(t, strings.Join([]string{
		"-rw-r--r-- 2025-01-01T00:00:00Z 1 a " + idOf("a").String(),
		"-rw-r--r-- 2025-01-01T00:00:00Z 1 b " + idOf("b").String(),
		"",
	}, "\n"))
	bid := putBytes(t, st, mustMarshal(t, base))
	// The patch names "a" twice: the last occurrence counts, as in c4m.
	// It repeats "b" exactly, which removes it.
	patch := &c4m.Manifest{Base: bid, Entries: []*c4m.Entry{
		{Name: "a", Mode: 0o644, Size: 2, Timestamp: c4m.NullTimestamp(), C4ID: idOf("aa")},
		{Name: "a", Mode: 0o600, Size: 3, Timestamp: c4m.NullTimestamp(), C4ID: idOf("aaa")},
		base.Entries[1],
	}}
	v, err := New(patch, st)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(t, v, "/"); !reflect.DeepEqual(got, []string{"a"}) {
		t.Errorf("names = %v", got)
	}
	if fi, _ := v.Stat("/a"); fi.Size() != 3 || fi.Mode() != 0o600 {
		t.Errorf("a = %d %v", fi.Size(), fi.Mode())
	}
	// A bad patch is reported, as is a base chain too long to follow.
	bad := &c4m.Manifest{Base: bid, Entries: []*c4m.Entry{{Name: "x", Depth: 3}}}
	if _, err := New(bad, st); !errors.Is(err, ErrInvalidManifest) {
		t.Errorf("bad patch = %v", err)
	}
	if _, err := v.fromChain(&chain{base: bid}, maxBaseHops); !errors.Is(err, ErrInvalidManifest) {
		t.Errorf("hop limit = %v", err)
	}
	// A patch through a directory whose base record is missing fails.
	dirBase := parse(t, "drwxr-xr-x - - d/ "+idOf("no such record").String()+"\n")
	did := putBytes(t, st, mustMarshal(t, dirBase))
	dp := &c4m.Manifest{Base: did, Entries: []*c4m.Entry{
		{Name: "d/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp(), C4ID: idOf("new")},
		{Name: "f", Depth: 1, Timestamp: c4m.NullTimestamp()},
	}}
	if _, err := New(dp, st); !errors.Is(err, ErrContentMissing) {
		t.Errorf("patch through missing record = %v", err)
	}
}

func mustNew(t testing.TB, m *c4m.Manifest, src store.Source) *View {
	t.Helper()
	v, err := New(m, src)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func mustMarshal(t testing.TB, m *c4m.Manifest) []byte {
	t.Helper()
	b, err := c4m.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sorted(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
