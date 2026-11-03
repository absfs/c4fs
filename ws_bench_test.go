package c4fs

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// resolvedSynthetic is synthetic with every directory's size, time and
// C4 ID filled in, as c4 id writes a real tree: a base whose identity
// needs no resolving.
func resolvedSynthetic(dirs, subdirs, files int) *c4m.Manifest {
	v, _ := New(synthetic(dirs, subdirs, files), nil)
	out := c4m.NewManifest()
	var build func(n *node, depth int) ([]*c4m.Entry, []*c4m.Entry)
	// build returns a directory's resolved children and its subtree in
	// manifest order.
	build = func(n *node, depth int) ([]*c4m.Entry, []*c4m.Entry) {
		kids, _ := v.kids(n)
		var lines, tree []*c4m.Entry
		for _, it := range listItems(kids) {
			e := *it.e
			e.Depth = depth
			var sub []*c4m.Entry
			if it.n.dir != nil {
				var childLines []*c4m.Entry
				childLines, sub = build(it.n, depth+1)
				cl := make([]canonical, len(childLines))
				for i, c := range childLines {
					cl[i] = canonical{c, c.Canonical()}
				}
				e.Size = -1
				resolveDirMeta(&e, cl)
				e.C4ID = listingID(cl)
			}
			lines = append(lines, &e)
			tree = append(tree, &e)
			tree = append(tree, sub...)
		}
		return lines, tree
	}
	_, out.Entries = build(v.root, 0)
	return out
}

// dirtyPaths names n existing files spread over the 100x10x100 tree.
func dirtyPaths(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("/dir%03d/sub%02d/file%03d.go", i%100, (i/100)%10, (i*7)%100)
	}
	return out
}

var benchBody = bytes.Repeat([]byte("x"), 1024)

func dirty(tb testing.TB, w *Workspace, paths []string) {
	for _, p := range paths {
		f, err := w.OpenFile(p, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			tb.Fatal(err)
		}
		f.Write(benchBody)
		f.Close()
	}
}

// BenchmarkCapture1kDirty100k captures a workspace with 1,000 files
// rewritten (1 KiB each) over a 100,000-file base: the hashing and
// storing of those files, the identity of the directories they are in,
// and the patch. Each capture is the first over a fresh View, so
// nothing is remembered from an earlier one; the base's content is not
// read.
func BenchmarkCapture1kDirty100k(b *testing.B) {
	m := resolvedSynthetic(100, 10, 100)
	paths := dirtyPaths(1000)
	for _, kind := range []string{"ram", "treestore"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			var entries int
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				var st store.Store = store.NewRAM()
				if kind == "treestore" {
					st = newTreeStore(b)
				}
				v, err := New(m, nil)
				if err != nil {
					b.Fatal(err)
				}
				w := newWS(b, v)
				dirty(b, w, paths)
				b.StartTimer()
				cs, err := w.Capture(st)
				if err != nil {
					b.Fatal(err)
				}
				if cs.Files != 1000 {
					b.Fatalf("hashed %d", cs.Files)
				}
				entries = len(cs.Patch.Entries)
			}
			b.ReportMetric(float64(entries), "patch-entries")
		})
	}
}

// BenchmarkCaptureIncremental captures 10 more rewritten files in a
// workspace already holding 1,000 captured ones: the cost follows what
// changed since the last capture.
func BenchmarkCaptureIncremental(b *testing.B) {
	v, _ := New(resolvedSynthetic(100, 10, 100), nil)
	st := store.NewRAM()
	w := newWS(b, v)
	dirty(b, w, dirtyPaths(1000))
	if _, err := w.Capture(st); err != nil {
		b.Fatal(err)
	}
	more := dirtyPaths(10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dirty(b, w, more)
		b.StartTimer()
		if _, err := w.Capture(st); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChangesetManifest builds the whole 100,000-entry manifest of
// a capture, which a caller needing only the patch never pays for.
func BenchmarkChangesetManifest(b *testing.B) {
	v, _ := New(resolvedSynthetic(100, 10, 100), nil)
	w := newWS(b, v)
	dirty(b, w, dirtyPaths(1000))
	cs, err := w.Capture(store.NewRAM())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cs.Manifest(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResume1k applies a 1,000-file changeset to a 100,000-file
// base, as Resume does.
func BenchmarkResume1k(b *testing.B) {
	v, _ := New(resolvedSynthetic(100, 10, 100), nil)
	st := store.NewRAM()
	w := newWS(b, v)
	dirty(b, w, dirtyPaths(1000))
	cs, err := w.Capture(st)
	if err != nil {
		b.Fatal(err)
	}
	text := cs.Text()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := v.Apply(bytes.NewReader(text), st); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWorkspaceStat stats a file of the View through a workspace
// with 1,000 changes elsewhere.
func BenchmarkWorkspaceStat(b *testing.B) {
	v, _ := New(resolvedSynthetic(100, 10, 100), nil)
	w := newWS(b, v)
	dirty(b, w, dirtyPaths(1000))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Stat("/dir057/sub03/file042.go"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWorkspaceNFSWrite is one NFS WRITE as absnfs performs it on
// a file already in scratch: open for writing, write 32 KiB at an
// offset, close.
func BenchmarkWorkspaceNFSWrite(b *testing.B) {
	v, _ := New(resolvedSynthetic(10, 10, 10), nil)
	w, err := NewWorkspace(v, folderScratch(b))
	if err != nil {
		b.Fatal(err)
	}
	dirty(b, w, []string{"/dir001/sub01/file001.go"})
	buf := bytes.Repeat([]byte("w"), 32<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := w.OpenFile("/dir001/sub01/file001.go", os.O_WRONLY, 0)
		if err != nil {
			b.Fatal(err)
		}
		f.WriteAt(buf, int64(i%64)*int64(len(buf)))
		f.Close()
	}
}

// BenchmarkWorkspaceCreate makes, writes and closes a small new file,
// as a build does with its outputs.
func BenchmarkWorkspaceCreate(b *testing.B) {
	v, _ := New(resolvedSynthetic(10, 10, 10), nil)
	w, err := NewWorkspace(v, folderScratch(b))
	if err != nil {
		b.Fatal(err)
	}
	w.Mkdir("/out", 0o755)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := w.Create("/out/obj" + strings.Repeat("x", i%8) + fmt.Sprint(i))
		if err != nil {
			b.Fatal(err)
		}
		f.Write(benchBody)
		f.Close()
	}
}

// TestResolvedSynthetic checks the benchmark base is what c4 would
// write: its ID needs nothing resolved, and agrees with c4m's.
func TestResolvedSynthetic(t *testing.T) {
	m := resolvedSynthetic(3, 2, 4)
	for _, e := range m.Entries {
		if e.IsDir() && (e.Size < 0 || e.C4ID.IsNil()) {
			t.Fatalf("unresolved %+v", e)
		}
	}
	v := mustNew(t, m, nil)
	if id, _ := v.ID(); id != m.ComputeC4ID() {
		t.Errorf("ID %s, c4m %s", id, m.ComputeC4ID())
	}
	w := newWS(t, v)
	dirty(t, w, []string{"/dir001/sub01/file002.go"})
	applyAll(t, m, capture(t, w, store.NewRAM()))
}
