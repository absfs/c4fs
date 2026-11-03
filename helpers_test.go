package c4fs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/scan"
	"github.com/Avalanche-io/c4/store"
)

// writeTree creates files on disk. A value starting with "->" makes a
// symlink to the rest; a key ending in "/" makes a directory.
func writeTree(t testing.TB, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if strings.HasSuffix(name, "/") {
			if err := os.MkdirAll(p, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if target, ok := strings.CutPrefix(body, "->"); ok {
			if err := os.Symlink(target, p); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				t.Fatal(err)
			}
			continue
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Age everything, so the scanner need not wait out files modified in
	// the current second ("racy" files) before trusting their content.
	old := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil && info.Mode()&os.ModeSymlink == 0 {
			os.Chtimes(p, old, old)
		}
		return nil
	})
}

// snapshot scans dir and stores it the way c4 id -s does: every file's
// content, each sequence's members and ID list, a one-level record per
// directory, the manifest's own text and the root record. It returns the
// manifest, the ID the CLI prints (the manifest text's ID) and the root
// directory's ID.
func snapshot(t testing.TB, dir string, st store.Store, sequences bool) (*c4m.Manifest, c4.ID, c4.ID) {
	t.Helper()
	m, err := scan.Dir(dir, scan.WithSequenceDetection(sequences))
	if err != nil {
		t.Fatal(err)
	}
	// Round-trip through c4m text, as a stored manifest is: timestamps
	// keep whole seconds only.
	text, err := c4m.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if m, err = c4m.Unmarshal(text); err != nil {
		t.Fatal(err)
	}
	var stack []string
	for _, e := range m.Entries {
		stack = stack[:min(e.Depth, len(stack))]
		if e.IsDir() {
			stack = append(stack, e.Name)
			continue
		}
		if e.Target != "" || e.C4ID.IsNil() {
			continue
		}
		rel := strings.Join(stack, "") + e.Name
		if e.IsSequence {
			names, err := c4m.ExpandSequencePattern(e.Name)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]c4.ID, len(names))
			for i, n := range names {
				ids[i] = putFile(t, st, filepath.Join(dir, strings.Join(stack, ""), n))
			}
			putBytes(t, st, c4m.IDListBytes(ids))
			continue
		}
		putFile(t, st, filepath.Join(dir, filepath.FromSlash(rel)))
	}
	for _, e := range m.Entries {
		if e.IsDir() {
			storeRecord(t, st, m, e)
		}
	}
	self := putBytes(t, st, text)
	rec := m.Copy()
	rec.Canonicalize()
	root := putBytes(t, st, []byte(rec.Canonical()))
	return m, self, root
}

// storeRecord stores a directory's one-level record as the c4 CLI does.
func storeRecord(t testing.TB, st store.Store, m *c4m.Manifest, dir *c4m.Entry) {
	t.Helper()
	children := m.Children(dir)
	if len(children) == 0 {
		return
	}
	sub := c4m.NewManifest()
	for _, c := range children {
		cp := *c
		cp.Depth = 0
		sub.AddEntry(&cp)
	}
	sub.Canonicalize()
	sub.SortEntries()
	if id := putBytes(t, st, []byte(sub.Canonical())); id != dir.C4ID {
		t.Fatalf("record for %s stored as %s, entry says %s", dir.Name, id, dir.C4ID)
	}
}

func putFile(t testing.TB, st store.Store, p string) c4.ID {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return putBytes(t, st, b)
}

func putBytes(t testing.TB, st store.Store, b []byte) c4.ID {
	t.Helper()
	id, err := st.Put(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func idOf(s string) c4.ID { return c4.Identify(strings.NewReader(s)) }

// parse decodes c4m text, failing the test on error.
func parse(t testing.TB, text string) *c4m.Manifest {
	t.Helper()
	m, err := c4m.Unmarshal([]byte(text))
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, text)
	}
	return m
}

// counting wraps a store and counts Opens per ID.
type counting struct {
	store.Store
	mu    sync.Mutex
	opens map[c4.ID]int
}

func newCounting(s store.Store) *counting {
	return &counting{Store: s, opens: map[c4.ID]int{}}
}

func (c *counting) Open(id c4.ID) (io.ReadCloser, error) {
	c.mu.Lock()
	c.opens[id]++
	c.mu.Unlock()
	return c.Store.Open(id)
}

func (c *counting) count(id c4.ID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens[id]
}

func (c *counting) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.opens {
		n += v
	}
	return n
}

// streamOnly hides every optional capability of a store: Open returns a
// plain io.ReadCloser, as a network store's would.
type streamOnly struct{ s store.Store }

func (s streamOnly) Open(id c4.ID) (io.ReadCloser, error) {
	rc, err := s.s.Open(id)
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{rc, rc}, nil
}

// readerAtSource serves bytes.Readers: random access without local files.
type readerAtSource map[c4.ID][]byte

func (s readerAtSource) Open(id c4.ID) (io.ReadCloser, error) {
	b, ok := s[id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return struct {
		*bytes.Reader
		io.Closer
	}{bytes.NewReader(b), io.NopCloser(nil)}, nil
}

// lyingSource serves the wrong bytes for every ID.
type lyingSource struct{ body []byte }

func (s lyingSource) Open(c4.ID) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.body)), nil
}

// erroringSource opens fine and then fails mid-read.
type erroringSource struct{}

func (erroringSource) Open(c4.ID) (io.ReadCloser, error) {
	return io.NopCloser(iotest.ErrReader(errors.New("disk error"))), nil
}

// failingSource fails every Open with err.
type failingSource struct{ err error }

func (s failingSource) Open(c4.ID) (io.ReadCloser, error) { return nil, s.err }

func newTreeStore(t testing.TB) *store.TreeStore {
	t.Helper()
	st, err := store.NewTreeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st.SetSyncMode(store.SyncNone)
	return st
}

func newTestCache(t testing.TB) *Cache {
	t.Helper()
	c, err := NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustRead(t testing.TB, v *View, name string) string {
	t.Helper()
	b, err := v.ReadFile(name)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", name, err)
	}
	return string(b)
}

func names(t testing.TB, v *View, dir string) []string {
	t.Helper()
	ents, err := v.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	out := make([]string, len(ents))
	for i, e := range ents {
		out[i] = e.Name()
	}
	return out
}
