package c4fs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

func oneFile(t testing.TB, name, body string) *c4m.Manifest {
	t.Helper()
	m := c4m.NewManifest()
	m.AddEntry(&c4m.Entry{Name: name, Mode: 0o644, Size: int64(len(body)), Timestamp: c4m.NullTimestamp(), C4ID: idOf(body)})
	return m
}

func TestContentSources(t *testing.T) {
	body := strings.Repeat("0123456789", 1000)
	id := idOf(body)

	tree := newTreeStore(t)
	putBytes(t, tree, []byte(body))
	ram := store.NewRAM()
	putBytes(t, ram, []byte(body))

	cases := map[string]struct {
		src      store.Source
		hydrates bool
	}{
		"treestore (local file)": {tree, false},
		"readerat (direct)":      {readerAtSource{id: []byte(body)}, false},
		"ram (hydrated)":         {ram, true},
		"stream (hydrated)":      {streamOnly{tree}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := newTestCache(t)
			v, err := New(oneFile(t, "f", body), tc.src, WithCache(cache))
			if err != nil {
				t.Fatal(err)
			}
			if cache.Has(id) {
				t.Fatal("hydrated before any read")
			}
			f, err := v.Open("/f")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			buf := make([]byte, 10)
			if n, err := f.ReadAt(buf, 9990); n != 10 || err != nil || string(buf) != "0123456789" {
				t.Errorf("ReadAt end = %d %v %q", n, err, buf)
			}
			if n, err := f.ReadAt(buf, 9995); n != 5 || err != io.EOF {
				t.Errorf("ReadAt past end = %d %v", n, err)
			}
			if got := cache.Has(id); got != tc.hydrates {
				t.Errorf("hydrated = %v, want %v", got, tc.hydrates)
			}
			if got := mustRead(t, v, "/f"); got != body {
				t.Error("ReadFile differs")
			}
		})
	}
}

func TestHydrationSharedAndVerified(t *testing.T) {
	body := "shared content"
	id := idOf(body)
	ram := store.NewRAM()
	putBytes(t, ram, []byte(body))
	cs := newCounting(ram)
	cache := newTestCache(t)

	// Many readers in two Views over one cache: one download. Each View
	// first learns that the store streams by reading something of its
	// own (a View that has not yet learned opens the store directly).
	two := func(name, warm string) *c4m.Manifest {
		putBytes(t, ram, []byte(warm))
		m := oneFile(t, name, body)
		m.AddEntry(&c4m.Entry{Name: "warm", Size: int64(len(warm)), Timestamp: c4m.NullTimestamp(), C4ID: idOf(warm)})
		return m
	}
	v1, _ := New(two("a", "warm one"), streamOnly{cs}, WithCache(cache))
	v2, _ := New(two("b", "warm two"), streamOnly{cs}, WithCache(cache))
	mustRead(t, v1, "/warm")
	mustRead(t, v2, "/warm")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, name := v1, "/a"
			if i%2 == 1 {
				v, name = v2, "/b"
			}
			if got, err := v.ReadFile(name); err != nil || string(got) != body {
				t.Errorf("read = %q %v", got, err)
			}
		}(i)
	}
	wg.Wait()
	if n := cs.count(id); n != 1 {
		t.Errorf("source opened %d times", n)
	}
	p, ok := cache.path(id)
	if !ok {
		t.Fatal("not cached")
	}
	if b, _ := os.ReadFile(p); string(b) != body {
		t.Errorf("cached = %q", b)
	}
	// The cache is a c4 TreeStore: the CLI's store code reads it.
	ts, err := store.NewTreeStore(cache.Dir())
	if err != nil || !ts.Has(id) {
		t.Errorf("cache is not a TreeStore: %v", err)
	}

	// Wrong bytes under an ID are refused and not kept.
	lie := lyingSource{[]byte("not the content")}
	lc := newTestCache(t)
	v, _ := New(oneFile(t, "f", body), lie, WithCache(lc))
	if _, err := v.ReadFile("/f"); !errors.Is(err, ErrIDMismatch) {
		t.Errorf("lying source = %v", err)
	}
	if lc.Has(idOf("not the content")) || lc.Has(id) {
		t.Error("mismatched content kept in cache")
	}
}

func TestCacheFillWaitsForFlight(t *testing.T) {
	cache := newTestCache(t)
	body := "slow"
	id := idOf(body)
	gate := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := cache.fill(id, opener(io.MultiReader(gatedReader{gate}, strings.NewReader(body))))
		first <- err
	}()
	// Wait until the first fill is registered, then start a second.
	for {
		cache.mu.Lock()
		_, busy := cache.flights[id]
		cache.mu.Unlock()
		if busy {
			break
		}
	}
	second := make(chan string, 1)
	go func() {
		p, err := cache.fill(id, func() (io.ReadCloser, error) {
			t.Error("second fill opened its source")
			return nil, errors.New("unused")
		})
		if err != nil {
			t.Error(err)
		}
		second <- p
	}()
	close(gate)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if p := <-second; filepath.Base(p) != id.String() {
		t.Errorf("second fill = %q", p)
	}
	// A fill of something already cached returns at once.
	if p, err := cache.fill(id, opener(strings.NewReader(""))); err != nil || filepath.Base(p) != id.String() {
		t.Errorf("cached fill = %q %v", p, err)
	}
	// A failing reader reports its error.
	if _, err := cache.fill(idOf("x"), opener(&failAfter{})); err == nil {
		t.Error("read error not reported")
	}
	boom := errors.New("cannot open")
	if _, err := cache.fill(idOf("y"), func() (io.ReadCloser, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Errorf("open error = %v", err)
	}
}

func opener(r io.Reader) func() (io.ReadCloser, error) {
	return func() (io.ReadCloser, error) { return io.NopCloser(r), nil }
}

type gatedReader struct{ gate chan struct{} }

func (g gatedReader) Read([]byte) (int, error) { <-g.gate; return 0, io.EOF }

type failAfter struct{}

func (*failAfter) Read([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestDefaultCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "xdg"))
	t.Setenv("LocalAppData", filepath.Join(home, "appdata"))
	defaultCache.mu.Lock()
	saved := defaultCache.c
	defaultCache.c = nil
	defaultCache.mu.Unlock()
	t.Cleanup(func() {
		defaultCache.mu.Lock()
		defaultCache.c = saved
		defaultCache.mu.Unlock()
	})

	dir := DefaultCacheDir()
	if !strings.HasPrefix(dir, home) {
		t.Fatalf("default cache dir %q not under %q", dir, home)
	}
	body := "default cached"
	ram := store.NewRAM()
	putBytes(t, ram, []byte(body))
	v, _ := New(oneFile(t, "f", body), ram)
	// A lookup alone creates nothing.
	if _, ok := v.localPath(idOf(body)); ok {
		t.Fatal("found before hydration")
	}
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("default cache created by a lookup")
	}
	if got := mustRead(t, v, "/f"); got != body {
		t.Errorf("read = %q", got)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("default cache not created: %v", err)
	}
	// Once it exists, a new process-wide cache picks it up for lookups.
	defaultCache.mu.Lock()
	defaultCache.c = nil
	defaultCache.mu.Unlock()
	if _, ok := v.localPath(idOf(body)); !ok {
		t.Error("existing default cache not used")
	}
	// An unusable default directory is reported on hydration.
	defaultCache.mu.Lock()
	defaultCache.c = nil
	defaultCache.mu.Unlock()
	os.RemoveAll(dir)
	os.WriteFile(dir, []byte("a file, not a dir"), 0o644)
	v2, _ := New(oneFile(t, "g", "other"), streamOnly{ram})
	putBytes(t, ram, []byte("other"))
	if _, err := v2.ReadFile("/g"); err == nil {
		t.Error("unusable default cache not reported")
	}
	if _, err := NewCache(filepath.Join(dir, "sub")); err == nil {
		t.Error("NewCache under a file succeeded")
	}
}

func TestMissingAndEmptyContent(t *testing.T) {
	st := newTreeStore(t)
	m := parse(t, strings.Join([]string{
		"-rw-r--r-- 2025-01-01T00:00:00Z 0 empty " + emptyID.String(),
		"-rw-r--r-- 2025-01-01T00:00:00Z 0 emptynull -",
		"-rw-r--r-- 2025-01-01T00:00:00Z 5 absent " + idOf("absnt").String(),
		"-rw-r--r-- 2025-01-01T00:00:00Z 5 noid -",
		"",
	}, "\n"))
	for name, src := range map[string]store.Source{"store": st, "none": nil} {
		t.Run(name, func(t *testing.T) {
			v, err := New(m, src, WithCache(newTestCache(t)))
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range []string{"/empty", "/emptynull"} {
				if b, err := v.ReadFile(n); err != nil || len(b) != 0 {
					t.Errorf("%s = %q %v", n, b, err)
				}
			}
			_, err = v.ReadFile("/absent")
			if !errors.Is(err, ErrContentMissing) || errors.Is(err, os.ErrNotExist) {
				t.Errorf("absent = %v", err)
			}
			if fi, err := v.Stat("/absent"); err != nil || fi.Size() != 5 {
				t.Errorf("absent still stats: %v %v", fi, err)
			}
			if _, err := v.ReadFile("/noid"); !errors.Is(err, ErrNoID) {
				t.Errorf("noid = %v", err)
			}
		})
	}
}

func TestSizeMismatch(t *testing.T) {
	st := newTreeStore(t)
	id := putBytes(t, st, []byte("four"))
	m := parse(t, "-rw-r--r-- 2025-01-01T00:00:00Z 9 f "+id.String()+"\n")
	for name, src := range map[string]store.Source{"local": st, "readerat": readerAtSource{id: []byte("four")}} {
		v, _ := New(m, src)
		if _, err := v.ReadFile("/f"); !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestTransientStoreError(t *testing.T) {
	boom := errors.New("timeout")
	v, _ := New(oneFile(t, "f", "x"), failingSource{boom})
	_, err := v.ReadFile("/f")
	if !errors.Is(err, boom) || errors.Is(err, ErrContentMissing) {
		t.Errorf("err = %v", err)
	}
	// A store that has Has but returns an odd error for a missing ID.
	v, _ = New(oneFile(t, "f", "x"), oddMissing{store.NewRAM()})
	if _, err := v.ReadFile("/f"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("odd missing = %v", err)
	}
}

// oddMissing reports absent content with an error that is not
// fs.ErrNotExist, as MultiStore does.
type oddMissing struct{ *store.RAM }

func (o oddMissing) Open(id c4.ID) (io.ReadCloser, error) {
	if !o.Has(id) {
		return nil, errors.New("not found in any store")
	}
	return o.RAM.Open(id)
}

func TestStaleLocalPath(t *testing.T) {
	st := newTreeStore(t)
	body := "moves"
	id := putBytes(t, st, []byte(body))
	m := parse(t, "- - - f "+id.String()+"\n")
	v, _ := New(m, st, WithCache(newTestCache(t)))
	if fi, _ := v.Stat("/f"); fi.Size() != int64(len(body)) {
		t.Fatalf("size = %d", fi.Size())
	}
	p, _ := st.ContentPath(id)
	moved := filepath.Join(filepath.Dir(p), "moved")
	if err := os.Rename(p, moved); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ReadFile("/f"); !errors.Is(err, ErrContentMissing) {
		t.Errorf("after removal = %v", err)
	}
	os.Rename(moved, p)
	if got := mustRead(t, v, "/f"); got != body {
		t.Errorf("after restore = %q", got)
	}
	// Size is remembered across the outage.
	if fi, _ := v.Stat("/f"); fi.Size() != int64(len(body)) {
		t.Errorf("size = %d", fi.Size())
	}
}

func TestNullSizeFromStream(t *testing.T) {
	ram := store.NewRAM()
	id := putBytes(t, ram, []byte("streamed"))
	m := parse(t, "- - - f "+id.String()+"\n")
	v, _ := New(m, streamOnly{ram}, WithCache(newTestCache(t)))
	if fi, _ := v.Stat("/f"); fi.Size() != 8 {
		t.Errorf("size = %d", fi.Size())
	}
	v, _ = New(m, readerAtSource{id: []byte("streamed")})
	if fi, _ := v.Stat("/f"); fi.Size() != 8 {
		t.Errorf("readerat size = %d", fi.Size())
	}
	_ = bytes.MinRead
}
