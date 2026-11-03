package c4fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// errPublishing stands in for the Windows sharing violation an open
// meets while the rename that publishes the file is still running. It
// is not one sharingViolation recognises, so openLocal does not retry it
// and an open inside the window fails the read, as it did on Windows.
var errPublishing = errors.New("emulated sharing violation: object still being published")

// TestCacheFillPublishRace has many goroutines open each of several
// uncached objects at once, through one View and its Cache, while the
// first of them fills it. "emulated" refuses any open of an object
// between its rename into place and the release of its fill, the window
// in which Windows refuses the open, so the race fails on every system;
// "real" runs the same load with no emulation, against the host's own
// rename.
func TestCacheFillPublishRace(t *testing.T) {
	for _, emulate := range []bool{true, false} {
		name := "real"
		if emulate {
			name = "emulated"
		}
		t.Run(name, func(t *testing.T) { publishRace(t, emulate) })
	}
}

func publishRace(t *testing.T, emulate bool) {
	const objects, readers = 8, 48
	ram := store.NewRAM()
	m := c4m.NewManifest()
	bodies := map[string][]byte{}
	for i := 0; i < objects; i++ {
		b := bytes.Repeat([]byte(fmt.Sprintf("object %d ", i)), 8<<10)
		name := fmt.Sprintf("f%d", i)
		bodies["/"+name] = b
		m.AddEntry(&c4m.Entry{Name: name, Mode: 0o644, Size: int64(len(b)), Timestamp: c4m.NullTimestamp(), C4ID: putBytes(t, ram, b)})
	}
	warm := []byte("warm")
	m.AddEntry(&c4m.Entry{Name: "warm", Mode: 0o644, Size: int64(len(warm)), Timestamp: c4m.NullTimestamp(), C4ID: putBytes(t, ram, warm)})
	cs := newCounting(ram)
	cache := newTestCache(t)
	v, err := New(m, streamOnly{cs}, WithCache(cache))
	if err != nil {
		t.Fatal(err)
	}
	// Learn that the store streams, so every first read below goes
	// through the cache rather than opening the store directly.
	mustRead(t, v, "/warm")

	var publishing sync.Map // C4 ID string -> struct{}, inside the window
	var refused atomic.Int64
	if emulate {
		cache.filled = func(id c4.ID) {
			publishing.Store(id.String(), struct{}{})
			time.Sleep(2 * time.Millisecond) // hold the window open
			publishing.Delete(id.String())
		}
		real := openFile
		openFile = func(p string) (*os.File, error) {
			if _, ok := publishing.Load(filepath.Base(p)); ok {
				refused.Add(1)
				return nil, &os.PathError{Op: "open", Path: p, Err: errPublishing}
			}
			return real(p)
		}
		t.Cleanup(func() { openFile = real })
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for name, want := range bodies {
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func(name string, want []byte, r int) {
				defer wg.Done()
				<-start
				if r%3 == 0 {
					// Stat sizes the file from the entry; Open and
					// ReadAt below exercise the open itself.
					if _, err := v.Stat(name); err != nil {
						t.Error(err)
					}
				}
				f, err := v.Open(name)
				if err != nil {
					t.Errorf("open %s: %v", name, err)
					return
				}
				defer f.Close()
				got := make([]byte, len(want))
				if n, err := f.ReadAt(got, 0); n != len(want) || (err != nil && err != io.EOF) {
					t.Errorf("read %s: %d bytes, %v", name, n, err)
					return
				}
				if !bytes.Equal(got, want) {
					t.Errorf("read %s: wrong bytes", name)
				}
			}(name, want, r)
		}
	}
	close(start)
	wg.Wait()

	if n := refused.Load(); n != 0 {
		t.Errorf("%d opens reached an object while its fill was still publishing it", n)
	}
	for name, want := range bodies {
		id := c4.Identify(bytes.NewReader(want))
		if n := cs.count(id); n != 1 {
			t.Errorf("%s: source opened %d times, want 1", name, n)
		}
		if !cache.Has(id) {
			t.Errorf("%s: not cached", name)
		}
	}
}

// TestCachePathHidesFlight checks that an object whose fill is in
// flight is not reported, even once its name exists, and is reported
// once the fill is released.
func TestCachePathHidesFlight(t *testing.T) {
	cache := newTestCache(t)
	body := "in flight"
	id := idOf(body)
	var during, has bool
	cache.filled = func(c4.ID) {
		_, during = cache.path(id)
		has = cache.Has(id)
		if _, ok := cache.ts.ContentPath(id); !ok {
			t.Error("object not yet in the store when its fill finished writing")
		}
	}
	p, err := cache.fill(id, opener(bytes.NewReader([]byte(body))))
	if err != nil {
		t.Fatal(err)
	}
	if during || has {
		t.Errorf("path during fill = %v, Has = %v; want both false", during, has)
	}
	if got, ok := cache.path(id); !ok || got != p {
		t.Errorf("path after fill = %q %v, want %q", got, ok, p)
	}
}

// TestOpenLocalRetriesSharingViolation checks openLocal's bounded
// backoff: a sharing violation is retried until the open succeeds or
// the retries run out, and any other error is returned at once.
func TestOpenLocalRetriesSharingViolation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(p, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	violation := errors.New("sharing violation")
	realOpen, realViolation := openFile, sharingViolation
	t.Cleanup(func() { openFile, sharingViolation = realOpen, realViolation })
	sharingViolation = func(err error) bool { return errors.Is(err, violation) }

	failing := func(fails int, err error) *int {
		calls := new(int)
		openFile = func(p string) (*os.File, error) {
			*calls++
			if *calls <= fails {
				return nil, &os.PathError{Op: "open", Path: p, Err: err}
			}
			return realOpen(p)
		}
		return calls
	}

	calls := failing(3, violation)
	c, err := openLocal(p)
	if err != nil {
		t.Fatalf("after 3 violations: %v", err)
	}
	c.close()
	if *calls != 4 || c.size != int64(len("content")) {
		t.Errorf("calls = %d, size = %d", *calls, c.size)
	}

	calls = failing(1<<30, violation)
	if _, err := openLocal(p); !errors.Is(err, violation) {
		t.Errorf("persistent violation = %v", err)
	}
	if *calls != openRetries+1 {
		t.Errorf("persistent violation tried %d times, want %d", *calls, openRetries+1)
	}

	other := errors.New("permission denied")
	calls = failing(1<<30, other)
	if _, err := openLocal(p); !errors.Is(err, other) {
		t.Errorf("other error = %v", err)
	}
	if *calls != 1 {
		t.Errorf("other error tried %d times, want 1", *calls)
	}

	if isSharingViolation(other) {
		t.Error("an ordinary error classified as a sharing violation")
	}
}

// TestCacheSplitMovesFilledObject checks that a read survives its
// object moving between the fill returning its path and the open: a
// fill of another ID pushes the cache's leaf directory over its split
// threshold, and the TreeStore moves every object into a subdirectory.
func TestCacheSplitMovesFilledObject(t *testing.T) {
	ram := store.NewRAM()
	m := c4m.NewManifest()
	body := []byte("the object that moves")
	id := putBytes(t, ram, body)
	m.AddEntry(&c4m.Entry{Name: "f0", Mode: 0o644, Size: int64(len(body)), Timestamp: c4m.NullTimestamp(), C4ID: id})
	cache := newTestCache(t)
	cache.ts.SetSplitThreshold(2)
	cs := newCounting(ram)
	v, err := New(m, streamOnly{cs}, WithCache(cache))
	if err != nil {
		t.Fatal(err)
	}
	cache.filled = func(got c4.ID) {
		if got != id {
			return
		}
		// What a concurrent reader of other objects does: fill
		// enough of them to split the leaf f0 was published in.
		for i := 0; i < 3; i++ {
			b := fmt.Sprintf("other %d", i)
			if _, err := cache.fill(idOf(b), opener(bytes.NewReader([]byte(b)))); err != nil {
				t.Error(err)
			}
		}
	}
	if got := mustRead(t, v, "/f0"); got != string(body) {
		t.Errorf("read %q", got)
	}
	if p, ok := cache.ts.ContentPath(id); !ok || filepath.Dir(p) == filepath.Clean(cache.Dir()) {
		t.Errorf("object at %q %v: the split did not move it", p, ok)
	}
	if n := cs.count(id); n != 1 {
		t.Errorf("source opened %d times, want 1", n)
	}
}

// TestCacheObjectRemovedAfterFill checks the other way a filled path
// goes stale: the object is removed (the cache cleaned) before it is
// opened. The read fills again from the store, and reports the content
// missing when the store has lost it too.
func TestCacheObjectRemovedAfterFill(t *testing.T) {
	for _, lost := range []bool{false, true} {
		ram := store.NewRAM()
		m := c4m.NewManifest()
		body := []byte("removed after its fill")
		id := putBytes(t, ram, body)
		m.AddEntry(&c4m.Entry{Name: "f", Mode: 0o644, Size: int64(len(body)), Timestamp: c4m.NullTimestamp(), C4ID: id})
		cache := newTestCache(t)
		cs := newCounting(ram)
		v, err := New(m, streamOnly{cs}, WithCache(cache))
		if err != nil {
			t.Fatal(err)
		}
		removed := false
		cache.filled = func(got c4.ID) {
			if removed {
				return
			}
			removed = true
			if err := cache.ts.Remove(got); err != nil {
				t.Error(err)
			}
			if lost {
				ram.Remove(got)
			}
		}
		b, err := v.ReadFile("/f")
		switch {
		case lost && !errors.Is(err, ErrContentMissing):
			t.Errorf("store lost it too: read = %q %v, want ErrContentMissing", b, err)
		case !lost && (err != nil || string(b) != string(body)):
			t.Errorf("refilled read = %q %v", b, err)
		case !lost && cs.count(id) != 2:
			t.Errorf("source opened %d times, want 2", cs.count(id))
		}
	}
}
