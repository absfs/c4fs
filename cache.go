package c4fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/store"
)

// Cache is a local, content-addressed cache of hydrated content. A View
// hydrates into it when its store cannot serve random access itself (a
// store whose readers are not io.ReaderAt and io.Seeker, such as S3).
//
// The cache directory is an ordinary c4 TreeStore, the same layout the c4
// CLI writes, so it can also be listed as a c4 store. Content is written
// to a temporary file, verified against its C4 ID, and renamed into place,
// so a reader never sees a partial object and a damaged download is
// refused. Concurrent fills of one ID within a Cache share one download,
// and no reader opens the object until its fill has returned (on Windows
// the publishing rename holds the file open until then, refusing opens).
// Share one Cache between the Views of a process; separate processes may
// share a directory too, since every object is published atomically.
//
// A Cache never removes anything: it grows with every object hydrated
// into it. Its owner decides its lifetime; delete the directory (when
// no View is reading from it) to reclaim the space.
type Cache struct {
	ts      *store.TreeStore
	mu      sync.Mutex
	flights map[c4.ID]*flight
	// filled, when set, runs after a fill has written its object and
	// before the flight is released: a test seam for the publish window.
	filled func(id c4.ID)
}

type flight struct {
	done chan struct{}
	path string
	err  error
}

// NewCache opens (creating if needed) a cache rooted at dir.
func NewCache(dir string) (*Cache, error) {
	ts, err := store.NewTreeStore(dir)
	if err != nil {
		return nil, err
	}
	// A cache can always be refilled from its source, so it skips the
	// per-object fsync a store of record needs.
	ts.SetSyncMode(store.SyncNone)
	return &Cache{ts: ts, flights: make(map[c4.ID]*flight)}, nil
}

// DefaultCacheDir is where a View hydrates when it was given no Cache:
// "c4fs" under os.UserCacheDir, or under os.TempDir if there is no user
// cache directory.
func DefaultCacheDir() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "c4fs")
}

var defaultCache struct {
	mu sync.Mutex
	c  *Cache
}

// sharedDefault returns the process-wide default cache. When create is
// false it returns nil rather than make a directory nobody needs yet.
func sharedDefault(create bool) (*Cache, error) {
	defaultCache.mu.Lock()
	defer defaultCache.mu.Unlock()
	if defaultCache.c != nil {
		return defaultCache.c, nil
	}
	dir := DefaultCacheDir()
	if !create {
		if _, err := os.Stat(dir); err != nil {
			return nil, nil
		}
	}
	c, err := NewCache(dir)
	if err != nil {
		return nil, err
	}
	defaultCache.c = c
	return c, nil
}

// Dir returns the cache's root directory.
func (c *Cache) Dir() string { return c.ts.Root() }

// Has reports whether the cache holds content for id. Content whose
// fill is still in flight is not held yet.
func (c *Cache) Has(id c4.ID) bool {
	_, ok := c.path(id)
	return ok
}

// path returns the local file holding id's content. It reports nothing
// while a fill of id is in flight, even once the object's name exists:
// on Windows the rename that publishes it holds the file open, and an
// open in that window fails with a sharing violation. A caller that
// finds nothing falls back to fill, which waits for the flight.
func (c *Cache) path(id c4.ID) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.flights[id]; ok {
		return "", false
	}
	return c.ts.ContentPath(id)
}

// fill hydrates id from the reader open returns, and returns the cached
// file's path. If id is cached already, or another fill of it is in
// flight, open is not called.
func (c *Cache) fill(id c4.ID, open func() (io.ReadCloser, error)) (string, error) {
	c.mu.Lock()
	// The flight comes first: its object may already be renamed into
	// place, but it is not released until the fill has returned.
	if f, ok := c.flights[id]; ok {
		c.mu.Unlock()
		<-f.done
		return f.path, f.err
	}
	if p, ok := c.ts.ContentPath(id); ok {
		c.mu.Unlock()
		return p, nil
	}
	f := &flight{done: make(chan struct{})}
	c.flights[id] = f
	c.mu.Unlock()

	if rc, err := open(); err != nil {
		f.err = err
	} else {
		f.path, f.err = c.put(id, rc)
	}
	if c.filled != nil {
		c.filled(id)
	}

	c.mu.Lock()
	delete(c.flights, id)
	c.mu.Unlock()
	close(f.done)
	return f.path, f.err
}

func (c *Cache) put(id c4.ID, rc io.ReadCloser) (string, error) {
	defer rc.Close()
	got, err := c.ts.Put(rc)
	if err != nil {
		return "", fmt.Errorf("hydrate %s: %w", id, err)
	}
	if got != id {
		c.ts.Remove(got)
		return "", fmt.Errorf("%w: wanted %s, read %s", ErrIDMismatch, id, got)
	}
	p, ok := c.ts.ContentPath(id)
	if !ok {
		return "", fmt.Errorf("hydrate %s: %w", id, ErrContentMissing)
	}
	return p, nil
}
