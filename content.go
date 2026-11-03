package c4fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

// contentPather is implemented by stores that keep objects as local
// files: the c4 TreeStore, Folder and MultiStore.
type contentPather interface {
	ContentPath(id c4.ID) (string, bool)
}

// haser is implemented by every c4 store.Store.
type haser interface {
	Has(id c4.ID) bool
}

// content is an open handle on a file's bytes.
type content struct {
	ra   io.ReaderAt
	c    io.Closer // nil when there is nothing to close
	size int64
}

func (c *content) close() error {
	if c.c == nil {
		return nil
	}
	return c.c.Close()
}

var emptyContent = &content{ra: bytes.NewReader(nil)}

// isSpecial reports a named pipe, socket or device: c4m records them,
// but they have no bytes to serve.
func isSpecial(e *c4m.Entry) bool {
	return e.Mode&(fs.ModeNamedPipe|fs.ModeSocket|fs.ModeDevice|fs.ModeCharDevice) != 0
}

// empty reports content that is known to be zero bytes without asking
// the store.
func empty(e *c4m.Entry) bool {
	return e.C4ID == emptyID || (e.C4ID.IsNil() && e.Size == 0) || isSpecial(e)
}

// openContent opens a file's content for random access. In order it
// tries a local path remembered from an earlier open, the store's own
// local file (ContentPath), the cache, and then the store's reader:
// used directly when it is an io.ReaderAt and io.Seeker, hydrated into
// the cache otherwise.
func (v *View) openContent(n *node) (*content, error) {
	e := n.entry
	if empty(e) {
		return emptyContent, nil
	}
	id := e.C4ID
	if id.IsNil() {
		return nil, ErrNoID
	}
	if m := n.memo.Load(); m != nil && m.path != "" {
		if c, err := openLocal(m.path); err == nil {
			return v.checkSize(n, c)
		}
		// The object moved (a store split its directory, or the cache
		// was cleaned): forget the path and look again.
		n.memo.Store(&memo{size: m.size})
	}
	if p, ok := v.localPath(id); ok {
		if c, err := openLocal(p); err == nil {
			v.remember(n, p, c.size)
			return v.checkSize(n, c)
		}
	}
	if v.src == nil {
		return nil, fmt.Errorf("%w (%s)", ErrContentMissing, id)
	}
	var rc io.ReadCloser
	if !v.streams.Load() {
		var err error
		if rc, err = v.src.Open(id); err != nil {
			return nil, v.openErr(id, err)
		}
		if ra, ok := rc.(io.ReaderAt); ok {
			if sk, ok := rc.(io.Seeker); ok {
				if size, err := sk.Seek(0, io.SeekEnd); err == nil {
					return v.checkSize(n, &content{ra: ra, c: rc, size: size})
				}
			}
		}
		// The store streams. Remember that, so later first reads go
		// straight to the cache, where concurrent fills of one ID share
		// a single Open.
		v.streams.Store(true)
	}
	p, err := v.hydrate(id, rc)
	if err != nil {
		return nil, err
	}
	c, err := openLocal(p)
	if err != nil {
		return nil, err
	}
	v.remember(n, p, c.size)
	return v.checkSize(n, c)
}

// hydrate fills the cache with id, from rc if the caller already opened
// it and from the store otherwise.
func (v *View) hydrate(id c4.ID, rc io.ReadCloser) (string, error) {
	used := false
	open := func() (io.ReadCloser, error) {
		used = true
		if rc != nil {
			return rc, nil
		}
		r, err := v.src.Open(id)
		if err != nil {
			return nil, v.openErr(id, err)
		}
		return r, nil
	}
	defer func() {
		if !used && rc != nil {
			rc.Close()
		}
	}()
	cache, err := v.cacheFor(true)
	if err != nil {
		return "", err
	}
	return cache.fill(id, open)
}

// Seams for openLocal: the per-OS open and sharing-violation test.
var (
	openFile         = openShared
	sharingViolation = isSharingViolation
)

// openRetries bounds how often openLocal retries a sharing violation;
// with the backoff below it waits at most about 0.6s in all.
const openRetries = 12

// openLocal opens a local object file for random access. On Windows the
// file is opened sharing delete access as well as read and write, so a
// rename publishing it (by this process or another sharing the
// directory) neither refuses the open nor is refused by it; a sharing
// violation that still occurs, from some other holder of the file, is
// retried with a bounded backoff.
func openLocal(p string) (*content, error) {
	f, err := openFile(p)
	for i, delay := 0, time.Millisecond; err != nil && i < openRetries && sharingViolation(err); i++ {
		time.Sleep(delay)
		delay = min(2*delay, 100*time.Millisecond)
		f, err = openFile(p)
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &content{ra: f, c: f, size: info.Size()}, nil
}

// localPath finds id as a local file in the store or the cache.
func (v *View) localPath(id c4.ID) (string, bool) {
	if cp, ok := v.src.(contentPather); ok {
		if p, ok := cp.ContentPath(id); ok {
			return p, true
		}
	}
	if c, _ := v.cacheFor(false); c != nil {
		return c.path(id)
	}
	return "", false
}

func (v *View) remember(n *node, path string, size int64) {
	n.memo.Store(&memo{size: size, path: path})
}

// checkSize refuses content whose length contradicts the c4m entry.
func (v *View) checkSize(n *node, c *content) (*content, error) {
	if want := n.entry.Size; want >= 0 && c.size != want {
		c.close()
		return nil, fmt.Errorf("%w: %s is %d bytes, entry says %d", ErrSizeMismatch, n.entry.C4ID, c.size, want)
	}
	return c, nil
}

// openErr classifies a store Open failure: absent content is
// ErrContentMissing, anything else (a network error, say) passes
// through so it is not mistaken for a permanent absence.
func (v *View) openErr(id c4.ID, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w (%s)", ErrContentMissing, id)
	}
	if h, ok := v.src.(haser); ok && !h.Has(id) {
		return fmt.Errorf("%w (%s)", ErrContentMissing, id)
	}
	return fmt.Errorf("open %s: %w", id, err)
}

// size returns the size FileInfo reports. A file whose c4m size is null
// is measured from its content on first request (a stat of a local
// file, or a hydration otherwise) and remembered; if the content is
// unavailable the size is 0 and a read reports why.
func (v *View) size(n *node) int64 {
	e := n.entry
	switch {
	case n.dir != nil:
		return max(e.Size, 0)
	case e.Size >= 0:
		return e.Size
	case n.isSymlink():
		return int64(len(e.Target))
	case empty(e) || e.C4ID.IsNil():
		return 0
	}
	if m := n.memo.Load(); m != nil && m.size >= 0 {
		return m.size
	}
	if p, ok := v.localPath(e.C4ID); ok {
		if info, err := os.Stat(p); err == nil {
			v.remember(n, p, info.Size())
			return info.Size()
		}
	}
	c, err := v.openContent(n)
	if err != nil {
		return 0
	}
	c.close()
	if m := n.memo.Load(); m == nil || m.size < 0 {
		n.memo.Store(&memo{size: c.size})
	}
	return c.size
}

// readVerified reads a whole object (a manifest, directory record or ID
// list) and refuses it unless its bytes hash to id.
func (v *View) readVerified(id c4.ID) ([]byte, error) {
	if v.src == nil {
		return nil, ErrNoStore
	}
	rc, err := v.src.Open(id)
	if err != nil {
		return nil, v.openErr(id, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", id, err)
	}
	if got := c4.Identify(bytes.NewReader(data)); got != id {
		return nil, fmt.Errorf("%w: wanted %s, read %s", ErrIDMismatch, id, got)
	}
	return data, nil
}

// loadChain reads and decodes a stored manifest, directory record or
// patch chain. It splits sections as the c4 CLI does (c4m's
// DecodePatchChain), which accepts what c4 diff writes: a first-line
// base, the patch, and a closing ID, with consecutive IDs where diffs
// are appended to one file.
func (v *View) loadChain(id c4.ID) (*chain, error) {
	data, err := v.readVerified(id)
	if err != nil {
		return nil, err
	}
	sections, err := c4m.DecodePatchChain(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrNotManifest, id, err)
	}
	ch := &chain{rd: rangeData(data)}
	for i, s := range sections {
		if i == 0 {
			ch.base = s.BaseID
		}
		ch.sections = append(ch.sections, s.Entries)
	}
	return ch, nil
}

// rangeData collects a stream's inline ID-list lines (c4m's range data
// for folded sequences), keyed by their C4 ID.
func rangeData(data []byte) map[c4.ID]string {
	var rd map[c4.ID]string
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) <= 90 || len(line)%90 != 0 || !bytes.HasPrefix(line, []byte("c4")) {
			continue
		}
		valid := true
		for i := 0; i < len(line) && valid; i += 90 {
			_, err := c4.Parse(string(line[i : i+90]))
			valid = err == nil
		}
		if !valid {
			continue
		}
		if rd == nil {
			rd = make(map[c4.ID]string)
		}
		rd[c4.Identify(bytes.NewReader(line))] = string(line)
	}
	return rd
}
