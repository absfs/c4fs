package c4fs

import (
	"io"
	"io/fs"
	"os"
	"sync"

	"github.com/absfs/absfs"
)

// file is an open name in a View: a file to read or a directory to list.
// Content opens on the first read, not at Open, so opening and closing a
// file (or stat through it) never touches the store.
type file struct {
	v    *View
	n    *node
	name string

	mu     sync.Mutex
	closed bool
	off    int64    // read offset
	c      *content // nil until first read
	pos    int      // directory listing position
}

var _ absfs.File = (*file)(nil)

func (f *file) Name() string { return f.name }

// acquire returns the open content, opening it on first use. A failed
// open is not remembered, so a later read retries.
func (f *file) acquire(op string) (*content, error) {
	if f.closed {
		return nil, pathErr(op, f.name, errClosed)
	}
	if f.n.dir != nil {
		return nil, pathErr(op, f.name, errIsDir)
	}
	if f.c == nil {
		c, err := f.v.openContent(f.n)
		if err != nil {
			return nil, pathErr(op, f.name, err)
		}
		f.c = c
	}
	return f.c, nil
}

func (f *file) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, err := f.acquire("read")
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	if f.off >= c.size {
		return 0, io.EOF
	}
	n, err := c.ra.ReadAt(b, f.off)
	f.off += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, wrapRead(f.name, err)
}

// ReadAt reads at an offset without moving the read position. Calls may
// run in parallel.
func (f *file) ReadAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	c, err := f.acquire("read")
	f.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, pathErr("read", f.name, errInvalid)
	}
	if off >= c.size {
		if len(b) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n, err := c.ra.ReadAt(b, off)
	if err == nil && n < len(b) {
		err = io.EOF
	}
	return n, wrapRead(f.name, err)
}

func wrapRead(name string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return pathErr("read", name, err)
}

// Seek sets the read position. Seeking from the end uses the c4m size,
// so it does not open the content. A directory accepts only a seek to
// its start, which restarts its listing.
func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, pathErr("seek", f.name, errClosed)
	}
	if f.n.dir != nil {
		if offset != 0 || whence != io.SeekStart {
			return 0, pathErr("seek", f.name, errInvalid)
		}
		f.pos = 0
		return 0, nil
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.off + offset
	case io.SeekEnd:
		abs = f.v.size(f.n) + offset
	default:
		return 0, pathErr("seek", f.name, errInvalid)
	}
	if abs < 0 {
		return 0, pathErr("seek", f.name, errInvalid)
	}
	f.off = abs
	return abs, nil
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return pathErr("close", f.name, errClosed)
	}
	f.closed = true
	if f.c != nil {
		return f.c.close()
	}
	return nil
}

func (f *file) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, pathErr("stat", f.name, errClosed)
	}
	return f.v.info(f.n, f.name), nil
}

// Sync does nothing: there is nothing to flush.
func (f *file) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return pathErr("sync", f.name, errClosed)
	}
	return nil
}

// next returns up to n directory entries from the listing position,
// with os.File's pagination: for n > 0 at most n entries and io.EOF
// once none remain; for n <= 0 all remaining entries and a nil error.
func (f *file) next(op string, n int) ([]*node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, pathErr(op, f.name, errClosed)
	}
	if f.n.dir == nil {
		return nil, pathErr(op, f.name, errNotDir)
	}
	kids, err := f.v.kids(f.n)
	if err != nil {
		return nil, pathErr(op, f.name, err)
	}
	rest := kids[min(f.pos, len(kids)):]
	if n > 0 {
		if len(rest) == 0 {
			return nil, io.EOF
		}
		rest = rest[:min(n, len(rest))]
	}
	f.pos += len(rest)
	return rest, nil
}

func (f *file) ReadDir(n int) ([]fs.DirEntry, error) {
	kids, err := f.next("readdir", n)
	out := make([]fs.DirEntry, len(kids))
	for i, k := range kids {
		out[i] = &fileInfo{v: f.v, n: k, name: k.name}
	}
	return out, err
}

func (f *file) Readdir(n int) ([]os.FileInfo, error) {
	kids, err := f.next("readdir", n)
	out := make([]os.FileInfo, len(kids))
	for i, k := range kids {
		out[i] = &fileInfo{v: f.v, n: k, name: k.name}
	}
	return out, err
}

func (f *file) Readdirnames(n int) ([]string, error) {
	kids, err := f.next("readdirnames", n)
	out := make([]string, len(kids))
	for i, k := range kids {
		out[i] = k.name
	}
	return out, err
}

// The writing methods fail with ErrReadOnly (or ErrClosed after Close).

func (f *file) denied(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return pathErr(op, f.name, errClosed)
	}
	return pathErr(op, f.name, ErrReadOnly)
}

func (f *file) Write(b []byte) (int, error)              { return 0, f.denied("write") }
func (f *file) WriteAt(b []byte, off int64) (int, error) { return 0, f.denied("write") }
func (f *file) WriteString(s string) (int, error)        { return 0, f.denied("write") }
func (f *file) Truncate(size int64) error                { return f.denied("truncate") }
