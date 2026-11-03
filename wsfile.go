package c4fs

import (
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/absfs/absfs"
)

// wsFile is an open name in a Workspace: a directory, a file of the
// View, or a changed file, whose content is a scratch file or, if only
// its metadata changed, the View's.
type wsFile struct {
	w     *Workspace
	name  string
	n     *wnode     // a changed file, or nil
	b     *node      // a View file, when n is nil
	dir   *ref       // non-nil for a directory
	inner absfs.File // the content; nil for a directory

	write   bool // opened for writing
	counted bool // holds a reference on n's scratch file

	mu     sync.Mutex
	closed bool
	ents   []fs.DirEntry // the directory's listing, read on first use
	pos    int
}

var _ absfs.File = (*wsFile)(nil)

func (f *wsFile) Name() string { return f.name }

// content returns the content handle for op, or the error a directory
// or a closed handle gives.
func (f *wsFile) content(op string) (absfs.File, error) {
	if f.dir != nil {
		f.mu.Lock()
		closed := f.closed
		f.mu.Unlock()
		if closed {
			return nil, pathErr(op, f.name, errClosed)
		}
		return nil, pathErr(op, f.name, errIsDir)
	}
	return f.inner, nil
}

// writable returns the content handle for a write.
func (f *wsFile) writable(op string) (absfs.File, error) {
	c, err := f.content(op)
	if err != nil {
		return nil, err
	}
	if !f.write {
		f.mu.Lock()
		closed := f.closed
		f.mu.Unlock()
		if closed {
			return nil, pathErr(op, f.name, errClosed)
		}
		return nil, pathErr(op, f.name, errBadFD)
	}
	return c, nil
}

func (f *wsFile) Read(b []byte) (int, error) {
	c, err := f.content("read")
	if err != nil {
		return 0, err
	}
	return c.Read(b)
}

func (f *wsFile) ReadAt(b []byte, off int64) (int, error) {
	c, err := f.content("read")
	if err != nil {
		return 0, err
	}
	return c.ReadAt(b, off)
}

func (f *wsFile) Write(b []byte) (int, error) {
	c, err := f.writable("write")
	if err != nil {
		return 0, err
	}
	n, err := c.Write(b)
	f.wrote(n)
	return n, err
}

func (f *wsFile) WriteAt(b []byte, off int64) (int, error) {
	c, err := f.writable("write")
	if err != nil {
		return 0, err
	}
	n, err := c.WriteAt(b, off)
	f.wrote(n)
	return n, err
}

func (f *wsFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Truncate changes the file's size.
func (f *wsFile) Truncate(size int64) error {
	c, err := f.writable("truncate")
	if err != nil {
		return err
	}
	if size < 0 {
		return pathErr("truncate", f.name, errInvalid)
	}
	if err := c.Truncate(size); err != nil {
		return err
	}
	f.w.wrote(f.n)
	return nil
}

// wrote records that n bytes were written.
func (f *wsFile) wrote(n int) {
	if n > 0 {
		f.w.wrote(f.n)
	}
}

// Seek sets the offset of a file. A directory accepts only a seek to
// its start, which restarts its listing.
func (f *wsFile) Seek(offset int64, whence int) (int64, error) {
	if f.dir == nil {
		return f.inner.Seek(offset, whence)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, pathErr("seek", f.name, errClosed)
	}
	if offset != 0 || whence != io.SeekStart {
		return 0, pathErr("seek", f.name, errInvalid)
	}
	f.ents, f.pos = nil, 0
	return 0, nil
}

func (f *wsFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return pathErr("close", f.name, errClosed)
	}
	f.closed = true
	f.mu.Unlock()
	var err error
	if f.inner != nil {
		err = f.inner.Close()
	}
	if f.counted {
		f.w.release(f.n)
	}
	return err
}

// Stat describes the open name as it is now: a changed file renamed
// since it was opened is described under its name at open.
func (f *wsFile) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return nil, pathErr("stat", f.name, errClosed)
	}
	switch {
	case f.dir != nil:
		f.w.mu.RLock()
		defer f.w.mu.RUnlock()
		return f.w.info(*f.dir, f.name), nil
	case f.n != nil:
		f.w.mu.RLock()
		defer f.w.mu.RUnlock()
		return f.w.nodeInfo(f.n, path.Base(f.name)), nil
	}
	return f.w.v.info(f.b, f.name), nil
}

func (f *wsFile) Sync() error {
	if f.dir == nil {
		return f.inner.Sync()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return pathErr("sync", f.name, errClosed)
	}
	return nil
}

// next returns up to n directory entries from the listing position,
// with os.File's pagination.
func (f *wsFile) next(op string, n int) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, pathErr(op, f.name, errClosed)
	}
	if f.dir == nil {
		return nil, pathErr(op, f.name, errNotDir)
	}
	if f.ents == nil {
		f.w.mu.RLock()
		ents, err := f.w.list(*f.dir)
		f.w.mu.RUnlock()
		if err != nil {
			return nil, pathErr(op, f.name, err)
		}
		f.ents = ents
	}
	rest := f.ents[min(f.pos, len(f.ents)):]
	if n > 0 {
		if len(rest) == 0 {
			return nil, io.EOF
		}
		rest = rest[:min(n, len(rest))]
	}
	f.pos += len(rest)
	return rest, nil
}

func (f *wsFile) ReadDir(n int) ([]fs.DirEntry, error) {
	ents, err := f.next("readdir", n)
	return append([]fs.DirEntry(nil), ents...), err
}

func (f *wsFile) Readdir(n int) ([]os.FileInfo, error) {
	ents, err := f.next("readdir", n)
	out := make([]os.FileInfo, len(ents))
	for i, e := range ents {
		out[i], _ = e.Info()
	}
	return out, err
}

func (f *wsFile) Readdirnames(n int) ([]string, error) {
	ents, err := f.next("readdirnames", n)
	out := make([]string, len(ents))
	for i, e := range ents {
		out[i] = e.Name()
	}
	return out, err
}

// wsInfo describes a changed name: a snapshot taken when it was asked
// for. Sys returns a *c4m.Entry of the same snapshot.
type wsInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	mtime time.Time
	sys   any
}

var (
	_ fs.FileInfo = (*wsInfo)(nil)
	_ fs.DirEntry = (*wsInfo)(nil)
)

func (fi *wsInfo) Name() string               { return fi.name }
func (fi *wsInfo) Size() int64                { return fi.size }
func (fi *wsInfo) Mode() fs.FileMode          { return fi.mode }
func (fi *wsInfo) ModTime() time.Time         { return fi.mtime }
func (fi *wsInfo) IsDir() bool                { return fi.mode.IsDir() }
func (fi *wsInfo) Sys() any                   { return fi.sys }
func (fi *wsInfo) Type() fs.FileMode          { return fi.mode.Type() }
func (fi *wsInfo) Info() (fs.FileInfo, error) { return fi, nil }
func (fi *wsInfo) String() string             { return fs.FormatDirEntry(fi) }
