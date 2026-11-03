package c4fs

import (
	"errors"
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

// content returns the content handle for op, or the error a closed
// handle or a directory gives. A closed handle is checked here rather
// than left to the content handle: not every scratch filesystem refuses
// a seek on a closed file.
func (f *wsFile) content(op string) (absfs.File, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	switch {
	case closed:
		return nil, pathErr(op, f.name, errClosed)
	case f.dir != nil:
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
		return nil, pathErr(op, f.name, errBadFD)
	}
	return c, nil
}

// ioErr names the workspace path in an error from the content handle,
// whose own path error may name a scratch file. io.EOF passes through.
func (f *wsFile) ioErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return pathErr(op, f.name, scratchErr(err))
}

func (f *wsFile) Read(b []byte) (int, error) {
	c, err := f.content("read")
	if err != nil {
		return 0, err
	}
	n, err := c.Read(b)
	return n, f.ioErr("read", err)
}

// ReadAt reads at off. A negative offset is EINVAL, as for an *os.File,
// whatever the scratch filesystem would make of it.
func (f *wsFile) ReadAt(b []byte, off int64) (int, error) {
	c, err := f.content("read")
	if err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, pathErr("read", f.name, errInvalid)
	}
	n, err := c.ReadAt(b, off)
	return n, f.ioErr("read", err)
}

func (f *wsFile) Write(b []byte) (int, error) {
	c, err := f.writable("write")
	if err != nil {
		return 0, err
	}
	n, err := c.Write(b)
	f.wrote(n)
	return n, f.ioErr("write", err)
}

// WriteAt writes at off. A negative offset is EINVAL: an NFS WRITE at an
// offset of 2^63 or more arrives as one.
func (f *wsFile) WriteAt(b []byte, off int64) (int, error) {
	c, err := f.writable("write")
	if err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, pathErr("write", f.name, errInvalid)
	}
	n, err := c.WriteAt(b, off)
	f.wrote(n)
	return n, f.ioErr("write", err)
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
		return f.ioErr("truncate", err)
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

// Seek sets the offset of a file. A seek to before the start, or with
// an unknown whence, is EINVAL, as for an *os.File. A directory accepts
// only a seek to its start, which restarts its listing: the next read
// lists the directory as it is then.
func (f *wsFile) Seek(offset int64, whence int) (int64, error) {
	if f.dir == nil {
		c, err := f.content("seek")
		if err != nil {
			return 0, err
		}
		pos, err := f.seekTarget(c, offset, whence)
		if err != nil {
			return 0, err
		}
		pos, err = c.Seek(pos, io.SeekStart)
		return pos, f.ioErr("seek", err)
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

// seekTarget is the absolute offset a seek on a file asks for. Not every
// scratch filesystem checks it (memfs accepts a negative one), so it is
// checked here and the seek made from the start.
func (f *wsFile) seekTarget(c absfs.File, offset int64, whence int) (int64, error) {
	var from int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		cur, err := c.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, f.ioErr("seek", err)
		}
		from = cur
	case io.SeekEnd:
		fi, err := c.Stat()
		if err != nil {
			return 0, f.ioErr("seek", err)
		}
		from = fi.Size()
	default:
		return 0, pathErr("seek", f.name, errInvalid)
	}
	pos := from + offset // from >= 0, so an overflow is negative too
	if pos < 0 {
		return 0, pathErr("seek", f.name, errInvalid)
	}
	return pos, nil
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
		err = f.ioErr("close", f.inner.Close())
	}
	if f.counted {
		f.w.release(f.n)
	}
	return err
}

// Stat describes what the handle was opened on as it is now, as fstat
// does: a later Chmod or write shows, even on a handle opened before
// the name was changed. A name renamed since it was opened is described
// under its name at open.
func (f *wsFile) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return nil, pathErr("stat", f.name, errClosed)
	}
	f.w.mu.RLock()
	defer f.w.mu.RUnlock()
	switch {
	case f.dir != nil:
		return f.w.info(f.w.current(*f.dir), f.name), nil
	case f.n != nil:
		return f.w.nodeInfo(f.n, path.Base(f.name)), nil
	}
	return f.w.info(f.w.current(ref{b: f.b}), f.name), nil
}

// Sync flushes the content to the scratch filesystem. On a directory it
// does nothing.
func (f *wsFile) Sync() error {
	c, err := f.content("sync")
	switch {
	case err == nil:
		return f.ioErr("sync", c.Sync())
	case f.dir != nil && errors.Is(err, errIsDir):
		return nil
	}
	return err
}

// next returns up to n directory entries from the listing position,
// with os.File's pagination. The listing is read on the first call and
// again after a rewind, so pages of one pass never skip or repeat.
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
		// The directory as it is now, not as it was at open: entries
		// made since, and rewinddir, show them.
		f.w.mu.RLock()
		ents, err := f.w.list(f.w.current(*f.dir))
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
