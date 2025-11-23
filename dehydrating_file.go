package c4fs

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/Avalanche-io/c4/c4m"
)

// dehydratingFile buffers writes and dehydrates content to the store on Close.
type dehydratingFile struct {
	c4fs *FS
	name string
	perm fs.FileMode
	buf  *bytes.Buffer
	pos  int64
}

// newDehydratingFile creates a new file for writing.
func newDehydratingFile(c4fs *FS, name string, perm fs.FileMode) (*dehydratingFile, error) {
	return &dehydratingFile{
		c4fs: c4fs,
		name: filepath.Clean(name),
		perm: perm,
		buf:  new(bytes.Buffer),
		pos:  0,
	}, nil
}

// Write writes data to the buffer.
func (f *dehydratingFile) Write(p []byte) (int, error) {
	n, err := f.buf.Write(p)
	f.pos += int64(n)
	return n, err
}

// WriteAt writes data at the specified offset.
func (f *dehydratingFile) WriteAt(p []byte, off int64) (int, error) {
	// For simplicity, only support sequential writes
	if off != f.pos {
		return 0, fmt.Errorf("non-sequential writes not supported")
	}
	return f.Write(p)
}

// WriteString writes a string to the buffer.
func (f *dehydratingFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Read is not supported on write-only files.
func (f *dehydratingFile) Read(p []byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: f.name,
		Err:  fmt.Errorf("file opened for writing"),
	}
}

// Seek changes the file position.
func (f *dehydratingFile) Seek(offset int64, whence int) (int64, error) {
	// For simplicity, only support seeking to current position
	switch whence {
	case io.SeekCurrent:
		f.pos += offset
		return f.pos, nil
	case io.SeekStart:
		f.pos = offset
		return f.pos, nil
	case io.SeekEnd:
		f.pos = int64(f.buf.Len()) + offset
		return f.pos, nil
	default:
		return 0, fmt.Errorf("invalid whence")
	}
}

// Stat returns file information.
func (f *dehydratingFile) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name:    filepath.Base(f.name),
		size:    int64(f.buf.Len()),
		mode:    f.perm,
		modTime: time.Now().UTC(),
		isDir:   false,
	}, nil
}

// Sync is a no-op for buffered files.
func (f *dehydratingFile) Sync() error {
	return nil
}

// Close dehydrates the buffered content to the store and updates the manifest.
func (f *dehydratingFile) Close() error {
	// Get buffered data
	data := f.buf.Bytes()

	// Dehydrate to store
	id, err := f.c4fs.store.Put(bytes.NewReader(data))
	if err != nil {
		return &fs.PathError{
			Op:   "close",
			Path: f.name,
			Err:  fmt.Errorf("failed to dehydrate content: %w", err),
		}
	}

	// Create entry in layer
	entry := &c4m.Entry{
		Mode:      f.perm,
		Timestamp: time.Now().UTC(),
		Size:      int64(len(data)),
		Name:      f.name,
		C4ID:      id,
	}

	f.c4fs.mu.Lock()
	f.c4fs.layer.AddEntry(entry)
	f.c4fs.mu.Unlock()

	return nil
}
