package c4fs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// FileSystem represents a filesystem interface compatible with io/fs.FS
// and extended with write operations.
type FileSystem interface {
	// Read operations (from io/fs.FS)
	Open(name string) (fs.File, error)

	// Extended read operations
	Stat(name string) (fs.FileInfo, error)
	ReadDir(name string) ([]fs.DirEntry, error)
	ReadFile(name string) ([]byte, error)

	// Write operations
	Create(name string) (File, error)
	Mkdir(name string, perm fs.FileMode) error
	MkdirAll(name string, perm fs.FileMode) error
	Remove(name string) error
	RemoveAll(name string) error
	WriteFile(name string, data []byte, perm fs.FileMode) error

	// Utility operations
	Rename(oldname, newname string) error
}

// File represents an open file with read/write capabilities.
type File interface {
	fs.File // Embeds Read, Close, Stat

	// Write operations
	Write(p []byte) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	WriteString(s string) (n int, err error)

	// Read operations
	ReadAt(p []byte, off int64) (n int, err error)

	// Seek operation
	Seek(offset int64, whence int) (int64, error)

	// Sync operation
	Sync() error

	// Truncate operation
	Truncate(size int64) error

	// Directory operations
	Readdirnames(n int) (names []string, err error)
	ReadDir(n int) ([]fs.DirEntry, error)
}

// FileInfo is an alias for fs.FileInfo for convenience.
type FileInfo = fs.FileInfo

// fileInfo implements fs.FileInfo for C4M entries.
type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() interface{}   { return nil }

// dirEntry implements fs.DirEntry for C4M entries.
type dirEntry struct {
	info *fileInfo
}

func (d *dirEntry) Name() string             { return d.info.Name() }
func (d *dirEntry) IsDir() bool              { return d.info.IsDir() }
func (d *dirEntry) Type() fs.FileMode        { return d.info.Mode().Type() }
func (d *dirEntry) Info() (fs.FileInfo, error) { return d.info, nil }

// readOnlyFile wraps a ReadCloser to implement fs.File.
type readOnlyFile struct {
	io.ReadCloser
	info *fileInfo
	pos  int64
}

func (f *readOnlyFile) Stat() (fs.FileInfo, error) {
	return f.info, nil
}

func (f *readOnlyFile) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	f.pos += int64(n)
	return n, err
}

func (f *readOnlyFile) ReadDir(n int) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{
		Op:   "readdir",
		Path: f.info.name,
		Err:  fs.ErrInvalid,
	}
}

func (f *readOnlyFile) Write(p []byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "write",
		Path: f.info.name,
		Err:  fs.ErrPermission,
	}
}

func (f *readOnlyFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{
		Op:   "write",
		Path: f.info.name,
		Err:  fs.ErrPermission,
	}
}

func (f *readOnlyFile) WriteString(s string) (int, error) {
	return 0, &fs.PathError{
		Op:   "write",
		Path: f.info.name,
		Err:  fs.ErrPermission,
	}
}

func (f *readOnlyFile) ReadAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: f.info.name,
		Err:  fmt.Errorf("ReadAt not supported on streaming files"),
	}
}

func (f *readOnlyFile) Seek(offset int64, whence int) (int64, error) {
	return 0, &fs.PathError{
		Op:   "seek",
		Path: f.info.name,
		Err:  fmt.Errorf("Seek not supported on streaming files"),
	}
}

func (f *readOnlyFile) Sync() error {
	return nil
}

func (f *readOnlyFile) Truncate(size int64) error {
	return &fs.PathError{
		Op:   "truncate",
		Path: f.info.name,
		Err:  fs.ErrPermission,
	}
}

func (f *readOnlyFile) Readdirnames(n int) ([]string, error) {
	return nil, &fs.PathError{
		Op:   "readdirnames",
		Path: f.info.name,
		Err:  fs.ErrInvalid,
	}
}

func (f *readOnlyFile) Name() string {
	return f.info.name
}

// writeFile implements File for write operations.
type writeFile struct {
	*os.File
}

func (f *writeFile) WriteString(s string) (int, error) {
	return f.File.WriteString(s)
}
