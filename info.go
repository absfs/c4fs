package c4fs

import (
	"io/fs"
	"time"
)

// fileInfo describes a node. It is both the fs.FileInfo of Stat and the
// fs.DirEntry of ReadDir.
type fileInfo struct {
	v    *View
	n    *node
	name string
}

var (
	_ fs.FileInfo = (*fileInfo)(nil)
	_ fs.DirEntry = (*fileInfo)(nil)
)

// Name returns the base name.
func (fi *fileInfo) Name() string { return fi.name }

// Size returns the c4m size. For a directory that is the total content
// size beneath it, as c4m records; null sizes are resolved from content
// for files and reported as 0 for directories.
func (fi *fileInfo) Size() int64 { return fi.v.size(fi.n) }

// Mode returns the c4m mode, with null modes defaulted.
func (fi *fileInfo) Mode() fs.FileMode { return fi.n.mode() }

// ModTime returns the c4m timestamp. A null timestamp is the Unix epoch,
// c4m's internal value for it.
func (fi *fileInfo) ModTime() time.Time { return fi.n.entry.Timestamp }

// IsDir reports a directory.
func (fi *fileInfo) IsDir() bool { return fi.n.dir != nil }

// Sys returns the *c4m.Entry behind the name, which carries its C4 ID.
// It must not be modified. For a member of a folded sequence it is a
// synthesised entry for that member; for the root it is synthesised too.
func (fi *fileInfo) Sys() any { return fi.n.entry }

// Type returns the type bits of Mode, for fs.DirEntry.
func (fi *fileInfo) Type() fs.FileMode { return fi.n.mode().Type() }

// Info returns the entry itself, for fs.DirEntry.
func (fi *fileInfo) Info() (fs.FileInfo, error) { return fi, nil }

// String formats the entry as fs.FormatDirEntry does.
func (fi *fileInfo) String() string { return fs.FormatDirEntry(fi) }
