package c4fs

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// Errors reported by the package. Filesystem methods (Stat, Open, Read
// and the rest) wrap them in an *fs.PathError naming the path and the
// operation. Open, OpenConfigured, New, View.Apply, Resume and Capture
// only add context (the C4 ID, say), so do not expect an *fs.PathError
// from them. Test for these errors with errors.Is.
var (
	// ErrContentMissing reports that a file's C4 ID is not in the store
	// (or no store was given). The file still lists and stats; only its
	// bytes are unavailable. It is deliberately not fs.ErrNotExist: the
	// file exists, its content does not.
	ErrContentMissing = errors.New("c4fs: content not in store")

	// ErrNoID reports a read of a non-empty file whose c4m entry has a
	// null C4 ID, so there is nothing to fetch.
	ErrNoID = errors.New("c4fs: entry has no C4 ID")

	// ErrIDMismatch reports content whose bytes do not hash to the C4 ID
	// that names them: a damaged or altered store object.
	ErrIDMismatch = errors.New("c4fs: content does not match its C4 ID")

	// ErrSizeMismatch reports content whose length differs from the size
	// its c4m entry records.
	ErrSizeMismatch = errors.New("c4fs: content size does not match its c4m entry")

	// ErrNoStore reports an operation that needs a store, such as loading
	// a manifest by ID, a directory record or a patch base, when the View
	// has none.
	ErrNoStore = errors.New("c4fs: no store")

	// ErrNotManifest reports that the object named as a manifest or
	// directory record does not parse as c4m.
	ErrNotManifest = errors.New("c4fs: object is not a c4m manifest")

	// ErrInvalidManifest reports a manifest that cannot be presented as a
	// tree: an entry whose depth skips its parent, or a name that is
	// empty, ".", ".." or contains a path separator.
	ErrInvalidManifest = errors.New("c4fs: invalid manifest")
)

// ErrReadOnly is the error a View's mutating methods wrap when no more
// specific error (EEXIST, ENOENT, ENOTDIR, EISDIR) applies:
// syscall.EROFS, "read-only file system". It is defined on every
// platform absfs builds for, Windows included (where Go defines it as
// an invented errno).
var ErrReadOnly error = syscall.EROFS

// errClosed is the os package's own sentinel, so callers checking
// errors.Is(err, os.ErrClosed) see the same thing as for an *os.File.
var errClosed = os.ErrClosed

// The syscall errors are used where an *os.File would report the same.
var (
	errNotDir   error = syscall.ENOTDIR
	errIsDir    error = syscall.EISDIR
	errInvalid  error = syscall.EINVAL
	errLoop     error = syscall.ELOOP
	errBusy     error = syscall.EBUSY
	errNotEmpty error = syscall.ENOTEMPTY
	errBadFD    error = syscall.EBADF
)

func pathErr(op, name string, err error) error {
	return &fs.PathError{Op: op, Path: name, Err: err}
}

// notExist reports whether err from a lookup in the tree means the name
// is absent. It is errors.Is(err, fs.ErrNotExist) except for errNotDir:
// on Windows syscall.ENOTDIR is ERROR_PATH_NOT_FOUND, which matches
// fs.ErrNotExist, and a path through a file would pass for a missing
// one (RemoveAll("/file/x") would succeed). The trees are virtual and
// must answer the same on every platform.
func notExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist) && !errors.Is(err, errNotDir)
}
