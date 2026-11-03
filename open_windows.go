//go:build windows

package c4fs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// errSharingViolation is ERROR_SHARING_VIOLATION, which package syscall
// does not name.
const errSharingViolation = syscall.Errno(32)

// openShared opens p for reading as os.Open does, but shares delete
// access as well as read and write. os.Open shares only read and
// write, so it is refused while any other handle on the file holds
// delete access, and a rename (MoveFileEx) holds one on the file it
// moves until the move returns, after the new name is already visible.
// A cache or TreeStore object can be found at its name inside that
// window. Sharing delete access also lets the file be moved (a
// TreeStore splitting its directory) or removed while this handle is
// open. Windows still refuses a rename that replaces it.
func openShared(p string) (*os.File, error) {
	name, err := syscall.UTF16PtrFromString(longPath(p))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: p, Err: err}
	}
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: p, Err: err}
	}
	return os.NewFile(uintptr(h), p), nil
}

// longPath gives a path whose absolute form reaches the classic
// MAX_PATH limit the extended-length prefix os.Open would add for it:
// \\?\ for a drive path and \\?\UNC\ for a network share. It measures
// the absolute form, so a short relative path under a deep working
// directory is prefixed too. A path already in extended-length or
// device form is left alone.
func longPath(p string) string {
	if strings.HasPrefix(p, `\\?\`) || strings.HasPrefix(p, `\??\`) || strings.HasPrefix(p, `\\.\`) {
		return p
	}
	if filepath.IsAbs(p) && len(p) < 248 {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil || len(abs) < 248 {
		return p
	}
	if strings.HasPrefix(abs, `\\`) {
		return `\\?\UNC\` + abs[2:]
	}
	return `\\?\` + abs
}

// isSharingViolation reports ERROR_SHARING_VIOLATION.
func isSharingViolation(err error) bool {
	return errors.Is(err, errSharingViolation)
}
