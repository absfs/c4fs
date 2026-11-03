//go:build !windows

package c4fs

import "os"

// openShared opens p for reading. Outside Windows an open file never
// blocks a rename or removal of its name, so os.Open serves.
func openShared(p string) (*os.File, error) { return os.Open(p) }

// isSharingViolation reports a Windows sharing violation, which no
// other system has.
func isSharingViolation(error) bool { return false }
