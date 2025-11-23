package c4fs

import (
	"io"

	"github.com/Avalanche-io/c4"
)

// Store provides content-addressable storage for C4 IDs.
// It stores content and retrieves it by C4 ID, providing the foundation
// for dehydration (content → C4 ID) and hydration (C4 ID → content).
type Store interface {
	// Put stores content and returns its C4 ID.
	// The C4 ID is computed from the content using SHA-512.
	Put(io.Reader) (c4.ID, error)

	// Get retrieves content by C4 ID.
	// Returns an error if the content does not exist.
	Get(c4.ID) (io.ReadCloser, error)

	// Has checks if content exists for the given C4 ID.
	Has(c4.ID) bool

	// Delete removes content for the given C4 ID.
	// Should implement reference counting in production use.
	Delete(c4.ID) error
}
