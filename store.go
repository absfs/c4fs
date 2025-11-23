package c4fs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/store"
)

// StoreAdapter wraps a c4/store.Store and provides high-level Put/Get operations
// that compute C4 IDs from content.
type StoreAdapter struct {
	store store.Store
}

// NewStoreAdapter creates a StoreAdapter from a c4/store.Store.
func NewStoreAdapter(s store.Store) *StoreAdapter {
	return &StoreAdapter{store: s}
}

// Put stores content and returns its C4 ID.
// The C4 ID is computed from the content using SHA-512.
// If the content already exists in the store, it returns the ID without error.
func (s *StoreAdapter) Put(r io.Reader) (c4.ID, error) {
	// Read content to compute C4 ID
	data, err := io.ReadAll(r)
	if err != nil {
		return c4.ID{}, fmt.Errorf("failed to read content: %w", err)
	}

	// Compute C4 ID from content
	id := c4.Identify(bytes.NewReader(data))

	// Check if already exists (deduplication)
	if s.Has(id) {
		return id, nil
	}

	// Create write handle in store
	wc, err := s.store.Create(id)
	if err != nil {
		return c4.ID{}, fmt.Errorf("failed to create in store: %w", err)
	}

	// Write content
	_, err = io.Copy(wc, bytes.NewReader(data))
	if err != nil {
		wc.Close()
		return c4.ID{}, fmt.Errorf("failed to write content: %w", err)
	}

	// Close writer
	if err := wc.Close(); err != nil {
		return c4.ID{}, fmt.Errorf("failed to close writer: %w", err)
	}

	return id, nil
}

// Get retrieves content by C4 ID.
// Returns an error if the content does not exist.
func (s *StoreAdapter) Get(id c4.ID) (io.ReadCloser, error) {
	return s.store.Open(id)
}

// Has checks if content exists for the given C4 ID.
// This is a best-effort check - tries to open and immediately close.
func (s *StoreAdapter) Has(id c4.ID) bool {
	rc, err := s.store.Open(id)
	if err != nil {
		return false
	}
	rc.Close()
	return true
}

// Delete removes content for the given C4 ID.
func (s *StoreAdapter) Delete(id c4.ID) error {
	return s.store.Remove(id)
}
