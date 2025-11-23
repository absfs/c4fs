package c4fs

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/Avalanche-io/c4"
)

// MemoryStore is an in-memory Store implementation.
// Useful for testing and temporary storage.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string][]byte),
	}
}

// Put stores content and returns its C4 ID.
func (s *MemoryStore) Put(r io.Reader) (c4.ID, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return c4.ID{}, fmt.Errorf("failed to read content: %w", err)
	}

	// Compute C4 ID from content
	id := c4.Identify(bytes.NewReader(data))

	s.mu.Lock()
	s.data[id.String()] = data
	s.mu.Unlock()

	return id, nil
}

// Get retrieves content by C4 ID.
func (s *MemoryStore) Get(id c4.ID) (io.ReadCloser, error) {
	s.mu.RLock()
	data, ok := s.data[id.String()]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("content not found for C4 ID: %s", id)
	}

	// Return a copy to avoid mutations
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Has checks if content exists for the given C4 ID.
func (s *MemoryStore) Has(id c4.ID) bool {
	s.mu.RLock()
	_, ok := s.data[id.String()]
	s.mu.RUnlock()
	return ok
}

// Delete removes content for the given C4 ID.
func (s *MemoryStore) Delete(id c4.ID) error {
	s.mu.Lock()
	delete(s.data, id.String())
	s.mu.Unlock()
	return nil
}

// Size returns the number of items in the store.
func (s *MemoryStore) Size() int {
	s.mu.RLock()
	size := len(s.data)
	s.mu.RUnlock()
	return size
}
