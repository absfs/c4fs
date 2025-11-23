package c4fs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Avalanche-io/c4"
)

// LocalStore is a disk-based Store implementation.
// Stores content in a directory hierarchy based on C4 ID prefixes.
// Layout: /base/[first-2]/[next-2]/[full-c4-id]
// Example: /var/c4/c4/1a/c41a2b3c...
type LocalStore struct {
	basePath string
}

// NewLocalStore creates a new disk-based store.
func NewLocalStore(basePath string) (*LocalStore, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalStore{
		basePath: basePath,
	}, nil
}

// idPath returns the file path for a C4 ID.
// Format: /base/[first-2]/[next-2]/[full-c4-id]
func (s *LocalStore) idPath(id c4.ID) string {
	idStr := id.String()
	if len(idStr) < 4 {
		// Shouldn't happen with valid C4 IDs, but handle gracefully
		return filepath.Join(s.basePath, idStr)
	}
	return filepath.Join(s.basePath, idStr[:2], idStr[2:4], idStr)
}

// Put stores content and returns its C4 ID.
func (s *LocalStore) Put(r io.Reader) (c4.ID, error) {
	// Read content to compute C4 ID
	data, err := io.ReadAll(r)
	if err != nil {
		return c4.ID{}, fmt.Errorf("failed to read content: %w", err)
	}

	// Compute C4 ID
	id := c4.Identify(bytes.NewReader(data))

	// Get file path
	path := s.idPath(id)

	// Check if already exists
	if _, err := os.Stat(path); err == nil {
		// Already exists, nothing to do
		return id, nil
	}

	// Create directory structure
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return c4.ID{}, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write content to file
	if err := os.WriteFile(path, data, 0644); err != nil {
		return c4.ID{}, fmt.Errorf("failed to write file: %w", err)
	}

	return id, nil
}

// Get retrieves content by C4 ID.
func (s *LocalStore) Get(id c4.ID) (io.ReadCloser, error) {
	path := s.idPath(id)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("content not found for C4 ID: %s", id)
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

// Has checks if content exists for the given C4 ID.
func (s *LocalStore) Has(id c4.ID) bool {
	path := s.idPath(id)
	_, err := os.Stat(path)
	return err == nil
}

// Delete removes content for the given C4 ID.
func (s *LocalStore) Delete(id c4.ID) error {
	path := s.idPath(id)

	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}

	// Try to remove empty parent directories
	dir := filepath.Dir(path)
	os.Remove(dir) // Ignore errors - may not be empty
	parentDir := filepath.Dir(dir)
	os.Remove(parentDir) // Ignore errors - may not be empty

	return nil
}
