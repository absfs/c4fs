package c4fs

import (
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// TestReferencedIDs tests that ReferencedIDs correctly identifies all referenced C4 IDs.
func TestReferencedIDs(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Write some files
	err := c4fs.WriteFile("file1.txt", []byte("content1"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	err = c4fs.WriteFile("file2.txt", []byte("content2"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	// Create a directory (should not be in referenced IDs)
	err = c4fs.Mkdir("subdir", 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	err = c4fs.WriteFile("subdir/file3.txt", []byte("content3"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file3: %v", err)
	}

	// Get referenced IDs
	refs := c4fs.ReferencedIDs()

	// Should have 3 IDs (file1, file2, file3 - not the directory)
	if len(refs) != 3 {
		t.Errorf("Expected 3 referenced IDs, got %d", len(refs))
	}

	// Remove a file and check that its ID is no longer referenced
	err = c4fs.Remove("file1.txt")
	if err != nil {
		t.Fatalf("Failed to remove file1: %v", err)
	}

	refs = c4fs.ReferencedIDs()
	if len(refs) != 2 {
		t.Errorf("Expected 2 referenced IDs after removal, got %d", len(refs))
	}
}

// TestReferencedIDsWithLayer tests ReferencedIDs with a layered filesystem.
func TestReferencedIDsWithLayer(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base manifest with some files
	base := c4m.NewManifest()
	c4fs := New(base, adapter)

	err := c4fs.WriteFile("base_file.txt", []byte("base content"), 0644)
	if err != nil {
		t.Fatalf("Failed to write base_file: %v", err)
	}

	err = c4fs.WriteFile("overwrite_file.txt", []byte("base version"), 0644)
	if err != nil {
		t.Fatalf("Failed to write overwrite_file: %v", err)
	}

	// Flatten to create base
	base = c4fs.Flatten()

	// Create new filesystem with this base
	c4fs2 := New(base, adapter)

	// Overwrite a file in the layer
	err = c4fs2.WriteFile("overwrite_file.txt", []byte("layer version"), 0644)
	if err != nil {
		t.Fatalf("Failed to overwrite file: %v", err)
	}

	// Add a new file in the layer
	err = c4fs2.WriteFile("layer_file.txt", []byte("layer content"), 0644)
	if err != nil {
		t.Fatalf("Failed to write layer_file: %v", err)
	}

	// Get referenced IDs
	refs := c4fs2.ReferencedIDs()

	// Should have 3 unique IDs:
	// 1. base_file.txt (from base)
	// 2. overwrite_file.txt (layer version, base version is shadowed)
	// 3. layer_file.txt (from layer)
	if len(refs) != 3 {
		t.Errorf("Expected 3 referenced IDs, got %d", len(refs))
	}

	// Remove a file from base via tombstone
	err = c4fs2.Remove("base_file.txt")
	if err != nil {
		t.Fatalf("Failed to remove base_file: %v", err)
	}

	refs = c4fs2.ReferencedIDs()
	// Should now have 2 IDs (base_file is tombstoned)
	if len(refs) != 2 {
		t.Errorf("Expected 2 referenced IDs after tombstone, got %d", len(refs))
	}
}

// TestReferencedIDsEmptyFiles tests that empty files (Size = 0) are not included.
func TestReferencedIDsEmptyFiles(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Write an empty file
	err := c4fs.WriteFile("empty.txt", []byte(""), 0644)
	if err != nil {
		t.Fatalf("Failed to write empty file: %v", err)
	}

	// Write a non-empty file
	err = c4fs.WriteFile("nonempty.txt", []byte("content"), 0644)
	if err != nil {
		t.Fatalf("Failed to write nonempty file: %v", err)
	}

	refs := c4fs.ReferencedIDs()

	// Should only have 1 ID (the non-empty file)
	if len(refs) != 1 {
		t.Errorf("Expected 1 referenced ID (empty files excluded), got %d", len(refs))
	}
}
