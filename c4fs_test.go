package c4fs

import (
	"bytes"
	"io"
	"io/fs"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
)

func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()

	// Test Put
	content := []byte("Hello, C4FS!")
	id, err := store.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Has
	if !store.Has(id) {
		t.Error("Has returned false for existing content")
	}

	// Test Get
	rc, err := store.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	retrieved, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read from Get: %v", err)
	}

	if !bytes.Equal(content, retrieved) {
		t.Errorf("Retrieved content mismatch: got %q, want %q", retrieved, content)
	}

	// Test Delete
	if err := store.Delete(id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if store.Has(id) {
		t.Error("Has returned true after deletion")
	}
}

func TestC4FSBasicOperations(t *testing.T) {
	// Create filesystem with memory store
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Test WriteFile (dehydration)
	testContent := []byte("This is a test file")
	err := c4fs.WriteFile("test.txt", testContent, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Test Stat
	info, err := c4fs.Stat("test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name() != "test.txt" {
		t.Errorf("Stat name: got %q, want %q", info.Name(), "test.txt")
	}

	if info.Size() != int64(len(testContent)) {
		t.Errorf("Stat size: got %d, want %d", info.Size(), len(testContent))
	}

	if info.IsDir() {
		t.Error("Stat IsDir: got true, want false")
	}

	// Test ReadFile (hydration)
	retrieved, err := c4fs.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(testContent, retrieved) {
		t.Errorf("ReadFile content mismatch: got %q, want %q", retrieved, testContent)
	}
}

func TestC4FSDeduplication(t *testing.T) {
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Write same content to different files
	content := []byte("Duplicate content")

	err := c4fs.WriteFile("file1.txt", content, 0644)
	if err != nil {
		t.Fatalf("WriteFile file1.txt failed: %v", err)
	}

	err = c4fs.WriteFile("file2.txt", content, 0644)
	if err != nil {
		t.Fatalf("WriteFile file2.txt failed: %v", err)
	}

	// Store should only have one copy
	if store.Size() != 1 {
		t.Errorf("Store size: got %d, want 1 (deduplication)", store.Size())
	}

	// Both files should be readable
	data1, err := c4fs.ReadFile("file1.txt")
	if err != nil {
		t.Fatalf("ReadFile file1.txt failed: %v", err)
	}

	data2, err := c4fs.ReadFile("file2.txt")
	if err != nil {
		t.Fatalf("ReadFile file2.txt failed: %v", err)
	}

	if !bytes.Equal(data1, content) {
		t.Error("file1.txt content mismatch")
	}

	if !bytes.Equal(data2, content) {
		t.Error("file2.txt content mismatch")
	}
}

func TestC4FSLayering(t *testing.T) {
	store := NewMemoryStore()

	// Create base manifest with a file
	base := c4m.NewManifest()
	baseContent := []byte("Base file content")
	baseID, _ := store.Put(bytes.NewReader(baseContent))

	base.AddEntry(&c4m.Entry{
		Mode: 0644,
		Size: int64(len(baseContent)),
		Name: "base.txt",
		C4ID: baseID,
	})

	// Create filesystem with base
	c4fs := New(base, store)

	// File from base should be readable
	data, err := c4fs.ReadFile("base.txt")
	if err != nil {
		t.Fatalf("ReadFile base.txt failed: %v", err)
	}

	if !bytes.Equal(data, baseContent) {
		t.Error("base.txt content mismatch")
	}

	// Write new file to layer
	layerContent := []byte("Layer file content")
	err = c4fs.WriteFile("layer.txt", layerContent, 0644)
	if err != nil {
		t.Fatalf("WriteFile layer.txt failed: %v", err)
	}

	// Both files should be readable
	baseData, err := c4fs.ReadFile("base.txt")
	if err != nil {
		t.Fatalf("ReadFile base.txt after layer write failed: %v", err)
	}

	layerData, err := c4fs.ReadFile("layer.txt")
	if err != nil {
		t.Fatalf("ReadFile layer.txt failed: %v", err)
	}

	if !bytes.Equal(baseData, baseContent) {
		t.Error("base.txt content changed after layer write")
	}

	if !bytes.Equal(layerData, layerContent) {
		t.Error("layer.txt content mismatch")
	}

	// Flatten should merge base and layer
	merged := c4fs.Flatten()
	if merged.GetEntry("base.txt") == nil {
		t.Error("Flatten: base.txt missing")
	}
	if merged.GetEntry("layer.txt") == nil {
		t.Error("Flatten: layer.txt missing")
	}
}

func TestC4FSOpen(t *testing.T) {
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Write test file
	content := []byte("Test file for Open")
	err := c4fs.WriteFile("test.txt", content, 0644)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open file
	f, err := c4fs.Open("test.txt")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	// Read via file interface
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Errorf("Open/Read content mismatch: got %q, want %q", data, content)
	}

	// Stat via file
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("File.Stat failed: %v", err)
	}

	if info.Size() != int64(len(content)) {
		t.Errorf("File.Stat size: got %d, want %d", info.Size(), len(content))
	}
}

func TestC4FSNonExistentFile(t *testing.T) {
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Try to read non-existent file
	_, err := c4fs.ReadFile("nonexistent.txt")
	if err == nil {
		t.Error("ReadFile should fail for non-existent file")
	}

	// Check that error is fs.ErrNotExist
	if !isPathErrorWithNotExist(err) {
		t.Errorf("Expected fs.ErrNotExist, got: %v", err)
	}
}

func TestC4FSCreate(t *testing.T) {
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Create file for writing
	f, err := c4fs.Create("created.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write content
	content := []byte("Created file content")
	_, err = f.Write(content)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Close (dehydrates to store)
	err = f.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file is readable
	data, err := c4fs.ReadFile("created.txt")
	if err != nil {
		t.Fatalf("ReadFile after Create failed: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Errorf("Create/Write content mismatch: got %q, want %q", data, content)
	}
}

func TestC4FSMkdir(t *testing.T) {
	store := NewMemoryStore()
	c4fs := New(nil, store)

	// Create directory
	err := c4fs.Mkdir("testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Stat directory
	info, err := c4fs.Stat("testdir")
	if err != nil {
		t.Fatalf("Stat testdir failed: %v", err)
	}

	if !info.IsDir() {
		t.Error("Stat IsDir: got false, want true")
	}

	// Try to create again (should fail)
	err = c4fs.Mkdir("testdir", 0755)
	if err == nil {
		t.Error("Mkdir should fail for existing directory")
	}
}

// Helper function to check if error is a PathError with ErrNotExist
func isPathErrorWithNotExist(err error) bool {
	if pathErr, ok := err.(*fs.PathError); ok {
		return pathErr.Err == fs.ErrNotExist
	}
	return false
}
