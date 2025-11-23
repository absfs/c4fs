package c4fs

import (
	"io/fs"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// TestRootDirectoryEdgeCases tests various edge cases for the root directory.
func TestRootDirectoryEdgeCases(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	base := c4m.NewManifest()
	c4fs := New(base, adapter)

	// Add some files to the root
	err := c4fs.WriteFile("file1.txt", []byte("content1"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	err = c4fs.WriteFile("file2.txt", []byte("content2"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file2: %v", err)
	}

	err = c4fs.Mkdir("subdir", 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Test 1: Stat("/") should work
	info, err := c4fs.Stat("/")
	if err != nil {
		t.Errorf("Stat('/') failed: %v", err)
	} else if info != nil && !info.IsDir() {
		t.Errorf("Stat('/') should return a directory, got mode: %v", info.Mode())
	}

	// Test 2: Stat(".") should work
	info, err = c4fs.Stat(".")
	if err != nil {
		t.Errorf("Stat('.') failed: %v", err)
	} else if info != nil && !info.IsDir() {
		t.Errorf("Stat('.') should return a directory, got mode: %v", info.Mode())
	}

	// Test 3: Open("/") should open root directory
	f, err := c4fs.Open("/")
	if err != nil {
		t.Errorf("Open('/') failed: %v", err)
	} else {
		defer f.Close()
		// Try to read it as a directory
		if dirFile, ok := f.(fs.ReadDirFile); ok {
			entries, err := dirFile.ReadDir(-1)
			if err != nil {
				t.Errorf("ReadDir on root failed: %v", err)
			}
			if len(entries) != 3 {
				t.Errorf("Expected 3 entries in root, got %d", len(entries))
			}
		} else {
			t.Error("Open('/') should return a ReadDirFile")
		}
	}

	// Test 4: ReadDir("/") should list root entries
	entries, err := c4fs.ReadDir("/")
	if err != nil {
		t.Errorf("ReadDir('/') failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries in root, got %d", len(entries))
	}

	// Test 5: ReadDir(".") should list root entries
	entries, err = c4fs.ReadDir(".")
	if err != nil {
		t.Errorf("ReadDir('.') failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries in root, got %d", len(entries))
	}

	// Test 6: Remove("/") should fail
	err = c4fs.Remove("/")
	if err == nil {
		t.Error("Remove('/') should fail, but succeeded")
	}

	// Test 7: Remove(".") should fail
	err = c4fs.Remove(".")
	if err == nil {
		t.Error("Remove('.') should fail, but succeeded")
	}

	// Test 8: Mkdir("/") should fail (already exists)
	err = c4fs.Mkdir("/", 0755)
	if err == nil {
		t.Error("Mkdir('/') should fail, but succeeded")
	}

	// Test 9: Mkdir(".") should fail
	err = c4fs.Mkdir(".", 0755)
	if err == nil {
		t.Error("Mkdir('.') should fail, but succeeded")
	}

	// Test 10: Rename("/", "newname") should fail
	err = c4fs.Rename("/", "newname")
	if err == nil {
		t.Error("Rename('/') should fail, but succeeded")
	}

	// Test 11: Rename(".", "newname") should fail
	err = c4fs.Rename(".", "newname")
	if err == nil {
		t.Error("Rename('.') should fail, but succeeded")
	}

	// Test 12: Rename("file1.txt", "/") should fail
	err = c4fs.Rename("file1.txt", "/")
	if err == nil {
		t.Error("Rename to '/' should fail, but succeeded")
	}
}

// TestRootDirectoryWithEmptyFilesystem tests root operations on empty filesystem.
func TestRootDirectoryWithEmptyFilesystem(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	base := c4m.NewManifest()
	c4fs := New(base, adapter)

	// Test 1: Stat("/") on empty filesystem should work
	info, err := c4fs.Stat("/")
	if err != nil {
		t.Errorf("Stat('/') on empty filesystem failed: %v", err)
	} else if info != nil && !info.IsDir() {
		t.Errorf("Stat('/') should return a directory even when empty")
	}

	// Test 2: ReadDir("/") on empty filesystem should return empty list
	entries, err := c4fs.ReadDir("/")
	if err != nil {
		t.Errorf("ReadDir('/') on empty filesystem failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries in empty root, got %d", len(entries))
	}

	// Test 3: Open("/") on empty filesystem should work
	f, err := c4fs.Open("/")
	if err != nil {
		t.Errorf("Open('/') on empty filesystem failed: %v", err)
	} else {
		f.Close()
	}
}
