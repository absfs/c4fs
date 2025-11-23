package c4fs

import (
	"bytes"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

func TestStoreAdapter(t *testing.T) {
	ramStore := store.NewRAM()
	adapter := NewStoreAdapter(ramStore)

	// Test Put
	content := []byte("Hello, C4FS!")
	id, err := adapter.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Has
	if !adapter.Has(id) {
		t.Error("Has returned false for existing content")
	}

	// Test Get
	rc, err := adapter.Get(id)
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
	if err := adapter.Delete(id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if adapter.Has(id) {
		t.Error("Has returned true after deletion")
	}
}

func TestC4FSBasicOperations(t *testing.T) {
	// Create filesystem with RAM store
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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

	// Note: We can't easily check deduplication at the store level with the adapter,
	// but we can verify both files are readable with the same content

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
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base manifest with a file
	base := c4m.NewManifest()
	baseContent := []byte("Base file content")
	baseID, _ := adapter.Put(bytes.NewReader(baseContent))

	base.AddEntry(&c4m.Entry{
		Mode: 0644,
		Size: int64(len(baseContent)),
		Name: "base.txt",
		C4ID: baseID,
	})

	// Create filesystem with base
	c4fs := New(base, adapter)

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
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

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

func TestC4FSGlob(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create test files
	c4fs.WriteFile("test1.txt", []byte("test"), 0644)
	c4fs.WriteFile("test2.txt", []byte("test"), 0644)
	c4fs.WriteFile("data.json", []byte("{}"), 0644)
	c4fs.Mkdir("subdir", 0755)

	// Test Glob pattern
	matches, err := c4fs.Glob("*.txt")
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}

	if len(matches) != 2 {
		t.Errorf("Glob *.txt: got %d matches, want 2", len(matches))
	}

	// Test exact match
	matches, err = c4fs.Glob("data.json")
	if err != nil {
		t.Fatalf("Glob exact match failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Glob data.json: got %d matches, want 1", len(matches))
	}
}

func TestC4FSSub(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory structure
	c4fs.Mkdir("docs", 0755)
	c4fs.WriteFile("docs/readme.md", []byte("Documentation"), 0644)
	c4fs.WriteFile("docs/guide.md", []byte("Guide"), 0644)
	c4fs.WriteFile("config.json", []byte("{}"), 0644)

	// Get subdirectory
	subfs, err := c4fs.Sub("docs")
	if err != nil {
		t.Fatalf("Sub failed: %v", err)
	}

	// Read file from subfs
	data, err := fs.ReadFile(subfs, "readme.md")
	if err != nil {
		t.Fatalf("ReadFile from subfs failed: %v", err)
	}

	if string(data) != "Documentation" {
		t.Errorf("SubFS ReadFile: got %q, want %q", string(data), "Documentation")
	}

	// Verify config.json is not accessible from subfs
	_, err = fs.ReadFile(subfs, "config.json")
	if err == nil {
		t.Error("SubFS should not access parent files")
	}

	// List directory in subfs
	entries, err := fs.ReadDir(subfs, ".")
	if err != nil {
		t.Fatalf("ReadDir from subfs failed: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("SubFS ReadDir: got %d entries, want 2", len(entries))
	}
}

func TestC4FSReadDirFile(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with files
	c4fs.Mkdir("testdir", 0755)
	c4fs.WriteFile("testdir/file1.txt", []byte("1"), 0644)
	c4fs.WriteFile("testdir/file2.txt", []byte("2"), 0644)
	c4fs.WriteFile("testdir/file3.txt", []byte("3"), 0644)

	// Open directory as file
	f, err := c4fs.Open("testdir")
	if err != nil {
		t.Fatalf("Open directory failed: %v", err)
	}
	defer f.Close()

	// Check if it implements ReadDirFile
	dirFile, ok := f.(fs.ReadDirFile)
	if !ok {
		t.Fatal("Directory file does not implement fs.ReadDirFile")
	}

	// Read entries one at a time
	entry1, err := dirFile.ReadDir(1)
	if err != nil {
		t.Fatalf("ReadDir(1) failed: %v", err)
	}
	if len(entry1) != 1 {
		t.Errorf("ReadDir(1): got %d entries, want 1", len(entry1))
	}

	// Read remaining entries
	remaining, err := dirFile.ReadDir(-1)
	if err != nil {
		t.Fatalf("ReadDir(-1) failed: %v", err)
	}
	if len(remaining) != 2 {
		t.Errorf("ReadDir(-1): got %d entries, want 2", len(remaining))
	}

	// Further reads should return EOF
	_, err = dirFile.ReadDir(1)
	if err != io.EOF {
		t.Errorf("ReadDir after exhaustion: got %v, want io.EOF", err)
	}
}

func TestC4FSInterfaceCompliance(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Verify c4fs implements standard interfaces
	var _ fs.FS = c4fs
	var _ fs.ReadDirFS = c4fs
	var _ fs.ReadFileFS = c4fs
	var _ fs.StatFS = c4fs
	var _ fs.GlobFS = c4fs
	var _ fs.SubFS = c4fs

	t.Log("FS implements all standard fs interfaces")
}

func TestC4FSUtilityMethods(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Test Exists with non-existent file
	if c4fs.Exists("nonexistent.txt") {
		t.Error("Exists should return false for non-existent file")
	}

	// Create a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	// Test Exists
	if !c4fs.Exists("test.txt") {
		t.Error("Exists should return true for existing file")
	}

	// Test IsFile
	if !c4fs.IsFile("test.txt") {
		t.Error("IsFile should return true for regular file")
	}

	// Test IsDir
	if c4fs.IsDir("test.txt") {
		t.Error("IsDir should return false for regular file")
	}

	// Test Size
	size, err := c4fs.Size("test.txt")
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 7 {
		t.Errorf("Size: got %d, want 7", size)
	}

	// Create a directory
	c4fs.Mkdir("testdir", 0755)

	// Test IsDir on directory
	if !c4fs.IsDir("testdir") {
		t.Error("IsDir should return true for directory")
	}

	// Test IsFile on directory
	if c4fs.IsFile("testdir") {
		t.Error("IsFile should return false for directory")
	}
}

func TestC4FSChmod(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	// Check initial mode
	info, _ := c4fs.Stat("test.txt")
	if info.Mode().Perm() != 0644 {
		t.Errorf("Initial mode: got %o, want 0644", info.Mode().Perm())
	}

	// Change mode
	err := c4fs.Chmod("test.txt", 0600)
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	// Verify new mode
	info, _ = c4fs.Stat("test.txt")
	if info.Mode().Perm() != 0600 {
		t.Errorf("After chmod: got %o, want 0600", info.Mode().Perm())
	}
}

func TestC4FSChtimes(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	// Get initial time
	info1, _ := c4fs.Stat("test.txt")
	initialTime := info1.ModTime()

	// Change time
	newTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	err := c4fs.Chtimes("test.txt", newTime, newTime)
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	// Verify new time
	info2, _ := c4fs.Stat("test.txt")
	if !info2.ModTime().Equal(newTime) {
		t.Errorf("After chtimes: got %v, want %v", info2.ModTime(), newTime)
	}

	// Verify time changed
	if info2.ModTime().Equal(initialTime) {
		t.Error("ModTime should have changed")
	}
}
