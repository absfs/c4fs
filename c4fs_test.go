package c4fs

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
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

	// Verify c4fs implements absfs.FileSystem interface
	var _ absfs.FileSystem = c4fs

	t.Log("FS implements absfs.FileSystem interface")
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

func TestC4FSMkdirAll(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create nested directory structure
	err := c4fs.MkdirAll("a/b/c/d", 0755)
	if err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Verify all directories were created
	for _, dir := range []string{"a", "a/b", "a/b/c", "a/b/c/d"} {
		if !c4fs.IsDir(dir) {
			t.Errorf("Directory %s should exist", dir)
		}
	}

	// MkdirAll on existing directory should succeed
	err = c4fs.MkdirAll("a/b/c", 0755)
	if err != nil {
		t.Errorf("MkdirAll on existing directory should succeed: %v", err)
	}

	// Create a file
	c4fs.WriteFile("file.txt", []byte("test"), 0644)

	// MkdirAll on existing file should fail
	err = c4fs.MkdirAll("file.txt", 0755)
	if err == nil {
		t.Error("MkdirAll on existing file should fail")
	}
}

func TestC4FSRemove(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create and remove a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)
	if !c4fs.Exists("test.txt") {
		t.Fatal("File should exist after creation")
	}

	err := c4fs.Remove("test.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if c4fs.Exists("test.txt") {
		t.Error("File should not exist after removal")
	}

	// Create and remove an empty directory
	c4fs.Mkdir("emptydir", 0755)
	err = c4fs.Remove("emptydir")
	if err != nil {
		t.Fatalf("Remove empty directory failed: %v", err)
	}

	if c4fs.Exists("emptydir") {
		t.Error("Directory should not exist after removal")
	}

	// Try to remove non-empty directory (should fail)
	c4fs.Mkdir("nonempty", 0755)
	c4fs.WriteFile("nonempty/file.txt", []byte("data"), 0644)

	err = c4fs.Remove("nonempty")
	if err == nil {
		t.Error("Remove should fail for non-empty directory")
	}

	// Remove non-existent file should fail
	err = c4fs.Remove("nonexistent.txt")
	if err == nil {
		t.Error("Remove should fail for non-existent file")
	}
}

func TestC4FSRemoveAll(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create nested structure
	c4fs.MkdirAll("dir/subdir1", 0755)
	c4fs.MkdirAll("dir/subdir2", 0755)
	c4fs.WriteFile("dir/file1.txt", []byte("data1"), 0644)
	c4fs.WriteFile("dir/subdir1/file2.txt", []byte("data2"), 0644)
	c4fs.WriteFile("dir/subdir2/file3.txt", []byte("data3"), 0644)

	// Remove entire tree
	err := c4fs.RemoveAll("dir")
	if err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify all removed
	if c4fs.Exists("dir") {
		t.Error("dir should not exist after RemoveAll")
	}
	if c4fs.Exists("dir/subdir1") {
		t.Error("dir/subdir1 should not exist after RemoveAll")
	}
	if c4fs.Exists("dir/file1.txt") {
		t.Error("dir/file1.txt should not exist after RemoveAll")
	}

	// RemoveAll on non-existent path should succeed
	err = c4fs.RemoveAll("nonexistent")
	if err != nil {
		t.Errorf("RemoveAll on non-existent path should succeed: %v", err)
	}

	// RemoveAll on file should work too
	c4fs.WriteFile("single.txt", []byte("data"), 0644)
	err = c4fs.RemoveAll("single.txt")
	if err != nil {
		t.Fatalf("RemoveAll on file failed: %v", err)
	}
	if c4fs.Exists("single.txt") {
		t.Error("single.txt should not exist after RemoveAll")
	}
}

func TestC4FSRename(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Test file rename
	content := []byte("test content")
	c4fs.WriteFile("old.txt", content, 0644)

	err := c4fs.Rename("old.txt", "new.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Old file should not exist
	if c4fs.Exists("old.txt") {
		t.Error("old.txt should not exist after rename")
	}

	// New file should exist with same content
	if !c4fs.Exists("new.txt") {
		t.Fatal("new.txt should exist after rename")
	}

	data, err := c4fs.ReadFile("new.txt")
	if err != nil {
		t.Fatalf("Reading renamed file failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Error("Renamed file content mismatch")
	}

	// Test directory rename
	c4fs.MkdirAll("olddir/subdir", 0755)
	c4fs.WriteFile("olddir/file.txt", []byte("data"), 0644)
	c4fs.WriteFile("olddir/subdir/file2.txt", []byte("data2"), 0644)

	err = c4fs.Rename("olddir", "newdir")
	if err != nil {
		t.Fatalf("Directory rename failed: %v", err)
	}

	// Old paths should not exist
	if c4fs.Exists("olddir") {
		t.Error("olddir should not exist after rename")
	}
	if c4fs.Exists("olddir/file.txt") {
		t.Error("olddir/file.txt should not exist after rename")
	}

	// New paths should exist
	if !c4fs.Exists("newdir") {
		t.Error("newdir should exist after rename")
	}
	if !c4fs.Exists("newdir/file.txt") {
		t.Error("newdir/file.txt should exist after rename")
	}
	if !c4fs.Exists("newdir/subdir/file2.txt") {
		t.Error("newdir/subdir/file2.txt should exist after rename")
	}

	// Verify content preserved
	data, _ = c4fs.ReadFile("newdir/file.txt")
	if string(data) != "data" {
		t.Error("Content not preserved in renamed directory")
	}

	// Rename to existing file should fail
	c4fs.WriteFile("existing.txt", []byte("exists"), 0644)
	err = c4fs.Rename("new.txt", "existing.txt")
	if err == nil {
		t.Error("Rename to existing file should fail")
	}

	// Rename non-existent should fail
	err = c4fs.Rename("nonexistent.txt", "whatever.txt")
	if err == nil {
		t.Error("Rename of non-existent file should fail")
	}
}

func TestC4FSRemoveFromLayer(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base with a file
	base := c4m.NewManifest()
	content := []byte("base content")
	id, _ := adapter.Put(bytes.NewReader(content))
	base.AddEntry(&c4m.Entry{
		Mode: 0644,
		Size: int64(len(content)),
		Name: "base.txt",
		C4ID: id,
	})

	// Create filesystem with base
	c4fs := New(base, adapter)

	// Verify file exists
	if !c4fs.Exists("base.txt") {
		t.Fatal("base.txt should exist")
	}

	// Remove file from base (adds tombstone to layer)
	err := c4fs.Remove("base.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// File should not be visible
	if c4fs.Exists("base.txt") {
		t.Error("base.txt should not exist after removal")
	}

	// Flatten should not include the removed file
	merged := c4fs.Flatten()
	if merged.GetEntry("base.txt") != nil {
		t.Error("Flattened manifest should not contain removed file")
	}
}

func TestC4FSSymlink(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	content := []byte("target file content")
	c4fs.WriteFile("target.txt", content, 0644)

	// Create symlink to file
	err := c4fs.Symlink("target.txt", "link.txt")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	// Lstat should show it as a symlink
	info, err := c4fs.Lstat("link.txt")
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		t.Error("Lstat should show file as symlink")
	}

	// Stat should follow the symlink
	info, err = c4fs.Stat("link.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		t.Error("Stat should follow symlink and not show symlink mode")
	}
	if info.Size() != int64(len(content)) {
		t.Errorf("Stat via symlink: size = %d, want %d", info.Size(), len(content))
	}

	// ReadLink should return target
	target, err := c4fs.ReadLink("link.txt")
	if err != nil {
		t.Fatalf("ReadLink failed: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("ReadLink: got %q, want %q", target, "target.txt")
	}

	// Reading through symlink should work
	data, err := c4fs.ReadFile("link.txt")
	if err != nil {
		t.Fatalf("ReadFile via symlink failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Error("Content read via symlink doesn't match")
	}

	// Symlink to existing file should fail
	err = c4fs.Symlink("target.txt", "link.txt")
	if err == nil {
		t.Error("Symlink to existing path should fail")
	}
}

func TestC4FSSymlinkToDirectory(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a directory with a file
	c4fs.Mkdir("dir", 0755)
	c4fs.WriteFile("dir/file.txt", []byte("content"), 0644)

	// Create symlink to directory
	err := c4fs.Symlink("dir", "dirlink")
	if err != nil {
		t.Fatalf("Symlink to directory failed: %v", err)
	}

	// Stat should show it as a directory (following symlink)
	info, err := c4fs.Stat("dirlink")
	if err != nil {
		t.Fatalf("Stat dirlink failed: %v", err)
	}
	if !info.IsDir() {
		t.Error("Stat should follow symlink to directory")
	}

	// Reading file through directory symlink
	data, err := c4fs.ReadFile("dirlink/file.txt")
	if err != nil {
		t.Fatalf("ReadFile via directory symlink failed: %v", err)
	}
	if string(data) != "content" {
		t.Error("Content via directory symlink doesn't match")
	}

	// Lstat on symlink itself
	info, err = c4fs.Lstat("dirlink")
	if err != nil {
		t.Fatalf("Lstat dirlink failed: %v", err)
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		t.Error("Lstat should show symlink mode")
	}
	if info.IsDir() {
		t.Error("Lstat on symlink should not show as directory")
	}
}

func TestC4FSSymlinkRelative(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory structure
	c4fs.MkdirAll("a/b", 0755)
	c4fs.WriteFile("a/b/file.txt", []byte("content"), 0644)
	c4fs.WriteFile("a/other.txt", []byte("other"), 0644)

	// Create relative symlink
	err := c4fs.Symlink("../other.txt", "a/b/link.txt")
	if err != nil {
		t.Fatalf("Relative symlink failed: %v", err)
	}

	// Read via relative symlink
	data, err := c4fs.ReadFile("a/b/link.txt")
	if err != nil {
		t.Fatalf("ReadFile via relative symlink failed: %v", err)
	}
	if string(data) != "other" {
		t.Errorf("Content via relative symlink: got %q, want %q", string(data), "other")
	}

	// Stat via relative symlink
	info, err := c4fs.Stat("a/b/link.txt")
	if err != nil {
		t.Fatalf("Stat via relative symlink failed: %v", err)
	}
	if info.Size() != 5 {
		t.Errorf("Size via relative symlink: got %d, want 5", info.Size())
	}
}

func TestC4FSSymlinkChain(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create file and chain of symlinks
	c4fs.WriteFile("file.txt", []byte("content"), 0644)
	c4fs.Symlink("file.txt", "link1")
	c4fs.Symlink("link1", "link2")
	c4fs.Symlink("link2", "link3")

	// Should be able to read through chain
	data, err := c4fs.ReadFile("link3")
	if err != nil {
		t.Fatalf("ReadFile via symlink chain failed: %v", err)
	}
	if string(data) != "content" {
		t.Error("Content via symlink chain doesn't match")
	}

	// Stat should work
	info, err := c4fs.Stat("link3")
	if err != nil {
		t.Fatalf("Stat via symlink chain failed: %v", err)
	}
	if info.Size() != 7 {
		t.Errorf("Size via symlink chain: got %d, want 7", info.Size())
	}

	// Each link should be identifiable via Lstat
	for _, link := range []string{"link1", "link2", "link3"} {
		info, err := c4fs.Lstat(link)
		if err != nil {
			t.Fatalf("Lstat %s failed: %v", link, err)
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			t.Errorf("%s should be a symlink", link)
		}
	}
}

func TestC4FSSymlinkBroken(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create symlink to non-existent file
	err := c4fs.Symlink("nonexistent.txt", "broken.txt")
	if err != nil {
		t.Fatalf("Creating broken symlink failed: %v", err)
	}

	// Lstat should work
	info, err := c4fs.Lstat("broken.txt")
	if err != nil {
		t.Fatalf("Lstat on broken symlink failed: %v", err)
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		t.Error("Should be a symlink")
	}

	// ReadLink should work
	target, err := c4fs.ReadLink("broken.txt")
	if err != nil {
		t.Fatalf("ReadLink on broken symlink failed: %v", err)
	}
	if target != "nonexistent.txt" {
		t.Errorf("ReadLink: got %q, want %q", target, "nonexistent.txt")
	}

	// But Stat and ReadFile should fail
	_, err = c4fs.Stat("broken.txt")
	if err == nil {
		t.Error("Stat on broken symlink should fail")
	}

	_, err = c4fs.ReadFile("broken.txt")
	if err == nil {
		t.Error("ReadFile on broken symlink should fail")
	}
}

func TestC4FSSymlinkLoop(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create symlink loop
	c4fs.Symlink("link2", "link1")
	c4fs.Symlink("link1", "link2")

	// Stat should detect loop
	_, err := c4fs.Stat("link1")
	if err == nil {
		t.Error("Stat should fail on symlink loop")
	}
	if !strings.Contains(err.Error(), "too many levels") {
		t.Errorf("Error should mention too many levels: %v", err)
	}

	// ReadFile should fail
	_, err = c4fs.ReadFile("link1")
	if err == nil {
		t.Error("ReadFile should fail on symlink loop")
	}
}

func TestC4FSSymlinkRemove(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create file and symlink
	c4fs.WriteFile("file.txt", []byte("content"), 0644)
	c4fs.Symlink("file.txt", "link.txt")

	// Remove symlink (not the target)
	err := c4fs.Remove("link.txt")
	if err != nil {
		t.Fatalf("Remove symlink failed: %v", err)
	}

	// Symlink should be gone
	if c4fs.Exists("link.txt") {
		t.Error("Symlink should not exist after removal")
	}

	// But target should still exist
	if !c4fs.Exists("file.txt") {
		t.Error("Target file should still exist")
	}

	data, err := c4fs.ReadFile("file.txt")
	if err != nil {
		t.Fatalf("Reading target after symlink removal failed: %v", err)
	}
	if string(data) != "content" {
		t.Error("Target content changed after symlink removal")
	}
}

func TestC4FSSymlinkRename(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create file and symlink
	c4fs.WriteFile("file.txt", []byte("content"), 0644)
	c4fs.Symlink("file.txt", "link1.txt")

	// Rename symlink
	err := c4fs.Rename("link1.txt", "link2.txt")
	if err != nil {
		t.Fatalf("Rename symlink failed: %v", err)
	}

	// Old symlink should be gone
	if c4fs.Exists("link1.txt") {
		t.Error("Old symlink should not exist")
	}

	// New symlink should exist and work
	target, err := c4fs.ReadLink("link2.txt")
	if err != nil {
		t.Fatalf("ReadLink after rename failed: %v", err)
	}
	if target != "file.txt" {
		t.Errorf("Target after rename: got %q, want %q", target, "file.txt")
	}

	// Should be able to read through renamed symlink
	data, err := c4fs.ReadFile("link2.txt")
	if err != nil {
		t.Fatalf("ReadFile via renamed symlink failed: %v", err)
	}
	if string(data) != "content" {
		t.Error("Content via renamed symlink doesn't match")
	}
}

func TestC4FSReadLinkOnNonSymlink(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create regular file
	c4fs.WriteFile("file.txt", []byte("content"), 0644)

	// ReadLink on non-symlink should fail
	_, err := c4fs.ReadLink("file.txt")
	if err == nil {
		t.Error("ReadLink on regular file should fail")
	}

	// ReadLink on directory should fail
	c4fs.Mkdir("dir", 0755)
	_, err = c4fs.ReadLink("dir")
	if err == nil {
		t.Error("ReadLink on directory should fail")
	}
}

// Tests for absfs.FileSystem interface methods

func TestC4FSSeparator(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	if sep := c4fs.Separator(); sep != '/' {
		t.Errorf("Separator: got %c, want /", sep)
	}
}

func TestC4FSListSeparator(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	if sep := c4fs.ListSeparator(); sep != ':' {
		t.Errorf("ListSeparator: got %c, want :", sep)
	}
}

func TestC4FSTempDir(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	if dir := c4fs.TempDir(); dir != "/tmp" {
		t.Errorf("TempDir: got %q, want /tmp", dir)
	}
}

func TestC4FSChdir(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Initial cwd should be "/"
	cwd, err := c4fs.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if cwd != "/" {
		t.Errorf("Initial cwd: got %q, want /", cwd)
	}

	// Create a directory
	if err := c4fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Chdir to it
	if err := c4fs.Chdir("testdir"); err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}

	// Verify cwd changed
	cwd, err = c4fs.Getwd()
	if err != nil {
		t.Fatalf("Getwd after Chdir failed: %v", err)
	}
	if cwd != "testdir" && cwd != "/testdir" {
		t.Errorf("Cwd after Chdir: got %q, want testdir or /testdir", cwd)
	}

	// Chdir to non-existent directory should fail
	if err := c4fs.Chdir("nonexistent"); err == nil {
		t.Error("Chdir to non-existent directory should fail")
	}

	// Create a file
	c4fs.WriteFile("file.txt", []byte("content"), 0644)

	// Chdir to file should fail
	if err := c4fs.Chdir("file.txt"); err == nil {
		t.Error("Chdir to file should fail")
	}

	// Chdir to root
	if err := c4fs.Chdir("/"); err != nil {
		t.Fatalf("Chdir to root failed: %v", err)
	}
	cwd, _ = c4fs.Getwd()
	if cwd != "/" {
		t.Errorf("Cwd after Chdir /: got %q, want /", cwd)
	}
}

func TestC4FSChdirRelativePaths(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create nested directories
	c4fs.MkdirAll("a/b/c", 0755)

	// Chdir to a
	c4fs.Chdir("a")

	// Create file using relative path
	c4fs.WriteFile("file.txt", []byte("content"), 0644)

	// File should be at a/file.txt (use absolute path to check)
	if !c4fs.Exists("/a/file.txt") {
		t.Error("File should exist at /a/file.txt")
	}

	// Read file using relative path (from cwd=a, file.txt means a/file.txt)
	data, err := c4fs.ReadFile("file.txt")
	if err != nil {
		t.Fatalf("ReadFile with relative path failed: %v", err)
	}
	if string(data) != "content" {
		t.Errorf("Content: got %q, want content", string(data))
	}

	// Chdir to b using relative path
	c4fs.Chdir("b")

	// Verify cwd
	cwd, _ := c4fs.Getwd()
	if cwd != "a/b" && cwd != "/a/b" {
		t.Errorf("Cwd: got %q, want a/b", cwd)
	}
}

func TestC4FSTruncate(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	content := []byte("Hello, World!")
	c4fs.WriteFile("test.txt", content, 0644)

	// Truncate to smaller size
	err := c4fs.Truncate("test.txt", 5)
	if err != nil {
		t.Fatalf("Truncate to smaller size failed: %v", err)
	}

	data, _ := c4fs.ReadFile("test.txt")
	if string(data) != "Hello" {
		t.Errorf("After truncate to 5: got %q, want Hello", string(data))
	}

	// Truncate to zero
	err = c4fs.Truncate("test.txt", 0)
	if err != nil {
		t.Fatalf("Truncate to zero failed: %v", err)
	}

	data, _ = c4fs.ReadFile("test.txt")
	if len(data) != 0 {
		t.Errorf("After truncate to 0: got length %d, want 0", len(data))
	}

	// Write new content and truncate to larger size
	c4fs.WriteFile("test.txt", []byte("Hi"), 0644)
	err = c4fs.Truncate("test.txt", 10)
	if err != nil {
		t.Fatalf("Truncate to larger size failed: %v", err)
	}

	data, _ = c4fs.ReadFile("test.txt")
	if len(data) != 10 {
		t.Errorf("After truncate to 10: got length %d, want 10", len(data))
	}
	if string(data[:2]) != "Hi" {
		t.Errorf("After truncate to 10: first 2 bytes should be Hi, got %q", string(data[:2]))
	}
	// Extended bytes should be zeros
	for i := 2; i < 10; i++ {
		if data[i] != 0 {
			t.Errorf("Extended byte %d should be zero, got %d", i, data[i])
		}
	}

	// Truncate non-existent file should fail
	err = c4fs.Truncate("nonexistent.txt", 5)
	if err == nil {
		t.Error("Truncate non-existent file should fail")
	}

	// Truncate directory should fail
	c4fs.Mkdir("dir", 0755)
	err = c4fs.Truncate("dir", 5)
	if err == nil {
		t.Error("Truncate directory should fail")
	}
}

func TestC4FSChown(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	// Chown should succeed (no-op)
	err := c4fs.Chown("test.txt", 1000, 1000)
	if err != nil {
		t.Errorf("Chown failed: %v", err)
	}

	// Chown on directory
	c4fs.Mkdir("dir", 0755)
	err = c4fs.Chown("dir", 1000, 1000)
	if err != nil {
		t.Errorf("Chown on directory failed: %v", err)
	}

	// Chown on non-existent file should fail
	err = c4fs.Chown("nonexistent.txt", 1000, 1000)
	if err == nil {
		t.Error("Chown on non-existent file should fail")
	}
}

func TestC4FSOpenFile(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// O_RDONLY on non-existent file should fail
	_, err := c4fs.OpenFile("nonexistent.txt", os.O_RDONLY, 0644)
	if err == nil {
		t.Error("OpenFile O_RDONLY on non-existent should fail")
	}

	// Create a file
	c4fs.WriteFile("test.txt", []byte("hello"), 0644)

	// O_RDONLY should work
	f, err := c4fs.OpenFile("test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile O_RDONLY failed: %v", err)
	}
	data := make([]byte, 5)
	n, _ := f.Read(data)
	f.Close()
	if string(data[:n]) != "hello" {
		t.Errorf("Read via OpenFile: got %q, want hello", string(data[:n]))
	}

	// O_WRONLY on non-existent without O_CREATE should fail
	_, err = c4fs.OpenFile("new.txt", os.O_WRONLY, 0644)
	if err == nil {
		t.Error("OpenFile O_WRONLY without O_CREATE on non-existent should fail")
	}

	// O_WRONLY|O_CREATE should work
	f, err = c4fs.OpenFile("new.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("OpenFile O_WRONLY|O_CREATE failed: %v", err)
	}
	f.Write([]byte("world"))
	f.Close()

	data2, _ := c4fs.ReadFile("new.txt")
	if string(data2) != "world" {
		t.Errorf("After O_WRONLY|O_CREATE: got %q, want world", string(data2))
	}

	// O_CREATE|O_EXCL on existing file should fail
	_, err = c4fs.OpenFile("test.txt", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err == nil {
		t.Error("OpenFile O_CREATE|O_EXCL on existing should fail")
	}

	// O_WRONLY on directory should fail
	c4fs.Mkdir("dir", 0755)
	_, err = c4fs.OpenFile("dir", os.O_WRONLY, 0644)
	if err == nil {
		t.Error("OpenFile O_WRONLY on directory should fail")
	}
}

func TestC4FSOpenFileAppend(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create initial file
	c4fs.WriteFile("test.txt", []byte("hello"), 0644)

	// Open with O_APPEND
	f, err := c4fs.OpenFile("test.txt", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("OpenFile O_APPEND failed: %v", err)
	}
	f.Write([]byte(" world"))
	f.Close()

	data, _ := c4fs.ReadFile("test.txt")
	if string(data) != "hello world" {
		t.Errorf("After O_APPEND: got %q, want 'hello world'", string(data))
	}
}

func TestC4FSOpenFileDirectory(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with files
	c4fs.MkdirAll("testdir", 0755)
	c4fs.WriteFile("testdir/file1.txt", []byte("a"), 0644)
	c4fs.WriteFile("testdir/file2.txt", []byte("b"), 0644)

	// Open directory for reading
	f, err := c4fs.OpenFile("testdir", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile on directory failed: %v", err)
	}

	// Should be able to get stat
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat on directory file failed: %v", err)
	}
	if !info.IsDir() {
		t.Error("Directory should have IsDir() = true")
	}

	// Should implement fs.ReadDirFile
	if rdf, ok := f.(fs.ReadDirFile); ok {
		entries, err := rdf.ReadDir(-1)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}
		if len(entries) != 2 {
			t.Errorf("ReadDir: got %d entries, want 2", len(entries))
		}
	} else {
		t.Error("Directory file should implement fs.ReadDirFile")
	}

	f.Close()
}

func TestC4FSFileSeek(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create file
	c4fs.WriteFile("test.txt", []byte("0123456789"), 0644)

	// Open for reading
	f, _ := c4fs.OpenFile("test.txt", os.O_RDONLY, 0)
	defer f.Close()

	// Seek to position 5
	pos, err := f.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 5 {
		t.Errorf("Seek position: got %d, want 5", pos)
	}

	// Read from position 5
	data := make([]byte, 3)
	f.Read(data)
	if string(data) != "567" {
		t.Errorf("Read after seek: got %q, want 567", string(data))
	}

	// Seek relative to current
	pos, _ = f.Seek(-2, io.SeekCurrent)
	if pos != 6 {
		t.Errorf("Seek relative: got %d, want 6", pos)
	}

	// Seek relative to end
	pos, _ = f.Seek(-3, io.SeekEnd)
	if pos != 7 {
		t.Errorf("Seek from end: got %d, want 7", pos)
	}
}

func TestC4FSFileReadAt(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create file
	c4fs.WriteFile("test.txt", []byte("0123456789"), 0644)

	// Open for reading
	f, _ := c4fs.OpenFile("test.txt", os.O_RDONLY, 0)
	defer f.Close()

	// ReadAt
	data := make([]byte, 3)
	n, err := f.ReadAt(data, 5)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 3 {
		t.Errorf("ReadAt n: got %d, want 3", n)
	}
	if string(data) != "567" {
		t.Errorf("ReadAt data: got %q, want 567", string(data))
	}

	// ReadAt past end should return io.EOF
	_, err = f.ReadAt(data, 10)
	if err != io.EOF {
		t.Errorf("ReadAt past end: got %v, want io.EOF", err)
	}
}

func TestC4FSNewWithLayer(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base with a file
	base := c4m.NewManifest()
	content := []byte("base content")
	id, _ := adapter.Put(bytes.NewReader(content))
	base.AddEntry(&c4m.Entry{
		Mode: 0644,
		Size: int64(len(content)),
		Name: "base.txt",
		C4ID: id,
	})

	// Create layer with a file
	layer := c4m.NewManifest()
	layerContent := []byte("layer content")
	layerId, _ := adapter.Put(bytes.NewReader(layerContent))
	layer.AddEntry(&c4m.Entry{
		Mode: 0644,
		Size: int64(len(layerContent)),
		Name: "layer.txt",
		C4ID: layerId,
	})

	// Create filesystem with both
	c4fs := NewWithLayer(base, layer, adapter)

	// Both files should exist
	if !c4fs.Exists("base.txt") {
		t.Error("base.txt should exist")
	}
	if !c4fs.Exists("layer.txt") {
		t.Error("layer.txt should exist")
	}

	// Read base file
	data, err := c4fs.ReadFile("base.txt")
	if err != nil {
		t.Fatalf("ReadFile base.txt failed: %v", err)
	}
	if string(data) != "base content" {
		t.Errorf("base.txt content: got %q, want 'base content'", string(data))
	}

	// Read layer file
	data, err = c4fs.ReadFile("layer.txt")
	if err != nil {
		t.Fatalf("ReadFile layer.txt failed: %v", err)
	}
	if string(data) != "layer content" {
		t.Errorf("layer.txt content: got %q, want 'layer content'", string(data))
	}
}

func TestC4FSBaseLayerStore(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Write a file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	// Base should return a copy
	base := c4fs.Base()
	if base == nil {
		t.Error("Base should not be nil")
	}

	// Layer should contain the new file
	layer := c4fs.Layer()
	if layer == nil {
		t.Error("Layer should not be nil")
	}
	if layer.GetEntry("test.txt") == nil {
		t.Error("Layer should contain test.txt")
	}

	// Store should be accessible
	store := c4fs.Store()
	if store == nil {
		t.Error("Store should not be nil")
	}
}

func TestC4FSSubFS(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory structure
	c4fs.MkdirAll("docs/api", 0755)
	c4fs.WriteFile("docs/readme.txt", []byte("readme"), 0644)
	c4fs.WriteFile("docs/api/endpoints.txt", []byte("endpoints"), 0644)

	// Get sub filesystem
	sub, err := c4fs.Sub("docs")
	if err != nil {
		t.Fatalf("Sub failed: %v", err)
	}

	// Open file through sub fs
	f, err := sub.Open("readme.txt")
	if err != nil {
		t.Fatalf("Open via sub failed: %v", err)
	}
	defer f.Close()

	// Read content
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("Read via sub failed: %v", err)
	}
	if string(data) != "readme" {
		t.Errorf("Content via sub: got %q, want readme", string(data))
	}

	// Sub of non-directory should fail
	_, err = c4fs.Sub("docs/readme.txt")
	if err == nil {
		t.Error("Sub of file should fail")
	}
}

func TestC4FSDehydratingFileOperations(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create a file
	f, err := c4fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Name should return the file name
	if name := f.Name(); name != "test.txt" {
		t.Errorf("Name: got %q, want test.txt", name)
	}

	// WriteString
	n, err := f.WriteString("hello")
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	if n != 5 {
		t.Errorf("WriteString n: got %d, want 5", n)
	}

	// Stat should work before close
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 5 {
		t.Errorf("Stat size: got %d, want 5", info.Size())
	}

	// Read on write file should fail
	buf := make([]byte, 5)
	_, err = f.Read(buf)
	if err == nil {
		t.Error("Read on write file should fail")
	}

	// ReadAt on write file should fail
	_, err = f.ReadAt(buf, 0)
	if err == nil {
		t.Error("ReadAt on write file should fail")
	}

	// Readdir on file should fail
	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("Readdir on file should fail")
	}

	// Readdirnames on file should fail
	_, err = f.Readdirnames(-1)
	if err == nil {
		t.Error("Readdirnames on file should fail")
	}

	// Sync should succeed
	if err := f.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	// Close
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify content
	data, _ := c4fs.ReadFile("test.txt")
	if string(data) != "hello" {
		t.Errorf("Content: got %q, want hello", string(data))
	}
}

func TestC4FSReadOnlyFileErrorPaths(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create and open file
	c4fs.WriteFile("test.txt", []byte("content"), 0644)
	f, _ := c4fs.OpenFile("test.txt", os.O_RDONLY, 0)
	defer f.Close()

	// Write should fail
	_, err := f.Write([]byte("data"))
	if err == nil {
		t.Error("Write on read-only file should fail")
	}

	// WriteAt should fail
	_, err = f.WriteAt([]byte("data"), 0)
	if err == nil {
		t.Error("WriteAt on read-only file should fail")
	}

	// WriteString should fail
	_, err = f.WriteString("data")
	if err == nil {
		t.Error("WriteString on read-only file should fail")
	}

	// Truncate should fail
	err = f.Truncate(0)
	if err == nil {
		t.Error("Truncate on read-only file should fail")
	}

	// Readdir should fail (not a directory)
	_, err = f.Readdir(-1)
	if err == nil {
		t.Error("Readdir on file should fail")
	}

	// Readdirnames should fail (not a directory)
	_, err = f.Readdirnames(-1)
	if err == nil {
		t.Error("Readdirnames on file should fail")
	}
}

func TestC4FSDirFileOperations(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with files
	c4fs.MkdirAll("testdir", 0755)
	c4fs.WriteFile("testdir/file1.txt", []byte("a"), 0644)
	c4fs.WriteFile("testdir/file2.txt", []byte("b"), 0644)

	// Open directory
	f, _ := c4fs.OpenFile("testdir", os.O_RDONLY, 0)
	defer f.Close()

	// Read should fail
	buf := make([]byte, 10)
	_, err := f.Read(buf)
	if err == nil {
		t.Error("Read on directory should fail")
	}

	// ReadAt should fail
	_, err = f.ReadAt(buf, 0)
	if err == nil {
		t.Error("ReadAt on directory should fail")
	}

	// Seek should fail
	_, err = f.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("Seek on directory should fail")
	}

	// Write should fail
	_, err = f.Write([]byte("data"))
	if err == nil {
		t.Error("Write on directory should fail")
	}

	// WriteAt should fail
	_, err = f.WriteAt([]byte("data"), 0)
	if err == nil {
		t.Error("WriteAt on directory should fail")
	}

	// WriteString should fail
	_, err = f.WriteString("data")
	if err == nil {
		t.Error("WriteString on directory should fail")
	}

	// Truncate should fail
	err = f.Truncate(0)
	if err == nil {
		t.Error("Truncate on directory should fail")
	}

	// Sync should succeed
	if err := f.Sync(); err != nil {
		t.Errorf("Sync on directory failed: %v", err)
	}

	// Readdir with n=1
	entries, err := f.Readdir(1)
	if err != nil {
		t.Fatalf("Readdir(1) failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Readdir(1): got %d entries, want 1", len(entries))
	}

	// Readdir with n=0 (get rest)
	entries, err = f.Readdir(0)
	if err != nil {
		t.Fatalf("Readdir(0) failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Readdir(0): got %d entries, want 1", len(entries))
	}

	// Readdir should return EOF
	_, err = f.Readdir(1)
	if err != io.EOF {
		t.Errorf("Readdir after exhaustion: got %v, want io.EOF", err)
	}
}

func TestC4FSGlobPatterns(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create files
	c4fs.WriteFile("file1.txt", []byte("a"), 0644)
	c4fs.WriteFile("file2.txt", []byte("b"), 0644)
	c4fs.WriteFile("data.json", []byte("{}"), 0644)
	c4fs.MkdirAll("dir", 0755)
	c4fs.WriteFile("dir/nested.txt", []byte("c"), 0644)

	// Glob for txt files
	matches, err := c4fs.Glob("*.txt")
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(matches) != 2 {
		t.Errorf("Glob *.txt: got %d matches, want 2", len(matches))
	}

	// Glob for json files
	matches, err = c4fs.Glob("*.json")
	if err != nil {
		t.Fatalf("Glob json failed: %v", err)
	}
	if len(matches) != 1 {
		t.Errorf("Glob *.json: got %d matches, want 1", len(matches))
	}
}

func TestC4FSDirFileReaddirnames(t *testing.T) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with files
	c4fs.MkdirAll("testdir", 0755)
	c4fs.WriteFile("testdir/file1.txt", []byte("a"), 0644)
	c4fs.WriteFile("testdir/file2.txt", []byte("b"), 0644)

	// Open directory
	f, _ := c4fs.OpenFile("testdir", os.O_RDONLY, 0)
	defer f.Close()

	// Readdirnames with n=1
	names, err := f.Readdirnames(1)
	if err != nil {
		t.Fatalf("Readdirnames(1) failed: %v", err)
	}
	if len(names) != 1 {
		t.Errorf("Readdirnames(1): got %d names, want 1", len(names))
	}

	// Readdirnames with n=0 (get rest)
	names, err = f.Readdirnames(0)
	if err != nil {
		t.Fatalf("Readdirnames(0) failed: %v", err)
	}
	if len(names) != 1 {
		t.Errorf("Readdirnames(0): got %d names, want 1", len(names))
	}

	// Should return EOF
	_, err = f.Readdirnames(1)
	if err != io.EOF {
		t.Errorf("Readdirnames after exhaustion: got %v, want io.EOF", err)
	}
}
