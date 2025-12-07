# c4fs - Content-Addressable Filesystem

[![Go Reference](https://pkg.go.dev/badge/github.com/absfs/c4fs.svg)](https://pkg.go.dev/github.com/absfs/c4fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/absfs/c4fs)](https://goreportcard.com/report/github.com/absfs/c4fs)
[![CI](https://github.com/absfs/c4fs/actions/workflows/ci.yml/badge.svg)](https://github.com/absfs/c4fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A content-addressable filesystem for the absfs ecosystem using C4 IDs (SMPTE ST 2114:2017) with transparent hydration/dehydration, snapshots, and efficient sync.

## Overview

c4fs is a C4-based content-addressable filesystem that provides:

- **Content Addressing:** Uses C4 IDs for cryptographically verifiable content identification
- **C4M Format:** Filesystem metadata stored in C4 Manifest (C4M) format
- **Transparent Operations:** Automatic hydration/dehydration of file content
- **Copy-on-Write:** Immutable base layer + mutable overlay architecture
- **Deduplication:** Same content stored once regardless of filename
- **Instant Snapshots:** Manifest is the snapshot
- **Efficient Sync:** Transfer manifest, fetch only missing C4 IDs

## Core Concepts

### Dehydration (Write)
```
File content → C4 Store → Get C4 ID → Store in C4M
```

When writing files, content is stored in the C4 Store and the resulting C4 ID is recorded in the C4M manifest.

### Hydration (Read)
```
C4M entry → Lookup C4 ID → C4 Store.Get() → File content
```

When reading files, the C4 ID is looked up in the manifest and the content is retrieved from the C4 Store.

### Metadata Preservation

Files appear with their original size and attributes, not the 90-byte C4 ID size. The C4M manifest preserves:
- File size
- Unix permissions
- Timestamps (modified, accessed, created)
- File type (regular file, directory, symlink)

### Copy-on-Write Architecture

```
Immutable base C4M (snapshot/version)
         +
Mutable overlay C4M (layer)
         ↓
   Combined view
```

Reads check layer first, then fall back to base. Writes always go to the layer. The `Flatten()` operation merges base + layer into a new manifest.

## Architecture

```
absfs.FileSystem Interface
        ↓
    c4fs.FS (base + layer manifests)
        ↓
C4 Store (content by C4 ID)
        ↓
Backend (osfs, s3fs, memfs, etc.)
```

## Key Features

- **Automatic Content Deduplication:** Same content = same C4 ID = stored once
- **Instant Snapshots:** Manifest is the snapshot - no data copying required
- **Efficient Sync:** Transfer manifest, fetch missing C4 IDs only
- **Cryptographic Verification:** C4 IDs are SHA-512 based
- **Versioning:** Maintain manifest history for time-travel
- **Copy-on-Write Efficiency:** Minimal storage overhead for snapshots
- **Backend Agnostic:** Works with any c4/store backend (RAM, Local, S3, etc.)
- **Full Symlink Support:** Unix-style symbolic links with relative/absolute paths
- **High Performance:** O(1) path lookups with hash map indexing (up to 21x faster)
- **Standard Library Compliance:** Full io/fs interface implementation
- **Garbage Collection Ready:** Track referenced C4 IDs for orphaned content cleanup
- **Thread-Safe:** Concurrent operations protected with read/write locks

## Components

### 1. C4 Store Interface

```go
type Store interface {
    // Put stores content and returns its C4 ID
    Put(io.Reader) (c4.ID, error)

    // Get retrieves content by C4 ID
    Get(c4.ID) (io.ReadCloser, error)

    // Has checks if content exists
    Has(c4.ID) bool

    // Delete removes content (with refcounting)
    Delete(c4.ID) error
}
```

### 2. Store Implementations

#### LocalStore
Disk-based storage with directory hierarchy:
```
/var/c4/[first-2]/[next-2]/[full-c4-id]
```

Example:
```
/var/c4/c4/1a/c41a2b3c...rest-of-id
```

#### MemoryStore
In-memory map for testing and temporary storage.

#### S3Store
S3 bucket with C4 IDs as object keys.

#### CachedStore
Local cache + remote backend for performance.

### 3. Filesystem (c4fs.FS)

```go
type FS struct {
    base  *c4m.Manifest  // Immutable base (snapshot)
    layer *c4m.Manifest  // Mutable overlay (starts empty)
    store Store          // Content storage
}
```

**Operations:**
- Base manifest: Immutable, read-only
- Layer manifest: Mutable overlay, starts empty
- Copy-on-write: Reads check layer → base, writes go to layer
- `Flatten()`: Merge base + layer into new manifest

### 4. File Operations

#### Read
1. Lookup entry in manifest (layer first, then base)
2. Get C4 ID from entry
3. Hydrate from C4 Store
4. Return content with metadata from manifest

#### Write
1. Buffer content in memory or temp file
2. On close, dehydrate to C4 Store
3. Get C4 ID
4. Update layer manifest with entry

#### Metadata
All metadata preserved from manifest:
- Size: Original file size, not C4 ID size
- Mode: Unix permissions
- Timestamps: Modified, accessed, created times

## Implementation Phases

### Phase 1: Core Store
- Store interface definition
- LocalStore implementation
- MemoryStore implementation
- Reference counting for garbage collection

### Phase 2: C4M Integration
- Parse C4M manifests (github.com/Avalanche-io/c4/c4m)
- Generate C4M manifests
- Merge base + layer manifests
- Diff manifests

### Phase 3: Basic Filesystem
- Read operations (Open, Stat, ReadDir)
- Hydration: C4M entry → Store.Get()
- absfs.FileSystem interface (read-only first)

### Phase 4: Write Operations
- Dehydrating file wrapper
- Write buffering
- Close() triggers dehydration
- Layer manifest updates

### Phase 5: Copy-on-Write
- Base + layer architecture
- Overlay semantics
- Flatten() operation
- Snapshot management

### Phase 6: Advanced Features
- Garbage collection (remove unreferenced C4 IDs)
- Efficient sync protocol
- S3Store implementation
- CachedStore implementation

## Implementation Status

### ✅ Completed Features

- **Core Filesystem Operations**: Open, Stat, ReadDir, ReadFile, WriteFile, Create
- **Directory Operations**: Mkdir, MkdirAll, Remove, RemoveAll, Rename
- **Copy-on-Write**: Base + layer architecture with Flatten() operation
- **Symbolic Links**: Full Unix-style symlink support with loop detection
- **Metadata Operations**: Chmod, Chtimes, Lstat
- **Pattern Matching**: Glob support
- **Subtrees**: Sub() for filesystem subtrees
- **Standard Interfaces**: Implements io/fs interfaces (FS, ReadDirFS, StatFS, GlobFS, SubFS)
- **Performance**: O(1) path lookups with hash map indexing
- **Garbage Collection**: ReferencedIDs() for identifying orphaned content
- **Root Directory**: Proper handling of "/", ".", and "" as root

### 🎯 Performance Characteristics

Recent optimizations (path indexing) provide dramatic speedups:
- **Stat operations**: 9-91% faster (up to 12x speedup for base manifest lookups)
- **Rename operations**: 82-95% faster (up to 21x speedup)
- **Remove operations**: 87% faster (7.8x speedup)
- **O(1) lookups**: Hash map indexing instead of linear scans
- **Concurrent-safe**: All operations protected with read/write locks

Benchmark results (on typical workloads):
- Stat: ~200 ns/op
- ReadFile (1KB): ~50 μs/op
- WriteFile (1KB): ~100 μs/op
- ReadDir (100 files): ~50 μs/op
- Rename: ~50 μs/op for files, ~5ms for directories with 1000 children

### 🔄 Future Enhancements

- **Advanced GC**: Automatic garbage collection with mark-and-sweep
- **Compression**: Optional content compression in store
- **Encryption**: Encrypted content storage
- **Caching**: Multi-level caching for remote stores
- **Sync Protocol**: Efficient filesystem synchronization
- **Watches**: File change notifications

## Usage Examples

### Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/Avalanche-io/c4/c4m"
    "github.com/Avalanche-io/c4/store"
    "github.com/absfs/c4fs"
)

func main() {
    // Create a RAM-based store for testing (or use store.NewLocal(path) for disk)
    adapter := c4fs.NewStoreAdapter(store.NewRAM())

    // Create a new filesystem with empty base manifest
    fs := c4fs.New(nil, adapter)

    // Write a file - content is automatically dehydrated to C4 store
    err := fs.WriteFile("hello.txt", []byte("Hello, C4FS!"), 0644)
    if err != nil {
        log.Fatal(err)
    }

    // Read the file back - content is automatically hydrated
    data, err := fs.ReadFile("hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Content: %s\n", data)

    // Take a snapshot
    snapshot := fs.Flatten()
    fmt.Printf("Snapshot has %d entries\n", len(snapshot.Entries))
}
```

### Working with Snapshots

```go
// Take a snapshot of current state
snapshot := fs.Flatten()

// Save snapshot to file (manifest only, not content)
f, err := os.Create("backup.c4m")
if err != nil {
    log.Fatal(err)
}
defer f.Close()
snapshot.WriteTo(f)

// Later: restore from snapshot
// The content is still in the C4 store, we just load the manifest
data, _ := os.ReadFile("backup.c4m")
restoredManifest := c4m.Parse(data)
restoredFS := c4fs.New(restoredManifest, adapter)

// The filesystem is now exactly as it was when snapshot was taken
```

### Automatic Deduplication

```go
// Write the same content to multiple files
content := []byte("This content appears in many files")

fs.WriteFile("file1.txt", content, 0644)
fs.WriteFile("file2.txt", content, 0644)
fs.WriteFile("docs/copy.txt", content, 0644)

// All three files reference the same C4 ID
// Content is stored only once in the C4 store
// Massive space savings for duplicate content!

// Verify deduplication
refs := fs.ReferencedIDs()
fmt.Printf("Unique content blobs: %d\n", len(refs)) // Will be 1
```

### Symbolic Links

```go
// Create a file
fs.WriteFile("target.txt", []byte("target content"), 0644)

// Create a symbolic link
err := fs.Symlink("target.txt", "link.txt")
if err != nil {
    log.Fatal(err)
}

// Read through the symlink
data, err := fs.ReadFile("link.txt")
// data contains "target content"

// Check what the symlink points to
target, err := fs.ReadLink("link.txt")
fmt.Printf("Link points to: %s\n", target) // "target.txt"

// Stat without following symlink
info, err := fs.Lstat("link.txt")
// info.Mode() will show it's a symlink
```

### Copy-on-Write Layering

```go
// Create base filesystem with initial content
baseFS := c4fs.New(nil, adapter)
baseFS.WriteFile("config.json", []byte(`{"version": 1}`), 0644)
baseFS.WriteFile("readme.md", []byte("# Project"), 0644)

// Take snapshot as base layer
base := baseFS.Flatten()

// Create new filesystem with base as immutable layer
layeredFS := c4fs.New(base, adapter)

// Make changes - these go to the mutable layer
layeredFS.WriteFile("config.json", []byte(`{"version": 2}`), 0644)
layeredFS.Remove("readme.md")
layeredFS.WriteFile("new-file.txt", []byte("new content"), 0644)

// The base manifest is unchanged
// All changes are in the layer
// Flatten creates a new merged snapshot
newSnapshot := layeredFS.Flatten()
```

### Garbage Collection

```go
// Get all currently referenced C4 IDs
refs := fs.ReferencedIDs()

// Example: iterate and check which IDs are actually used
for id := range refs {
    fmt.Printf("Referenced: %s\n", id.String())
}

// Use this to identify orphaned content in your store
// and clean it up with adapter.Delete(id)

// Note: Be careful with GC across multiple filesystems/snapshots!
// An ID might be orphaned in one FS but referenced in another
```

### Directory Operations

```go
// Create directories
err := fs.Mkdir("project", 0755)
err = fs.MkdirAll("project/src/components", 0755)

// Write files in directories
fs.WriteFile("project/src/main.go", []byte("package main"), 0644)

// List directory contents
entries, err := fs.ReadDir("project/src")
for _, entry := range entries {
    fmt.Printf("%s (%d bytes)\n", entry.Name(), entry.Size())
}

// Glob pattern matching
matches, err := fs.Glob("project/**/*.go")
for _, match := range matches {
    fmt.Printf("Go file: %s\n", match)
}
```

### Rename and Move Operations

```go
// Rename a file
err := fs.Rename("old-name.txt", "new-name.txt")

// Move file to different directory
err = fs.Rename("file.txt", "archive/file.txt")

// Rename a directory (renames all children too)
err = fs.Rename("old-dir", "new-dir")
// All files like "old-dir/file.txt" become "new-dir/file.txt"
```

## Comparison with Traditional Filesystems

| Aspect | Traditional FS | C4FS |
|--------|---------------|------|
| Content | Filename → Content (mutable) | Filename → C4 ID → Content (immutable) |
| Storage | Duplicate content stored multiple times | Deduplicated by C4 ID |
| Snapshots | Copy all data | Just save manifest |
| Sync | Transfer all files | Transfer manifest + missing C4 IDs |
| Verification | None or checksums | Cryptographic C4 IDs |
| Versioning | Complex (requires external tools) | Simple (manifest history) |

## Benefits

### Space Efficiency
Automatic deduplication means same content is stored once, regardless of how many files reference it.

### Instant Snapshots
Snapshots are just manifests - no data copying required. Create thousands of snapshots with minimal overhead.

### Cryptographic Integrity
C4 IDs are based on SHA-512, providing cryptographic verification of content integrity.

### Efficient Remote Sync
1. Transfer manifest (small)
2. Compare local and remote C4 IDs
3. Only fetch missing content
4. Verify with C4 IDs

### Versioning and Time-Travel
Maintain manifest history to access any previous state of the filesystem.

### Git-like Content Addressing
Similar to how Git stores objects by hash, but for entire filesystems.

## Integration with absfs Ecosystem

c4fs integrates seamlessly with other absfs filesystem wrappers:

```go
// Compose with other wrappers
base := c4m.Parse("snapshot.c4m")
store := c4store.NewS3("my-bucket")

fs := c4fs.New(base, store)
fs = cachefs.New(fs)          // Add caching
fs = encryptfs.New(fs)        // Add encryption
fs = metricsfs.New(fs)        // Add metrics
fs = retryfs.New(fs)          // Add retry logic
```

## Technical Details

### C4 ID Format
- Length: 90 characters
- Format: `c4` + 88 base58-encoded characters
- Based on: SHA-512 hash of content
- Example: `c41a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7a8b9c0d1e2f3g4h5i6j7k8l9m0n1`

### Storage Layout
Content-addressable directory structure:
```
/var/c4/
  c4/
    1a/
      c41a2b3c...  (full C4 ID as filename)
    2b/
      c42b3c4d...
```

### Manifest Format
C4M v1.0 specification (SMPTE ST 2114:2017):
- JSON-based manifest
- Entry per file/directory
- Stores: path, C4 ID, size, mode, timestamps
- Hierarchical directory structure

### Metadata Storage
All metadata stored in C4M manifest:
- Unix permissions (mode)
- Timestamps (modified, accessed, created)
- Original file size
- File type (regular, directory, symlink)

### Reference Counting
For garbage collection:
- Track C4 ID references across all manifests
- Only delete when refcount reaches zero
- Incremental GC to avoid performance impact

### Copy-on-Write Semantics
- Base manifest: Immutable
- Layer manifest: Mutable
- Read priority: Layer → Base
- Write destination: Always layer
- Flatten: Merge into new immutable manifest

## Testing Strategy

### Unit Tests
- Store implementations (Local, Memory, S3)
- C4M parsing and generation
- Reference counting
- Manifest merging and diffing

### Integration Tests
- Full hydration/dehydration cycle
- Copy-on-write operations
- Snapshot creation and restoration
- Deduplication verification

### Performance Benchmarks
- Store operations (Put, Get, Has, Delete)
- Hydration/dehydration throughput
- Manifest operations (merge, diff, flatten)
- Large directory listing

### Correctness Tests
- Metadata preservation
- Content integrity verification
- Reference counting accuracy
- Concurrent access safety

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please ensure:
- Tests pass: `go test ./...`
- Code formatted: `go fmt ./...`
- Linter clean: `golangci-lint run`

## Related Projects

- [absfs](https://github.com/absfs/absfs) - Abstract filesystem interface
- [c4](https://github.com/Avalanche-io/c4) - C4 ID implementation
- [osfs](https://github.com/absfs/osfs) - OS filesystem wrapper
- [memfs](https://github.com/absfs/memfs) - In-memory filesystem
- [s3fs](https://github.com/absfs/s3fs) - S3 filesystem wrapper
