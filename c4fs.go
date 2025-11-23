package c4fs

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Avalanche-io/c4/c4m"
)

// FS implements a content-addressable filesystem using C4 IDs.
// It uses a copy-on-write architecture with an immutable base manifest
// and a mutable layer manifest for changes.
type FS struct {
	mu    sync.RWMutex
	base  *c4m.Manifest  // Immutable base (snapshot)
	layer *c4m.Manifest  // Mutable overlay (starts empty)
	store *StoreAdapter  // Content storage
}

// New creates a new C4FS filesystem.
// If base is nil, an empty manifest is created.
func New(base *c4m.Manifest, store *StoreAdapter) *FS {
	if base == nil {
		base = c4m.NewManifest()
	}

	return &FS{
		base:  base,
		layer: c4m.NewManifest(),
		store: store,
	}
}

// NewWithLayer creates a new C4FS filesystem with an existing layer.
func NewWithLayer(base, layer *c4m.Manifest, store *StoreAdapter) *FS {
	if base == nil {
		base = c4m.NewManifest()
	}
	if layer == nil {
		layer = c4m.NewManifest()
	}

	return &FS{
		base:  base,
		layer: layer,
		store: store,
	}
}

// getEntry looks up an entry in the filesystem.
// Checks layer first, then falls back to base.
func (c4fs *FS) getEntry(path string) (*c4m.Entry, error) {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Normalize path
	path = filepath.Clean(path)
	if path == "." {
		path = ""
	}

	// Check layer first
	if entry := c4fs.layer.GetEntry(path); entry != nil {
		return entry, nil
	}

	// Fall back to base
	if entry := c4fs.base.GetEntry(path); entry != nil {
		return entry, nil
	}

	return nil, &fs.PathError{
		Op:   "stat",
		Path: path,
		Err:  fs.ErrNotExist,
	}
}

// Stat returns file information for the given path.
func (c4fs *FS) Stat(name string) (fs.FileInfo, error) {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return nil, err
	}

	return &fileInfo{
		name:    filepath.Base(entry.Name),
		size:    entry.Size,
		mode:    entry.Mode,
		modTime: entry.Timestamp,
		isDir:   entry.IsDir(),
	}, nil
}

// Open opens the named file for reading.
func (c4fs *FS) Open(name string) (fs.File, error) {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return nil, err
	}

	if entry.IsDir() {
		return c4fs.openDir(name, entry)
	}

	return c4fs.openFile(name, entry)
}

// openFile opens a regular file for reading (hydration).
func (c4fs *FS) openFile(name string, entry *c4m.Entry) (fs.File, error) {
	// Get content from store
	rc, err := c4fs.store.Get(entry.C4ID)
	if err != nil {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fmt.Errorf("failed to hydrate content: %w", err),
		}
	}

	info := &fileInfo{
		name:    filepath.Base(entry.Name),
		size:    entry.Size,
		mode:    entry.Mode,
		modTime: entry.Timestamp,
		isDir:   false,
	}

	return &readOnlyFile{
		ReadCloser: rc,
		info:       info,
		pos:        0,
	}, nil
}

// openDir opens a directory for reading.
func (c4fs *FS) openDir(name string, entry *c4m.Entry) (fs.File, error) {
	entries, err := c4fs.readDir(name)
	if err != nil {
		return nil, err
	}

	info := &fileInfo{
		name:    filepath.Base(entry.Name),
		size:    entry.Size,
		mode:    entry.Mode | fs.ModeDir,
		modTime: entry.Timestamp,
		isDir:   true,
	}

	return &dirFile{
		entries: entries,
		info:    info,
	}, nil
}

// readDir reads the contents of a directory.
func (c4fs *FS) readDir(name string) ([]fs.DirEntry, error) {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Normalize path
	name = filepath.Clean(name)
	if name == "." {
		name = ""
	}

	// Collect entries from both layer and base
	seen := make(map[string]bool)
	var entries []fs.DirEntry

	// Add entries from layer
	for _, e := range c4fs.layer.Entries {
		if c4fs.isDirectChild(name, e.Name) {
			basename := filepath.Base(e.Name)
			if !seen[basename] {
				seen[basename] = true
				entries = append(entries, &dirEntry{
					info: &fileInfo{
						name:    basename,
						size:    e.Size,
						mode:    e.Mode,
						modTime: e.Timestamp,
						isDir:   e.IsDir(),
					},
				})
			}
		}
	}

	// Add entries from base (if not already in layer)
	for _, e := range c4fs.base.Entries {
		if c4fs.isDirectChild(name, e.Name) {
			basename := filepath.Base(e.Name)
			if !seen[basename] {
				seen[basename] = true
				entries = append(entries, &dirEntry{
					info: &fileInfo{
						name:    basename,
						size:    e.Size,
						mode:    e.Mode,
						modTime: e.Timestamp,
						isDir:   e.IsDir(),
					},
				})
			}
		}
	}

	return entries, nil
}

// isDirectChild checks if childPath is a direct child of parentPath.
func (c4fs *FS) isDirectChild(parentPath, childPath string) bool {
	parentPath = filepath.Clean(parentPath)
	childPath = filepath.Clean(childPath)

	if parentPath == "." {
		parentPath = ""
	}

	if parentPath == "" {
		// Root directory - check if child has no slashes
		return !strings.Contains(childPath, "/")
	}

	// Check if child starts with parent
	if !strings.HasPrefix(childPath, parentPath+"/") {
		return false
	}

	// Check if there are no additional slashes after parent
	remainder := strings.TrimPrefix(childPath, parentPath+"/")
	return !strings.Contains(remainder, "/")
}

// ReadDir reads the directory named by dirname and returns
// a list of directory entries.
func (c4fs *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	return c4fs.readDir(name)
}

// ReadFile reads the named file and returns its contents.
func (c4fs *FS) ReadFile(name string) ([]byte, error) {
	f, err := c4fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}

// dirFile implements fs.File and fs.ReadDirFile for directories.
type dirFile struct {
	entries []fs.DirEntry
	info    *fileInfo
	pos     int
}

func (d *dirFile) Stat() (fs.FileInfo, error) {
	return d.info, nil
}

func (d *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: d.info.name,
		Err:  fmt.Errorf("is a directory"),
	}
}

func (d *dirFile) Close() error {
	return nil
}

// ReadDir reads the contents of the directory.
// This implements fs.ReadDirFile for better compatibility.
func (d *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		// Return all remaining entries
		entries := d.entries[d.pos:]
		d.pos = len(d.entries)
		return entries, nil
	}

	if d.pos >= len(d.entries) {
		return nil, io.EOF
	}

	end := d.pos + n
	if end > len(d.entries) {
		end = len(d.entries)
	}

	entries := d.entries[d.pos:end]
	d.pos = end

	return entries, nil
}

// Flatten merges the base and layer manifests into a new manifest.
// This creates a new snapshot of the current filesystem state.
func (c4fs *FS) Flatten() *c4m.Manifest {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Use c4m.Union to merge base and layer
	result, err := c4m.Union(
		&c4m.ManifestSource{Manifest: c4fs.base},
		&c4m.ManifestSource{Manifest: c4fs.layer},
	)
	if err != nil {
		// Shouldn't happen with valid manifests, but handle gracefully
		result = c4m.NewManifest()
	}

	return result
}

// Base returns a copy of the base manifest.
func (c4fs *FS) Base() *c4m.Manifest {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()
	return c4fs.base.Copy()
}

// Layer returns a copy of the layer manifest.
func (c4fs *FS) Layer() *c4m.Manifest {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()
	return c4fs.layer.Copy()
}

// Store returns the underlying content store.
func (c4fs *FS) Store() *StoreAdapter {
	return c4fs.store
}

// WriteFile writes data to the named file, creating it if necessary.
// This is a dehydration operation: content → C4 ID → layer manifest.
func (c4fs *FS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	// Dehydrate content to store
	id, err := c4fs.store.Put(bytes.NewReader(data))
	if err != nil {
		return &fs.PathError{
			Op:   "write",
			Path: name,
			Err:  fmt.Errorf("failed to dehydrate content: %w", err),
		}
	}

	// Create entry in layer
	entry := &c4m.Entry{
		Mode:      perm,
		Timestamp: time.Now().UTC(),
		Size:      int64(len(data)),
		Name:      filepath.Clean(name),
		C4ID:      id,
	}

	c4fs.mu.Lock()
	c4fs.updateEntryInLayer(entry)
	c4fs.mu.Unlock()

	return nil
}

// Create creates a file for writing.
func (c4fs *FS) Create(name string) (File, error) {
	return newDehydratingFile(c4fs, name, 0644)
}

// Mkdir creates a new directory.
func (c4fs *FS) Mkdir(name string, perm fs.FileMode) error {
	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	name = filepath.Clean(name)

	// Check if already exists
	if entry := c4fs.layer.GetEntry(name); entry != nil {
		return &fs.PathError{
			Op:   "mkdir",
			Path: name,
			Err:  fs.ErrExist,
		}
	}
	if entry := c4fs.base.GetEntry(name); entry != nil {
		return &fs.PathError{
			Op:   "mkdir",
			Path: name,
			Err:  fs.ErrExist,
		}
	}

	// Create directory entry
	entry := &c4m.Entry{
		Mode:      perm | fs.ModeDir,
		Timestamp: time.Now().UTC(),
		Size:      0,
		Name:      name,
	}

	c4fs.updateEntryInLayer(entry)
	return nil
}

// MkdirAll creates a directory and all necessary parents.
func (c4fs *FS) MkdirAll(name string, perm fs.FileMode) error {
	// TODO: Implement recursive directory creation
	return c4fs.Mkdir(name, perm)
}

// Remove removes the named file or empty directory.
func (c4fs *FS) Remove(name string) error {
	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	// Add a deletion marker in the layer
	// This is a simplified implementation
	// A full implementation would need proper deletion semantics

	// For now, we'll just not add anything to represent deletion
	// A proper implementation would need to handle this better

	return &fs.PathError{
		Op:   "remove",
		Path: name,
		Err:  fmt.Errorf("not implemented"),
	}
}

// RemoveAll removes a path and any children it contains.
func (c4fs *FS) RemoveAll(name string) error {
	return c4fs.Remove(name)
}

// Rename renames (moves) oldpath to newpath.
func (c4fs *FS) Rename(oldname, newname string) error {
	return &fs.PathError{
		Op:   "rename",
		Path: oldname,
		Err:  fmt.Errorf("not implemented"),
	}
}

// Sub returns an FS corresponding to the subtree rooted at dir.
// This implements fs.SubFS for better composability.
func (c4fs *FS) Sub(dir string) (fs.FS, error) {
	// Normalize the directory path
	dir = filepath.Clean(dir)
	if dir == "." {
		dir = ""
	}

	// Check that dir exists and is a directory
	if dir != "" {
		entry, err := c4fs.getEntry(dir)
		if err != nil {
			return nil, err
		}
		if !entry.IsDir() {
			return nil, &fs.PathError{
				Op:   "sub",
				Path: dir,
				Err:  fmt.Errorf("not a directory"),
			}
		}
	}

	return &subFS{
		parent: c4fs,
		prefix: dir,
	}, nil
}

// Glob returns the names of all files matching pattern.
// This implements fs.GlobFS for pattern matching.
func (c4fs *FS) Glob(pattern string) ([]string, error) {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Collect all file paths from base and layer
	seen := make(map[string]bool)
	var allPaths []string

	// Add from layer
	for _, e := range c4fs.layer.Entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			allPaths = append(allPaths, e.Name)
		}
	}

	// Add from base
	for _, e := range c4fs.base.Entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			allPaths = append(allPaths, e.Name)
		}
	}

	// Filter by pattern
	var matches []string
	for _, path := range allPaths {
		matched, err := filepath.Match(pattern, path)
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, path)
		}
	}

	return matches, nil
}

// subFS implements a view into a subdirectory of an FS.
type subFS struct {
	parent *FS
	prefix string
}

func (s *subFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	fullPath := filepath.Join(s.prefix, name)
	return s.parent.Open(fullPath)
}

func (s *subFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	fullPath := filepath.Join(s.prefix, name)
	return s.parent.ReadDir(fullPath)
}

func (s *subFS) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrInvalid}
	}
	fullPath := filepath.Join(s.prefix, name)
	return s.parent.ReadFile(fullPath)
}

func (s *subFS) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	fullPath := filepath.Join(s.prefix, name)
	return s.parent.Stat(fullPath)
}

func (s *subFS) Glob(pattern string) ([]string, error) {
	// Get all matches from parent
	matches, err := s.parent.Glob(filepath.Join(s.prefix, pattern))
	if err != nil {
		return nil, err
	}

	// Strip prefix from matches
	var result []string
	for _, match := range matches {
		rel, err := filepath.Rel(s.prefix, match)
		if err == nil && !strings.HasPrefix(rel, "..") {
			result = append(result, rel)
		}
	}

	return result, nil
}

func (s *subFS) Sub(dir string) (fs.FS, error) {
	if !fs.ValidPath(dir) {
		return nil, &fs.PathError{Op: "sub", Path: dir, Err: fs.ErrInvalid}
	}
	fullPath := filepath.Join(s.prefix, dir)
	return s.parent.Sub(fullPath)
}

// updateEntryInLayer adds or updates an entry in the layer manifest.
// If an entry with the same name exists in the layer, it is removed first.
func (c4fs *FS) updateEntryInLayer(entry *c4m.Entry) {
	name := entry.Name

	// Remove existing entry with same name from layer (if any)
	var newEntries []*c4m.Entry
	for _, e := range c4fs.layer.Entries {
		if e.Name != name {
			newEntries = append(newEntries, e)
		}
	}
	c4fs.layer.Entries = newEntries

	// Add new entry
	c4fs.layer.AddEntry(entry)
}

// Chmod changes the mode of the named file in the layer.
func (c4fs *FS) Chmod(name string, mode fs.FileMode) error {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return err
	}

	// Create updated entry in layer with new mode
	newEntry := &c4m.Entry{
		Mode:      mode,
		Timestamp: entry.Timestamp, // Preserve timestamp
		Size:      entry.Size,
		Name:      filepath.Clean(name),
		C4ID:      entry.C4ID,
		Target:    entry.Target,
	}

	c4fs.mu.Lock()
	c4fs.updateEntryInLayer(newEntry)
	c4fs.mu.Unlock()

	return nil
}

// Chtimes changes the access and modification times of the named file in the layer.
func (c4fs *FS) Chtimes(name string, atime, mtime time.Time) error {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return err
	}

	// Create updated entry in layer with new timestamp
	// Note: C4M only stores one timestamp, so we use mtime
	newEntry := &c4m.Entry{
		Mode:      entry.Mode,
		Timestamp: mtime,
		Size:      entry.Size,
		Name:      filepath.Clean(name),
		C4ID:      entry.C4ID,
		Target:    entry.Target,
	}

	c4fs.mu.Lock()
	c4fs.updateEntryInLayer(newEntry)
	c4fs.mu.Unlock()

	return nil
}

// Exists checks if a file or directory exists.
func (c4fs *FS) Exists(name string) bool {
	_, err := c4fs.getEntry(name)
	return err == nil
}

// IsDir checks if the path is a directory.
func (c4fs *FS) IsDir(name string) bool {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return false
	}
	return entry.IsDir()
}

// IsFile checks if the path is a regular file.
func (c4fs *FS) IsFile(name string) bool {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return false
	}
	return !entry.IsDir()
}

// Size returns the size of the named file.
func (c4fs *FS) Size(name string) (int64, error) {
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return 0, err
	}
	return entry.Size, nil
}
