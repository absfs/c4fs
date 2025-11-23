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

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

// FS implements a content-addressable filesystem using C4 IDs.
// It uses a copy-on-write architecture with an immutable base manifest
// and a mutable layer manifest for changes.
type FS struct {
	mu         sync.RWMutex
	base       *c4m.Manifest           // Immutable base (snapshot)
	layer      *c4m.Manifest           // Mutable overlay (starts empty)
	store      *StoreAdapter           // Content storage
	baseIndex  map[string]*c4m.Entry   // Index for fast base lookups
	layerIndex map[string]*c4m.Entry   // Index for fast layer lookups
}

// buildIndex creates a path -> entry index from a manifest for O(1) lookups.
func buildIndex(manifest *c4m.Manifest) map[string]*c4m.Entry {
	index := make(map[string]*c4m.Entry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		index[entry.Name] = entry
	}
	return index
}

// New creates a new C4FS filesystem.
// If base is nil, an empty manifest is created.
func New(base *c4m.Manifest, store *StoreAdapter) *FS {
	if base == nil {
		base = c4m.NewManifest()
	}

	return &FS{
		base:       base,
		layer:      c4m.NewManifest(),
		store:      store,
		baseIndex:  buildIndex(base),
		layerIndex: make(map[string]*c4m.Entry),
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
		base:       base,
		layer:      layer,
		store:      store,
		baseIndex:  buildIndex(base),
		layerIndex: buildIndex(layer),
	}
}

// getEntry looks up an entry in the filesystem.
// Checks layer first, then falls back to base.
// Returns error if entry is a tombstone (deleted).
func (c4fs *FS) getEntry(path string) (*c4m.Entry, error) {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Normalize path
	path = filepath.Clean(path)
	if path == "." || path == "/" {
		path = ""
	}

	// Special case: root directory always exists as a virtual directory
	if path == "" {
		return &c4m.Entry{
			Mode:      fs.ModeDir | 0755,
			Timestamp: time.Now().UTC(),
			Size:      0,
			Name:      "",
			C4ID:      c4.ID{},
		}, nil
	}

	// Check layer first using index for O(1) lookup
	if entry, exists := c4fs.layerIndex[path]; exists {
		// Check for tombstone (Size = -1 means deleted)
		if entry.Size == -1 {
			return nil, &fs.PathError{
				Op:   "stat",
				Path: path,
				Err:  fs.ErrNotExist,
			}
		}
		return entry, nil
	}

	// Fall back to base using index for O(1) lookup
	if entry, exists := c4fs.baseIndex[path]; exists {
		return entry, nil
	}

	return nil, &fs.PathError{
		Op:   "stat",
		Path: path,
		Err:  fs.ErrNotExist,
	}
}

// Stat returns file information for the given path.
// Unlike Lstat, this follows symbolic links.
func (c4fs *FS) Stat(name string) (fs.FileInfo, error) {
	// Resolve symlinks (max depth 40, same as Linux)
	entry, err := c4fs.resolveSymlink(name, 40)
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
// This follows symbolic links.
func (c4fs *FS) Open(name string) (fs.File, error) {
	// Resolve symlinks (max depth 40)
	entry, err := c4fs.resolveSymlink(name, 40)
	if err != nil {
		return nil, err
	}

	if entry.IsDir() {
		// For directories, use the resolved path
		return c4fs.openDir(entry.Name, entry)
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
	if name == "." || name == "/" {
		name = ""
	}

	// Collect entries from both layer and base
	seen := make(map[string]bool)
	tombstones := make(map[string]bool)
	var entries []fs.DirEntry

	// Add entries from layer (and track tombstones)
	for _, e := range c4fs.layer.Entries {
		if c4fs.isDirectChild(name, e.Name) {
			basename := filepath.Base(e.Name)
			if !seen[basename] {
				seen[basename] = true
				// Check for tombstone
				if e.Size == -1 {
					tombstones[basename] = true
					continue
				}
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

	// Add entries from base (if not already in layer and not tombstoned)
	for _, e := range c4fs.base.Entries {
		if c4fs.isDirectChild(name, e.Name) {
			basename := filepath.Base(e.Name)
			if !seen[basename] && !tombstones[basename] {
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

	if parentPath == "." || parentPath == "/" {
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
// Tombstones in the layer cause corresponding base entries to be excluded.
func (c4fs *FS) Flatten() *c4m.Manifest {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	result := c4m.NewManifest()

	// Collect tombstones from layer
	tombstones := make(map[string]bool)
	for _, e := range c4fs.layer.Entries {
		if e.Size == -1 {
			tombstones[e.Name] = true
		}
	}

	// Add entries from base (excluding tombstoned ones)
	for _, e := range c4fs.base.Entries {
		if !tombstones[e.Name] {
			result.AddEntry(e)
		}
	}

	// Add non-tombstone entries from layer
	for _, e := range c4fs.layer.Entries {
		if e.Size != -1 {
			result.AddEntry(e)
		}
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

// ReferencedIDs returns a set of all C4 IDs currently referenced by the filesystem.
// This includes IDs from both the base and layer manifests, excluding tombstones
// and shadowed entries. The returned map can be used for garbage collection to
// identify orphaned content.
func (c4fs *FS) ReferencedIDs() map[c4.ID]bool {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	refs := make(map[c4.ID]bool)

	// Collect tombstones and shadowed entries from layer
	tombstones := make(map[string]bool)
	layerEntries := make(map[string]bool)
	for _, e := range c4fs.layer.Entries {
		if e.Size == -1 {
			tombstones[e.Name] = true
		} else {
			layerEntries[e.Name] = true
		}
	}

	// Add IDs from base (excluding tombstoned and shadowed entries and directories)
	for _, e := range c4fs.base.Entries {
		if !tombstones[e.Name] && !layerEntries[e.Name] && !e.IsDir() && e.Size > 0 {
			refs[e.C4ID] = true
		}
	}

	// Add IDs from layer (excluding tombstones and directories)
	for _, e := range c4fs.layer.Entries {
		if e.Size != -1 && !e.IsDir() && e.Size > 0 {
			refs[e.C4ID] = true
		}
	}

	return refs
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
	if name == "." || name == "/" {
		name = ""
	}

	// Cannot create root directory
	if name == "" {
		return &fs.PathError{
			Op:   "mkdir",
			Path: name,
			Err:  fs.ErrExist,
		}
	}

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
	name = filepath.Clean(name)

	// If already exists, check if it's a directory
	if c4fs.Exists(name) {
		if !c4fs.IsDir(name) {
			return &fs.PathError{
				Op:   "mkdir",
				Path: name,
				Err:  fmt.Errorf("not a directory"),
			}
		}
		return nil
	}

	// Get parent directory
	parent := filepath.Dir(name)
	if parent != "." && parent != "/" && parent != "" {
		// Recursively create parent
		if err := c4fs.MkdirAll(parent, perm); err != nil {
			return err
		}
	}

	// Create this directory
	return c4fs.Mkdir(name, perm)
}

// Remove removes the named file or empty directory.
// In a copy-on-write filesystem, this adds a tombstone marker to the layer.
func (c4fs *FS) Remove(name string) error {
	name = filepath.Clean(name)
	if name == "." || name == "/" {
		name = ""
	}

	// Cannot remove root directory
	if name == "" {
		return &fs.PathError{
			Op:   "remove",
			Path: name,
			Err:  fmt.Errorf("cannot remove root directory"),
		}
	}

	// Check if file exists
	entry, err := c4fs.getEntry(name)
	if err != nil {
		return err
	}

	// If it's a directory, check that it's empty
	if entry.IsDir() {
		entries, err := c4fs.readDir(name)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return &fs.PathError{
				Op:   "remove",
				Path: name,
				Err:  fmt.Errorf("directory not empty"),
			}
		}
	}

	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	// Add tombstone marker to layer
	// Tombstone is an entry with Size = -1
	tombstone := &c4m.Entry{
		Mode:      0,
		Timestamp: time.Now().UTC(),
		Size:      -1, // Tombstone marker
		Name:      name,
		C4ID:      c4.ID{}, // Empty ID
	}

	c4fs.updateEntryInLayer(tombstone)
	return nil
}

// RemoveAll removes a path and any children it contains.
// For directories, it recursively removes all contents.
func (c4fs *FS) RemoveAll(name string) error {
	name = filepath.Clean(name)

	// Check if exists
	entry, err := c4fs.getEntry(name)
	if err != nil {
		// If doesn't exist, RemoveAll succeeds (like os.RemoveAll)
		if isPathErrorWithNotExist(err) {
			return nil
		}
		return err
	}

	// If it's a directory, remove all children first
	if entry.IsDir() {
		entries, err := c4fs.readDir(name)
		if err != nil {
			return err
		}

		// Recursively remove all children
		for _, e := range entries {
			childPath := filepath.Join(name, e.Name())
			if err := c4fs.RemoveAll(childPath); err != nil {
				return err
			}
		}
	}

	// Now remove the entry itself (directory is now empty)
	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	// Add tombstone marker to layer
	tombstone := &c4m.Entry{
		Mode:      0,
		Timestamp: time.Now().UTC(),
		Size:      -1, // Tombstone marker
		Name:      name,
		C4ID:      c4.ID{}, // Empty ID
	}

	c4fs.updateEntryInLayer(tombstone)
	return nil
}

// Helper function to check if error is a PathError with ErrNotExist
func isPathErrorWithNotExist(err error) bool {
	if pathErr, ok := err.(*fs.PathError); ok {
		return pathErr.Err == fs.ErrNotExist
	}
	return false
}

// Rename renames (moves) oldpath to newpath.
// For directories, all children are recursively renamed.
func (c4fs *FS) Rename(oldname, newname string) error {
	oldname = filepath.Clean(oldname)
	newname = filepath.Clean(newname)
	if oldname == "." || oldname == "/" {
		oldname = ""
	}
	if newname == "." || newname == "/" {
		newname = ""
	}

	// Cannot rename root directory
	if oldname == "" || newname == "" {
		return &fs.PathError{
			Op:   "rename",
			Path: oldname,
			Err:  fmt.Errorf("cannot rename root directory"),
		}
	}

	// Check source exists
	oldEntry, err := c4fs.getEntry(oldname)
	if err != nil {
		return err
	}

	// Check if destination already exists
	if c4fs.Exists(newname) {
		return &fs.PathError{
			Op:   "rename",
			Path: newname,
			Err:  fs.ErrExist,
		}
	}

	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	// If it's a directory, we need to rename all children
	if oldEntry.IsDir() {
		// Get all entries that are descendants of oldname
		var toRename []*c4m.Entry

		// Check both base and layer for children
		for _, e := range c4fs.base.Entries {
			if e.Name == oldname || strings.HasPrefix(e.Name, oldname+"/") {
				toRename = append(toRename, e)
			}
		}
		for _, e := range c4fs.layer.Entries {
			// Skip tombstones
			if e.Size == -1 {
				continue
			}
			if e.Name == oldname || strings.HasPrefix(e.Name, oldname+"/") {
				// Check if already in toRename (from base)
				found := false
				for i, existing := range toRename {
					if existing.Name == e.Name {
						// Replace with layer version
						toRename[i] = e
						found = true
						break
					}
				}
				if !found {
					toRename = append(toRename, e)
				}
			}
		}

		// Create new entries with updated paths
		for _, e := range toRename {
			newPath := strings.Replace(e.Name, oldname, newname, 1)
			newEntry := &c4m.Entry{
				Mode:      e.Mode,
				Timestamp: e.Timestamp,
				Size:      e.Size,
				Name:      newPath,
				C4ID:      e.C4ID,
				Target:    e.Target,
			}
			c4fs.updateEntryInLayer(newEntry)
		}

		// Add tombstones for all old paths
		for _, e := range toRename {
			tombstone := &c4m.Entry{
				Mode:      0,
				Timestamp: time.Now().UTC(),
				Size:      -1,
				Name:      e.Name,
				C4ID:      c4.ID{},
			}
			c4fs.updateEntryInLayer(tombstone)
		}
	} else {
		// Simple file rename
		newEntry := &c4m.Entry{
			Mode:      oldEntry.Mode,
			Timestamp: oldEntry.Timestamp,
			Size:      oldEntry.Size,
			Name:      newname,
			C4ID:      oldEntry.C4ID,
			Target:    oldEntry.Target,
		}
		c4fs.updateEntryInLayer(newEntry)

		// Add tombstone for old name
		tombstone := &c4m.Entry{
			Mode:      0,
			Timestamp: time.Now().UTC(),
			Size:      -1,
			Name:      oldname,
			C4ID:      c4.ID{},
		}
		c4fs.updateEntryInLayer(tombstone)
	}

	return nil
}

// Sub returns an FS corresponding to the subtree rooted at dir.
// This implements fs.SubFS for better composability.
func (c4fs *FS) Sub(dir string) (fs.FS, error) {
	// Normalize the directory path
	dir = filepath.Clean(dir)
	if dir == "." || dir == "/" {
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
	tombstones := make(map[string]bool)
	var allPaths []string

	// Add from layer (and track tombstones)
	for _, e := range c4fs.layer.Entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			if e.Size == -1 {
				tombstones[e.Name] = true
			} else {
				allPaths = append(allPaths, e.Name)
			}
		}
	}

	// Add from base (excluding tombstones)
	for _, e := range c4fs.base.Entries {
		if !seen[e.Name] && !tombstones[e.Name] {
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
// This also updates the layer index for O(1) lookups.
func (c4fs *FS) updateEntryInLayer(entry *c4m.Entry) {
	name := entry.Name

	// Check if entry already exists in layer
	if oldEntry, exists := c4fs.layerIndex[name]; exists {
		// Remove old entry from manifest (linear scan, but only when updating)
		for i, e := range c4fs.layer.Entries {
			if e.Name == name {
				c4fs.layer.Entries = append(c4fs.layer.Entries[:i], c4fs.layer.Entries[i+1:]...)
				break
			}
		}
		_ = oldEntry // Suppress unused warning
	}

	// Add new entry to manifest
	c4fs.layer.AddEntry(entry)

	// Update index
	c4fs.layerIndex[name] = entry
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

// Symlink creates a symbolic link at name pointing to target.
func (c4fs *FS) Symlink(target, name string) error {
	c4fs.mu.Lock()
	defer c4fs.mu.Unlock()

	name = filepath.Clean(name)

	// Check if already exists
	if entry := c4fs.layer.GetEntry(name); entry != nil && entry.Size != -1 {
		return &fs.PathError{
			Op:   "symlink",
			Path: name,
			Err:  fs.ErrExist,
		}
	}
	if entry := c4fs.base.GetEntry(name); entry != nil {
		return &fs.PathError{
			Op:   "symlink",
			Path: name,
			Err:  fs.ErrExist,
		}
	}

	// Create symlink entry
	entry := &c4m.Entry{
		Mode:      fs.ModeSymlink | 0777, // Symlinks typically have 0777 permissions
		Timestamp: time.Now().UTC(),
		Size:      0,
		Name:      name,
		Target:    target,
		C4ID:      c4.ID{}, // Empty ID for symlinks
	}

	c4fs.updateEntryInLayer(entry)
	return nil
}

// ReadLink reads the target of a symbolic link.
// It returns the target path without resolving it.
func (c4fs *FS) ReadLink(name string) (string, error) {
	// Use lstatEntry to get symlink without following it
	entry, err := c4fs.lstatEntry(name)
	if err != nil {
		return "", err
	}

	if entry.Mode&fs.ModeSymlink == 0 {
		return "", &fs.PathError{
			Op:   "readlink",
			Path: name,
			Err:  fmt.Errorf("not a symlink"),
		}
	}

	return entry.Target, nil
}

// Lstat returns file information for the named file without following symlinks.
// This is like Stat but doesn't follow symbolic links.
func (c4fs *FS) Lstat(name string) (fs.FileInfo, error) {
	entry, err := c4fs.lstatEntry(name)
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

// lstatEntry is like getEntry but doesn't follow symlinks.
func (c4fs *FS) lstatEntry(path string) (*c4m.Entry, error) {
	c4fs.mu.RLock()
	defer c4fs.mu.RUnlock()

	// Normalize path
	path = filepath.Clean(path)
	if path == "." || path == "/" {
		path = ""
	}

	// Special case: root directory always exists as a virtual directory
	if path == "" {
		return &c4m.Entry{
			Mode:      fs.ModeDir | 0755,
			Timestamp: time.Now().UTC(),
			Size:      0,
			Name:      "",
			C4ID:      c4.ID{},
		}, nil
	}

	// Check layer first using index for O(1) lookup
	if entry, exists := c4fs.layerIndex[path]; exists {
		// Check for tombstone (Size = -1 means deleted)
		if entry.Size == -1 {
			return nil, &fs.PathError{
				Op:   "lstat",
				Path: path,
				Err:  fs.ErrNotExist,
			}
		}
		return entry, nil
	}

	// Fall back to base using index for O(1) lookup
	if entry, exists := c4fs.baseIndex[path]; exists {
		return entry, nil
	}

	return nil, &fs.PathError{
		Op:   "lstat",
		Path: path,
		Err:  fs.ErrNotExist,
	}
}

// resolveSymlink resolves a symlink entry to its target entry.
// It follows symlink chains up to a maximum depth to prevent infinite loops.
// This also resolves symlinks in the directory path (e.g., "dirlink/file.txt").
func (c4fs *FS) resolveSymlink(path string, maxDepth int) (*c4m.Entry, error) {
	if maxDepth <= 0 {
		return nil, &fs.PathError{
			Op:   "stat",
			Path: path,
			Err:  fmt.Errorf("too many levels of symbolic links"),
		}
	}

	// Clean the path
	path = filepath.Clean(path)

	// Resolve symlinks in each component of the path
	components := strings.Split(path, "/")
	resolvedPath := ""

	for i, component := range components {
		if component == "" || component == "." {
			continue
		}

		// Build current path
		if resolvedPath == "" {
			resolvedPath = component
		} else {
			resolvedPath = filepath.Join(resolvedPath, component)
		}

		// Check if this component is a symlink
		entry, err := c4fs.lstatEntry(resolvedPath)
		if err != nil {
			// If we can't find an intermediate component, return the error
			return nil, err
		}

		// If it's a symlink, resolve it
		if entry.Mode&fs.ModeSymlink != 0 {
			target := entry.Target

			// Handle relative vs absolute paths
			if !filepath.IsAbs(target) {
				dir := filepath.Dir(resolvedPath)
				if dir != "." && dir != "" {
					target = filepath.Join(dir, target)
				}
			}

			// If there are more components, append them to the target
			if i < len(components)-1 {
				remaining := filepath.Join(components[i+1:]...)
				target = filepath.Join(target, remaining)
			}

			// Recursively resolve from the target
			return c4fs.resolveSymlink(target, maxDepth-1)
		}
	}

	// Return the entry at the resolved path
	entry, err := c4fs.lstatEntry(resolvedPath)
	if err != nil {
		return nil, err
	}

	return entry, nil
}
