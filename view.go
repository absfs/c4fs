package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// MaxSymlinks is how many symbolic links one path resolution follows
// before failing with ELOOP, as Linux's MAXSYMLINKS.
const MaxSymlinks = 40

// View is a read-only absfs filesystem over the tree a c4m manifest
// describes, with file content read lazily by C4 ID from a c4 store.
//
// A View is safe for concurrent use by any number of goroutines.
type View struct {
	src   store.Source
	cache *Cache
	root  *node
	cwd   atomic.Pointer[string]

	// streams is set once the store's Open is seen to return a reader
	// without random access, after which first reads hydrate directly.
	streams atomic.Bool

	id atomic.Pointer[c4.ID] // the tree's c4m ID, once computed
}

var _ absfs.SymlinkFileSystem = (*View)(nil)

// Option configures a View.
type Option func(*View)

// WithCache sets the cache a View hydrates into when its store cannot
// serve random access. Without it the View uses a process-wide cache in
// DefaultCacheDir, created only if it is needed.
func WithCache(c *Cache) Option {
	return func(v *View) { v.cache = c }
}

// New returns a View of the tree m describes, reading content from src.
//
// src may be nil for a View of names and metadata only: listing and
// stat work, and reading a non-empty file fails with ErrContentMissing.
// src is needed to load what m refers to rather than contains: an
// external patch base (m.Base), a directory listed by C4 ID without its
// children, or a sequence's ID list.
//
// m must not be modified while the View is in use.
func New(m *c4m.Manifest, src store.Source, opts ...Option) (*View, error) {
	v := newView(src, opts)
	kids, err := v.fromChain(&chain{base: m.Base, sections: [][]*c4m.Entry{m.Entries}, rd: m.RangeData}, 0)
	if err != nil {
		return nil, err
	}
	v.setRoot(kids)
	return v, nil
}

// Open returns a View of the manifest whose C4 ID is id, read from src.
// id may name a full manifest (as c4 id -s stores under the ID it
// prints), a one-level directory record (the root directory's ID), or a
// patch whose first line names its base. Directories given only by ID
// load their records on first use, so opening a large tree reads only
// its top level.
func Open(src store.Source, id c4.ID, opts ...Option) (*View, error) {
	if src == nil {
		return nil, ErrNoStore
	}
	v := newView(src, opts)
	kids, err := v.fromID(id, 0)
	if err != nil {
		return nil, err
	}
	v.setRoot(kids)
	return v, nil
}

// OpenConfigured is Open over the store the c4 CLI is configured to use:
// C4_STORE, or the store lines of ~/.c4/config (see store.OpenStore).
func OpenConfigured(id c4.ID, opts ...Option) (*View, error) {
	s, err := store.OpenStore()
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrNoStore
	}
	return Open(s, id, opts...)
}

func newView(src store.Source, opts []Option) *View {
	v := &View{src: src}
	for _, o := range opts {
		o(v)
	}
	root := "/"
	v.cwd.Store(&root)
	return v
}

// setRoot makes the root directory from its children.
func (v *View) setRoot(kids []*node) { v.root = newRoot(kids) }

// newRoot makes a root directory node. Its size and time follow c4m's
// nil-infectious rule over the top-level entries: the sum and the
// latest, unless any of them is null.
func newRoot(kids []*node) *node {
	e := &c4m.Entry{Name: "/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp()}
	var size int64
	var latest time.Time
	nullSize, nullTime := false, false
	for _, k := range kids {
		if k.entry.Size < 0 {
			nullSize = true
		} else {
			size += k.entry.Size
		}
		if t := k.entry.Timestamp; t.Equal(c4m.NullTimestamp()) {
			nullTime = true
		} else if t.After(latest) {
			latest = t
		}
	}
	if !nullSize {
		e.Size = size
	}
	if !nullTime && len(kids) > 0 {
		e.Timestamp = latest
	}
	return &node{entry: e, dir: loadedDir(kids)}
}

// derive returns a View of the tree at root that reads content from
// src and shares v's cache.
func (v *View) derive(root *node, src store.Source) *View {
	nv := newView(src, nil)
	nv.cache = v.cache
	nv.root = root
	return nv
}

func (v *View) cacheFor(create bool) (*Cache, error) {
	if v.cache != nil {
		return v.cache, nil
	}
	return sharedDefault(create)
}

// abs makes name absolute against the working directory and cleans it.
func (v *View) abs(name string) string {
	if !path.IsAbs(name) {
		name = path.Join(*v.cwd.Load(), name)
	}
	return path.Clean(name)
}

// lookup resolves name to a node. Symbolic links met on the way are
// followed, and so is the last element when follow is set. Relative
// link targets resolve against the link's directory; absolute ones
// against the View's root.
func (v *View) lookup(op, name string, follow bool) (*node, error) {
	if name == "" {
		return nil, pathErr(op, name, fs.ErrNotExist)
	}
	p := v.abs(name)
	hops := 0
restart:
	cur, dir := v.root, "/"
	rest := strings.TrimPrefix(p, "/")
	for rest != "" {
		elem := rest
		rest = ""
		if i := strings.IndexByte(elem, '/'); i >= 0 {
			elem, rest = elem[:i], elem[i+1:]
		}
		if cur.dir == nil {
			return nil, pathErr(op, name, errNotDir)
		}
		kids, err := v.kids(cur)
		if err != nil {
			return nil, pathErr(op, name, err)
		}
		i, ok := find(kids, elem)
		if !ok {
			return nil, pathErr(op, name, fs.ErrNotExist)
		}
		child := kids[i]
		if child.isSymlink() && (rest != "" || follow) {
			if hops++; hops > MaxSymlinks {
				return nil, pathErr(op, name, errLoop)
			}
			target := child.entry.Target
			if !path.IsAbs(target) {
				target = path.Join(dir, target)
			}
			p = path.Join(target, rest)
			goto restart
		}
		cur, dir = child, path.Join(dir, elem)
	}
	return cur, nil
}

func (v *View) info(n *node, name string) *fileInfo {
	return &fileInfo{v: v, n: n, name: path.Base(name)}
}

// Stat returns the FileInfo of name, following symbolic links. Its Sys
// method returns the *c4m.Entry it describes.
func (v *View) Stat(name string) (os.FileInfo, error) {
	n, err := v.lookup("stat", name, true)
	if err != nil {
		return nil, err
	}
	return v.info(n, name), nil
}

// Lstat is Stat without following a final symbolic link.
func (v *View) Lstat(name string) (os.FileInfo, error) {
	n, err := v.lookup("lstat", name, false)
	if err != nil {
		return nil, err
	}
	return v.info(n, name), nil
}

// Readlink returns the target of the symbolic link name.
func (v *View) Readlink(name string) (string, error) {
	n, err := v.lookup("readlink", name, false)
	if err != nil {
		return "", err
	}
	if !n.isSymlink() {
		return "", pathErr("readlink", name, errInvalid)
	}
	return n.entry.Target, nil
}

// Open opens name for reading.
func (v *View) Open(name string) (absfs.File, error) {
	return v.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile opens name. Any flag that would write (O_WRONLY, O_RDWR,
// O_APPEND, O_CREATE, O_TRUNC) fails: with EEXIST for O_CREATE|O_EXCL
// on an existing name, EISDIR for a directory, the lookup error for a
// missing name without O_CREATE, and ErrReadOnly otherwise.
func (v *View) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC) != 0
	n, err := v.lookup("open", name, true)
	if err != nil {
		if write && flag&os.O_CREATE != 0 && notExist(err) {
			return nil, v.denyCreate("open", name)
		}
		return nil, err
	}
	if write {
		switch {
		case flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
			return nil, pathErr("open", name, fs.ErrExist)
		case n.dir != nil:
			return nil, pathErr("open", name, errIsDir)
		}
		return nil, pathErr("open", name, ErrReadOnly)
	}
	return &file{v: v, n: n, name: name}, nil
}

// Create fails: the View is read-only.
func (v *View) Create(name string) (absfs.File, error) {
	return v.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

// ReadDir returns the entries of the directory name, sorted by name,
// following symbolic links. A directory known only by its C4 ID loads
// its record from the store on first use.
func (v *View) ReadDir(name string) ([]fs.DirEntry, error) {
	n, err := v.lookup("readdir", name, true)
	if err != nil {
		return nil, err
	}
	if n.dir == nil {
		return nil, pathErr("readdir", name, errNotDir)
	}
	kids, err := v.kids(n)
	if err != nil {
		return nil, pathErr("readdir", name, err)
	}
	out := make([]fs.DirEntry, len(kids))
	for i, k := range kids {
		out[i] = &fileInfo{v: v, n: k, name: k.name}
	}
	return out, nil
}

// ReadFile returns the whole content of name.
func (v *View) ReadFile(name string) ([]byte, error) {
	f, err := v.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Sub returns an io/fs view of the subtree at dir.
func (v *View) Sub(dir string) (fs.FS, error) {
	return absfs.FilerToFS(v, v.abs(dir))
}

// Chdir sets the directory relative names resolve against. It changes
// no data, so unlike the mutating methods it succeeds. The working
// directory is the View's own, shared by every user of the View
// (workspaces, unionfs lowers, NFS goroutines): one caller's Chdir
// changes how relative names resolve for all of them, so code that
// shares a View should use absolute paths.
func (v *View) Chdir(dir string) error {
	n, err := v.lookup("chdir", dir, true)
	if err != nil {
		return err
	}
	if n.dir == nil {
		return pathErr("chdir", dir, errNotDir)
	}
	p := v.abs(dir)
	v.cwd.Store(&p)
	return nil
}

// Getwd returns the working directory set by Chdir, "/" initially.
func (v *View) Getwd() (string, error) {
	return *v.cwd.Load(), nil
}

// TempDir returns "/tmp". The View cannot create anything there; it is
// reported only to satisfy absfs.FileSystem.
func (v *View) TempDir() string { return "/tmp" }

// The mutating methods below all fail. Where the operating system would
// report something more specific than a read-only filesystem (a missing
// name, an existing one), they report that, as a read-only mount does.

// denyExisting fails a mutation of an existing name.
func (v *View) denyExisting(op, name string, follow bool) error {
	if _, err := v.lookup(op, name, follow); err != nil {
		return err
	}
	return pathErr(op, name, ErrReadOnly)
}

// denyCreate fails the creation of a new name.
func (v *View) denyCreate(op, name string) error {
	if name == "" {
		return pathErr(op, name, fs.ErrNotExist)
	}
	n, err := v.lookup(op, path.Dir(v.abs(name)), true)
	if err != nil {
		return pathErr(op, name, errors.Unwrap(err))
	}
	if n.dir == nil {
		return pathErr(op, name, errNotDir)
	}
	return pathErr(op, name, ErrReadOnly)
}

// Mkdir fails: EEXIST if name exists, ErrReadOnly otherwise.
func (v *View) Mkdir(name string, perm os.FileMode) error {
	if _, err := v.lookup("mkdir", name, false); err == nil {
		return pathErr("mkdir", name, fs.ErrExist)
	}
	return v.denyCreate("mkdir", name)
}

// MkdirAll succeeds if name is already a directory, as os.MkdirAll does,
// and otherwise fails.
func (v *View) MkdirAll(name string, perm os.FileMode) error {
	n, err := v.lookup("mkdir", name, true)
	switch {
	case err == nil && n.dir != nil:
		return nil
	case err == nil:
		return pathErr("mkdir", name, errNotDir)
	}
	return pathErr("mkdir", name, ErrReadOnly)
}

// Remove fails.
func (v *View) Remove(name string) error { return v.denyExisting("remove", name, false) }

// RemoveAll succeeds if name does not exist, as os.RemoveAll does, and
// otherwise fails.
func (v *View) RemoveAll(name string) error {
	if _, err := v.lookup("removeall", name, false); err != nil {
		if notExist(err) {
			return nil
		}
		return err
	}
	return pathErr("removeall", name, ErrReadOnly)
}

// Rename fails.
func (v *View) Rename(oldpath, newpath string) error {
	return v.denyExisting("rename", oldpath, false)
}

// Chmod fails.
func (v *View) Chmod(name string, mode os.FileMode) error {
	return v.denyExisting("chmod", name, true)
}

// Chtimes fails.
func (v *View) Chtimes(name string, atime, mtime time.Time) error {
	return v.denyExisting("chtimes", name, true)
}

// Chown fails.
func (v *View) Chown(name string, uid, gid int) error {
	return v.denyExisting("chown", name, true)
}

// Lchown fails.
func (v *View) Lchown(name string, uid, gid int) error {
	return v.denyExisting("lchown", name, false)
}

// Truncate fails.
func (v *View) Truncate(name string, size int64) error {
	return v.denyExisting("truncate", name, true)
}

// Symlink fails: EEXIST if newname exists, ErrReadOnly otherwise.
func (v *View) Symlink(oldname, newname string) error {
	if _, err := v.lookup("symlink", newname, false); err == nil {
		return pathErr("symlink", newname, fs.ErrExist)
	}
	return v.denyCreate("symlink", newname)
}
