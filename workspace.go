package c4fs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// Workspace is a writable absfs filesystem layered over a View: the
// View's tree, plus every change made since. The View is never
// modified, so any number of workspaces can share one.
//
// A Workspace keeps its changes in two places. The content of files
// written since it was made lives in a scratch filesystem the caller
// supplies (an osfs or basefs folder, or a memfs), one scratch file per
// written file. Everything else (names created, removed and renamed,
// modes, times, symbolic links) is an in-memory change set over the
// View. A file is copied from the View into scratch the first time it
// is opened for writing, truncated or appended to; a new file is
// written straight to scratch; a change of mode or time copies nothing.
//
// Nothing is hashed as it is written. NFSv3 has no close, and a build
// makes many short-lived files, so content is identified only when
// Capture records the workspace: then the files written since the last
// capture are hashed and put into a c4 store, and the change comes back
// as a c4m patch and a new manifest.
//
// A Workspace is safe for concurrent use.
type Workspace struct {
	v       *View
	scratch absfs.FileSystem
	tag     string // prefixes scratch file names
	now     func() time.Time
	cwd     atomic.Pointer[string]

	mu     sync.RWMutex
	root   *wnode
	seq    uint64
	closed bool

	// adopted maps each View node the workspace has taken into its
	// change set to the workspace node standing for it, so a handle
	// opened on the View node sees later changes.
	adopted map[*node]*wnode

	// The state the last Capture recorded, or the View's own before
	// the first: the tree, its c4m ID, and the stores holding its
	// content. headOK is false until the View's ID has been computed.
	head    *node
	headID  c4.ID
	headSrc store.Source
	headOK  bool
}

var _ absfs.SymlinkFileSystem = (*Workspace)(nil)

// kind is what a workspace node is.
type kind uint8

const (
	kindFile kind = iota
	kindDir
	kindLink
)

// wnode is a name the workspace has changed, or a directory on the way
// to one. Names the workspace has not touched are not represented: they
// fall through to the View.
type wnode struct {
	name   string
	parent *wnode // nil for the root, or once removed
	kind   kind

	// entry holds the node's current metadata: mode, time, a link's
	// target and, for a file whose content is not dirty, its C4 ID and
	// size. Its name and depth are not kept up to date.
	entry c4m.Entry

	// base is the View node this one stands for, if any: for a
	// directory, where the children it has not changed come from; for
	// a file not yet copied to scratch, where its content is.
	base *node

	// Files.
	scratch  string        // scratch file holding the content, or ""
	dirty    bool          // content written since it was last hashed
	opens    int           // open handles on the scratch file
	copying  chan struct{} // closed when an in-flight copy-up ends
	detached bool          // removed from the tree

	// noPerm marks a file whose mode was set to 0. c4m records that as
	// its null mode, which reads back as 0644, but the workspace itself
	// reports what the caller set.
	noPerm bool

	// Directories: the children this directory has changed. A nil value
	// is a whiteout, hiding a child of base.
	kids map[string]*wnode

	// stale marks a node changed since the last Capture; cap is the
	// node that Capture built for it, reused while it is not stale. A
	// stale node's parent is stale too.
	stale bool
	cap   *node
}

// ref is a name in the workspace's merged tree: either a node the
// workspace has changed, or a View node beneath no change.
type ref struct {
	w *wnode
	b *node
}

func (r ref) isDir() bool {
	if r.w != nil {
		return r.w.kind == kindDir
	}
	return r.b.dir != nil
}

func (r ref) isLink() bool {
	if r.w != nil {
		return r.w.kind == kindLink
	}
	return r.b.isSymlink()
}

func (r ref) target() string {
	if r.w != nil {
		return r.w.entry.Target
	}
	return r.b.entry.Target
}

// chmodBits are the mode bits Chmod sets, as in package os.
const chmodBits = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// NewWorkspace returns a writable workspace over base, keeping the
// content of written files in scratch. scratch should be dedicated to
// workspaces: each workspace names its files with a random prefix, so
// several may share one, but nothing else should write there.
func NewWorkspace(base *View, scratch absfs.FileSystem) (*Workspace, error) {
	if base == nil {
		return nil, errors.New("c4fs: NewWorkspace needs a base View")
	}
	if scratch == nil {
		return nil, errors.New("c4fs: NewWorkspace needs a scratch filesystem")
	}
	var tag [6]byte
	if _, err := rand.Read(tag[:]); err != nil {
		return nil, err
	}
	w := &Workspace{
		v:       base,
		scratch: scratch,
		tag:     "/c4fs-" + hex.EncodeToString(tag[:]),
		now:     time.Now,
		head:    base.root,
		headSrc: base.src,
		adopted: map[*node]*wnode{},
	}
	if id := base.id.Load(); id != nil {
		w.headID, w.headOK = *id, true
	}
	w.root = &wnode{kind: kindDir, base: base.root, entry: *base.root.entry, kids: map[string]*wnode{}}
	root := "/"
	w.cwd.Store(&root)
	return w, nil
}

// Base returns the View the workspace was made over. For a workspace
// made by Resume, that is the View Resume built (its base with the chain
// applied), not the base the chain starts from: keep that base to
// resume the whole chain again.
func (w *Workspace) Base() *View { return w.v }

// Head returns the C4 ID of the state the last Capture recorded, which
// the next Capture's patch will apply to: the base View's ID until the
// first Capture.
func (w *Workspace) Head() (c4.ID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.initHead(); err != nil {
		return c4.ID{}, err
	}
	return w.headID, nil
}

func (w *Workspace) initHead() error {
	if w.headOK {
		return nil
	}
	id, err := w.v.ID()
	if err != nil {
		return err
	}
	w.headID, w.headOK = id, true
	return nil
}

// Close removes the workspace's scratch files and makes every later
// operation fail with os.ErrClosed. Handles still open keep working
// until they are closed; their scratch files are removed then. Capture
// first: what Close removes is not recoverable.
func (w *Workspace) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	w.detach(w.root)
	return nil
}

// abs makes name absolute against the working directory and cleans it.
func (w *Workspace) abs(name string) string {
	if !path.IsAbs(name) {
		name = path.Join(*w.cwd.Load(), name)
	}
	return path.Clean(name)
}

// walk resolves name in the merged tree. Symbolic links met on the way
// are followed, and so is the last element when follow is set. It
// returns what name is and its physical path: absolute, clean, and free
// of symbolic links. The caller holds w.mu.
func (w *Workspace) walk(op, name string, follow bool) (ref, string, error) {
	if w.closed {
		return ref{}, "", pathErr(op, name, os.ErrClosed)
	}
	if name == "" {
		return ref{}, "", pathErr(op, name, fs.ErrNotExist)
	}
	p := w.abs(name)
	hops := 0
restart:
	cur, dir := ref{w: w.root}, "/"
	rest := strings.TrimPrefix(p, "/")
	for rest != "" {
		elem := rest
		rest = ""
		if i := strings.IndexByte(elem, '/'); i >= 0 {
			elem, rest = elem[:i], elem[i+1:]
		}
		if !cur.isDir() {
			return ref{}, "", pathErr(op, name, errNotDir)
		}
		child, err := w.child(cur, elem)
		if err != nil {
			return ref{}, "", pathErr(op, name, err)
		}
		if child.isLink() && (rest != "" || follow) {
			if hops++; hops > MaxSymlinks {
				return ref{}, "", pathErr(op, name, errLoop)
			}
			target := child.target()
			if target == "" {
				return ref{}, "", pathErr(op, name, fs.ErrNotExist)
			}
			if !path.IsAbs(target) {
				target = path.Join(dir, target)
			}
			p = path.Join(target, rest)
			goto restart
		}
		cur, dir = child, path.Join(dir, elem)
	}
	return cur, dir, nil
}

// child looks name up in a directory of the merged tree.
func (w *Workspace) child(dir ref, name string) (ref, error) {
	if d := dir.w; d != nil {
		if k, ok := d.kids[name]; ok {
			if k == nil {
				return ref{}, fs.ErrNotExist
			}
			return ref{w: k}, nil
		}
		if d.base == nil {
			return ref{}, fs.ErrNotExist
		}
		return w.baseChild(d.base, name)
	}
	return w.baseChild(dir.b, name)
}

func (w *Workspace) baseChild(dir *node, name string) (ref, error) {
	kids, err := w.v.kids(dir)
	if err != nil {
		return ref{}, err
	}
	i, ok := find(kids, name)
	if !ok {
		return ref{}, fs.ErrNotExist
	}
	return ref{b: kids[i]}, nil
}

// parent resolves the directory that holds name, following symbolic
// links, and returns it with its physical path and name's last element.
// For the root the element is "".
func (w *Workspace) parent(op, name string) (ref, string, string, error) {
	if w.closed {
		return ref{}, "", "", pathErr(op, name, os.ErrClosed)
	}
	if name == "" {
		return ref{}, "", "", pathErr(op, name, fs.ErrNotExist)
	}
	p := w.abs(name)
	if p == "/" {
		return ref{w: w.root}, "/", "", nil
	}
	d, phys, err := w.walk(op, path.Dir(p), true)
	if err != nil {
		return ref{}, "", "", pathErr(op, name, errors.Unwrap(err))
	}
	if !d.isDir() {
		return ref{}, "", "", pathErr(op, name, errNotDir)
	}
	return d, phys, path.Base(p), nil
}

// materialize returns the workspace node for a physical path walk just
// resolved, adopting View nodes on the way as needed.
func (w *Workspace) materialize(phys string) *wnode {
	cur := w.root
	for _, elem := range strings.Split(strings.TrimPrefix(phys, "/"), "/") {
		if elem == "" {
			continue
		}
		k, ok := cur.kids[elem]
		if !ok {
			// walk found elem, so the View has it.
			r, _ := w.baseChild(cur.base, elem)
			k = w.adopt(r.b, elem, cur)
			cur.kids[elem] = k
		}
		cur = k
	}
	return cur
}

// adopt makes a workspace node standing for a View node. A View node is
// adopted at most once: its name is the only way to reach it, and from
// then on that name leads to the workspace node.
func (w *Workspace) adopt(b *node, name string, parent *wnode) *wnode {
	n := &wnode{name: name, parent: parent, base: b, entry: *b.entry}
	w.adopted[b] = n
	n.entry.Depth = 0
	switch {
	case b.dir != nil:
		n.kind = kindDir
		n.kids = map[string]*wnode{}
	case b.isSymlink():
		n.kind = kindLink
	}
	return n
}

// touch marks n and its ancestors changed since the last Capture.
func (w *Workspace) touch(n *wnode) {
	for x := n; x != nil; x = x.parent {
		if x.stale && x != n {
			return
		}
		x.stale = true
	}
}

// dirChanged records a change to a directory's names: its time moves to
// now, as a directory's modification time does on a real filesystem.
func (w *Workspace) dirChanged(d *wnode) {
	d.entry.Timestamp = w.now()
	w.touch(d)
}

// insert adds a new node to a directory.
func (w *Workspace) insert(dir, n *wnode) {
	n.parent = dir
	dir.kids[n.name] = n
	w.dirChanged(dir)
	w.touch(n)
}

// unlink removes name from dir: a whiteout if dir's View directory has
// the name, otherwise simply forgotten. The removed node is detached,
// releasing its scratch content, unless keep is set (a rename).
func (w *Workspace) unlink(dir *wnode, name string, keep bool) {
	old := dir.kids[name]
	if dir.base != nil && w.baseHas(dir.base, name) {
		dir.kids[name] = nil
	} else {
		delete(dir.kids, name)
	}
	if old == nil {
		return
	}
	old.parent = nil
	if !keep {
		w.detach(old)
	}
}

// baseHas reports whether a View directory has name. If its record
// cannot be loaded it may, so the answer is yes: a needless whiteout is
// harmless, a missing one would let the name reappear.
func (w *Workspace) baseHas(dir *node, name string) bool {
	_, err := w.baseChild(dir, name)
	return !notExist(err)
}

// detach marks a removed subtree and releases the scratch content of
// every file in it that no handle holds open.
func (w *Workspace) detach(n *wnode) {
	n.detached = true
	switch n.kind {
	case kindFile:
		if n.opens == 0 {
			w.dropScratch(n)
		}
	case kindDir:
		for _, k := range n.kids {
			if k != nil {
				w.detach(k)
			}
		}
	}
}

func (w *Workspace) dropScratch(n *wnode) {
	if n.scratch != "" {
		w.scratch.Remove(n.scratch)
		n.scratch = ""
	}
}

func (w *Workspace) newScratchPath() string {
	w.seq++
	return w.tag + "-" + itoa(w.seq)
}

func itoa(n uint64) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		if n /= 10; n == 0 {
			return string(b[i:])
		}
	}
}

// isEmpty reports whether a directory of the merged tree has no names.
func (w *Workspace) isEmpty(d ref) (bool, error) {
	if d.b != nil {
		kids, err := w.v.kids(d.b)
		return len(kids) == 0, err
	}
	n := d.w
	for _, k := range n.kids {
		if k != nil {
			return false, nil
		}
	}
	if n.base == nil {
		return true, nil
	}
	kids, err := w.v.kids(n.base)
	if err != nil {
		return false, err
	}
	for _, k := range kids {
		if _, ok := n.kids[k.name]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// current returns what r stands for now: a View node the workspace has
// since adopted, because it or something beneath it changed, is the
// workspace node that took it over. The caller holds w.mu.
func (w *Workspace) current(r ref) ref {
	if r.b != nil {
		if n := w.adopted[r.b]; n != nil {
			return ref{w: n}
		}
	}
	return r
}

// info describes a name of the merged tree.
func (w *Workspace) info(r ref, name string) os.FileInfo {
	if r.b != nil {
		return w.v.info(r.b, name)
	}
	return w.nodeInfo(r.w, path.Base(name))
}

// nodeInfo describes a workspace node. The caller holds w.mu.
func (w *Workspace) nodeInfo(n *wnode, name string) *wsInfo {
	e := n.entry
	e.Depth = 0
	fi := &wsInfo{name: name, mode: modeOf(e.Mode, n.kind == kindDir, n.kind == kindLink), mtime: e.Timestamp, sys: &e}
	if n.noPerm {
		fi.mode &= fs.ModeType
	}
	switch n.kind {
	case kindDir:
		e.Name = n.name + "/"
		fi.size = max(e.Size, 0)
	case kindLink:
		e.Name = n.name
		fi.size = int64(len(e.Target))
	default:
		e.Name = n.name
		switch {
		case n.scratch != "":
			if st, err := w.scratch.Stat(n.scratch); err == nil {
				fi.size = st.Size()
			}
		case e.Size >= 0:
			fi.size = e.Size
		case n.base != nil:
			fi.size = w.v.size(n.base)
		}
		if n.dirty {
			e.C4ID, e.Size = c4.ID{}, fi.size
		}
	}
	return fi
}

// list returns the entries of a directory of the merged tree, sorted
// by name. The caller holds w.mu.
func (w *Workspace) list(d ref) ([]fs.DirEntry, error) {
	if d.b != nil {
		kids, err := w.v.kids(d.b)
		if err != nil {
			return nil, err
		}
		out := make([]fs.DirEntry, len(kids))
		for i, k := range kids {
			out[i] = &fileInfo{v: w.v, n: k, name: k.name}
		}
		return out, nil
	}
	n := d.w
	var base []*node
	if n.base != nil {
		var err error
		if base, err = w.v.kids(n.base); err != nil {
			return nil, err
		}
	}
	out := make([]fs.DirEntry, 0, len(base)+len(n.kids))
	for _, b := range base {
		if _, ok := n.kids[b.name]; !ok {
			out = append(out, &fileInfo{v: w.v, n: b, name: b.name})
		}
	}
	for name, k := range n.kids {
		if k != nil {
			out = append(out, w.nodeInfo(k, name))
		}
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return out, nil
}

// Stat returns the FileInfo of name, following symbolic links. Its Sys
// method returns a *c4m.Entry: the View's own for an unchanged name,
// and for a changed one a snapshot of its current metadata, with a null
// C4 ID while its content has not been hashed.
func (w *Workspace) Stat(name string) (os.FileInfo, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	r, _, err := w.walk("stat", name, true)
	if err != nil {
		return nil, err
	}
	return w.info(r, name), nil
}

// Lstat is Stat without following a final symbolic link.
func (w *Workspace) Lstat(name string) (os.FileInfo, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	r, _, err := w.walk("lstat", name, false)
	if err != nil {
		return nil, err
	}
	return w.info(r, name), nil
}

// Readlink returns the target of the symbolic link name.
func (w *Workspace) Readlink(name string) (string, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	r, _, err := w.walk("readlink", name, false)
	if err != nil {
		return "", err
	}
	if !r.isLink() {
		return "", pathErr("readlink", name, errInvalid)
	}
	return r.target(), nil
}

// ReadDir returns the entries of the directory name, sorted by name,
// following symbolic links.
func (w *Workspace) ReadDir(name string) ([]fs.DirEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	r, _, err := w.walk("readdir", name, true)
	if err != nil {
		return nil, err
	}
	if !r.isDir() {
		return nil, pathErr("readdir", name, errNotDir)
	}
	ents, err := w.list(r)
	if err != nil {
		return nil, pathErr("readdir", name, err)
	}
	return ents, nil
}

// ReadFile returns the whole content of name.
func (w *Workspace) ReadFile(name string) ([]byte, error) {
	f, err := w.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Sub returns an io/fs view of the subtree at dir.
func (w *Workspace) Sub(dir string) (fs.FS, error) {
	return absfs.FilerToFS(w, w.abs(dir))
}

// Chdir sets the directory relative names resolve against.
func (w *Workspace) Chdir(dir string) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	r, _, err := w.walk("chdir", dir, true)
	if err != nil {
		return err
	}
	if !r.isDir() {
		return pathErr("chdir", dir, errNotDir)
	}
	p := w.abs(dir)
	w.cwd.Store(&p)
	return nil
}

// Getwd returns the working directory set by Chdir, "/" initially.
func (w *Workspace) Getwd() (string, error) { return *w.cwd.Load(), nil }

// TempDir returns "/tmp". It need not exist.
func (w *Workspace) TempDir() string { return "/tmp" }

// Open opens name for reading.
func (w *Workspace) Open(name string) (absfs.File, error) {
	return w.OpenFile(name, os.O_RDONLY, 0)
}

// Create creates or truncates name, as os.Create does.
func (w *Workspace) Create(name string) (absfs.File, error) {
	return w.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

// accessWrite are the access modes that let a handle write.
const accessWrite = os.O_WRONLY | os.O_RDWR

// OpenFile opens name with os.OpenFile's flags. perm is a new file's
// mode, applied as given (no umask). Opening a file of the View for
// writing (O_WRONLY or O_RDWR), or with O_TRUNC, copies it to scratch
// first (O_TRUNC copies nothing); opening it only to read does not, even
// with O_CREATE or O_APPEND, which a read-only handle cannot use. A
// read-only handle on a file that has not been copied to scratch reads
// the View's content even if the file is written through another handle
// later, as with overlayfs.
func (w *Workspace) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	if flag&(accessWrite|os.O_TRUNC|os.O_CREATE) == 0 {
		return w.openRead(name)
	}
	return w.openWrite(name, flag, perm, -1)
}

func (w *Workspace) openRead(name string) (absfs.File, error) {
	w.mu.RLock()
	r, _, err := w.walk("open", name, true)
	if err != nil || r.w == nil || r.isDir() {
		w.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		return w.handle(r, name), nil
	}
	w.mu.RUnlock()
	// A changed file: its handle may hold a scratch file open, which
	// is counted, so take the write lock and look again.
	w.mu.Lock()
	defer w.mu.Unlock()
	if r, _, err = w.walk("open", name, true); err != nil {
		return nil, err
	}
	if r.w == nil || r.isDir() {
		return w.handle(r, name), nil
	}
	return w.openNode(r.w, name, os.O_RDONLY)
}

// handle returns a handle on a directory, or on a View file.
func (w *Workspace) handle(r ref, name string) absfs.File {
	if r.isDir() {
		return &wsFile{w: w, name: name, dir: &r}
	}
	return &wsFile{w: w, name: name, b: r.b, inner: &file{v: w.v, n: r.b, name: name}}
}

// openNode opens a changed file. The caller holds w.mu for writing.
func (w *Workspace) openNode(n *wnode, name string, flag int) (absfs.File, error) {
	if n.scratch == "" {
		// Only metadata changed: read the View's content.
		return &wsFile{w: w, name: name, n: n, inner: &file{v: w.v, n: n.base, name: name}}, nil
	}
	inner, err := w.scratch.OpenFile(n.scratch, flag&^(os.O_CREATE|os.O_EXCL), 0)
	if err != nil {
		return nil, pathErr("open", name, scratchErr(err))
	}
	n.opens++
	return &wsFile{w: w, name: name, n: n, inner: inner, counted: true, write: flag&accessWrite != 0}, nil
}

// scratchErr unwraps a scratch filesystem's path error, so the error a
// caller sees names the workspace path rather than the scratch file.
func scratchErr(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

// openWrite opens name with flags that write or create. limit bounds
// how much of a View file a copy-up copies (-1 for all), for Truncate.
func (w *Workspace) openWrite(name string, flag int, perm os.FileMode, limit int64) (absfs.File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		r, phys, err := w.walk("open", name, true)
		if err != nil {
			if flag&os.O_CREATE != 0 && notExist(err) {
				return w.create(name, flag, perm)
			}
			return nil, err
		}
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			return nil, pathErr("open", name, fs.ErrExist)
		}
		if r.isDir() {
			return nil, pathErr("open", name, errIsDir)
		}
		if flag&(accessWrite|os.O_TRUNC) == 0 {
			// O_CREATE on a file that exists, read-only: an open to read.
			if r.w == nil {
				return w.handle(r, name), nil
			}
			return w.openNode(r.w, name, os.O_RDONLY)
		}
		n := w.materialize(phys)
		if n.scratch == "" {
			if ch := n.copying; ch != nil {
				w.mu.Unlock()
				if testHookCopyWait != nil {
					testHookCopyWait()
				}
				<-ch
				w.mu.Lock()
				continue
			}
			if err := w.copyUp(n, flag&os.O_TRUNC != 0, limit); err != nil {
				return nil, pathErr("open", name, err)
			}
			// The lock was released for the copy: look again.
			continue
		}
		if flag&os.O_TRUNC != 0 {
			n.dirty = true
			n.entry.Timestamp = w.now()
			w.touch(n)
		}
		return w.openNode(n, name, flag)
	}
}

// testHookCopyWait, if set, is called when an opener starts waiting for
// another's copy-up, without the lock. Tests use it to order events.
var testHookCopyWait func()

// create makes a new file for OpenFile with O_CREATE. A dangling
// symbolic link at name is followed and its target created, as os does,
// unless O_EXCL is set.
func (w *Workspace) create(name string, flag int, perm os.FileMode) (absfs.File, error) {
	p := w.abs(name)
	for hops := 0; ; hops++ {
		d, phys, base, err := w.parent("open", p)
		if err != nil {
			return nil, pathErr("open", name, errors.Unwrap(err))
		}
		if base == "" {
			return nil, pathErr("open", name, errIsDir)
		}
		c, err := w.child(d, base)
		if err == nil {
			if !c.isLink() || flag&os.O_EXCL != 0 {
				return nil, pathErr("open", name, fs.ErrExist)
			}
			if hops >= MaxSymlinks {
				// Unreachable while w.mu is held: walk followed this
				// same chain within MaxSymlinks before create was
				// called. Kept so the loop is bounded on its own.
				return nil, pathErr("open", name, errLoop)
			}
			target := c.target()
			if !path.IsAbs(target) {
				target = path.Join(phys, target)
			}
			p = path.Clean(target)
			continue
		}
		if !notExist(err) {
			return nil, pathErr("open", name, err)
		}
		sp := w.newScratchPath()
		f, err := w.scratch.OpenFile(sp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, pathErr("open", name, scratchErr(err))
		}
		f.Close()
		dir := w.materialize(phys)
		n := &wnode{name: base, kind: kindFile, scratch: sp, dirty: true, noPerm: perm&chmodBits == 0, entry: c4m.Entry{
			Mode:      perm & chmodBits,
			Timestamp: w.now(),
			Size:      -1,
		}}
		w.insert(dir, n)
		return w.openNode(n, name, flag&^os.O_TRUNC)
	}
}

// copyUp copies a View file's content into a new scratch file. It is
// called, and returns, with w.mu held, but releases it for the copy;
// concurrent openers wait on n.copying. trunc copies nothing; limit, if
// not negative, copies at most that many bytes.
func (w *Workspace) copyUp(n *wnode, trunc bool, limit int64) error {
	ch := make(chan struct{})
	n.copying = ch
	src, sp := n.base, w.newScratchPath()
	n.opens++ // so a removal meanwhile does not drop what is being copied
	w.mu.Unlock()
	err := w.fill(sp, src, trunc, limit)
	w.mu.Lock()
	n.copying = nil
	close(ch)
	n.opens--
	if err != nil {
		return err
	}
	n.scratch = sp
	if trunc || limit >= 0 {
		// The copy is not the View's content: whoever looks next must
		// see it changed, even if this opener never gets to write.
		n.dirty = true
		n.entry.Timestamp = w.now()
		w.touch(n)
	}
	if n.detached && n.opens == 0 {
		w.dropScratch(n)
	}
	return nil
}

func (w *Workspace) fill(sp string, src *node, trunc bool, limit int64) error {
	f, err := w.scratch.OpenFile(sp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return scratchErr(err)
	}
	if !trunc && limit != 0 {
		var c *content
		if c, err = w.v.openContent(src); err == nil {
			size := c.size
			if limit >= 0 && limit < size {
				size = limit
			}
			_, err = io.Copy(f, io.NewSectionReader(c.ra, 0, size))
			c.close()
		}
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		w.scratch.Remove(sp)
	}
	return err
}

// wrote records a write through a handle on n.
func (w *Workspace) wrote(n *wnode) {
	w.mu.Lock()
	n.dirty = true
	n.entry.Timestamp = w.now()
	w.touch(n)
	w.mu.Unlock()
}

// release drops a handle's hold on n's scratch file.
func (w *Workspace) release(n *wnode) {
	w.mu.Lock()
	n.opens--
	if n.detached && n.opens == 0 {
		w.dropScratch(n)
	}
	w.mu.Unlock()
}

// Truncate changes the size of name, following symbolic links. A file
// of the View is copied to scratch first, at most size bytes of it.
func (w *Workspace) Truncate(name string, size int64) error {
	if size < 0 {
		return pathErr("truncate", name, errInvalid)
	}
	flag := os.O_WRONLY
	if size == 0 {
		flag |= os.O_TRUNC
	}
	f, err := w.openWrite(name, flag, 0, size)
	if err != nil {
		return relabel(err, "truncate")
	}
	err = f.Truncate(size)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// relabel renames the operation of a path error.
func relabel(err error, op string) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pathErr(op, pe.Path, pe.Err)
	}
	return err
}

// Mkdir creates the directory name. A directory made where the View had
// one that was removed starts empty: the View's children do not return.
func (w *Workspace) Mkdir(name string, perm os.FileMode) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mkdir(name, perm)
}

func (w *Workspace) mkdir(name string, perm os.FileMode) error {
	d, phys, base, err := w.parent("mkdir", name)
	if err != nil {
		return err
	}
	if base == "" {
		return pathErr("mkdir", name, fs.ErrExist)
	}
	if _, err := w.child(d, base); err == nil {
		return pathErr("mkdir", name, fs.ErrExist)
	} else if !notExist(err) {
		return pathErr("mkdir", name, err)
	}
	n := &wnode{name: base, kind: kindDir, kids: map[string]*wnode{}, entry: c4m.Entry{
		Mode:      fs.ModeDir | perm&chmodBits,
		Timestamp: w.now(),
		Size:      -1,
	}}
	w.insert(w.materialize(phys), n)
	return nil
}

// MkdirAll creates name and any missing parents, as os.MkdirAll does,
// and reports what it would: ENOTDIR for a file, a link to one or a
// path through one; EEXIST for a link that cannot be followed
// (dangling, a loop, or a link to a path through a file). Errors name
// the path as given, or the parent of it that failed.
func (w *Workspace) MkdirAll(name string, perm os.FileMode) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return pathErr("mkdir", name, os.ErrClosed)
	}
	if name == "" {
		return pathErr("mkdir", name, fs.ErrNotExist)
	}
	return w.mkdirAll(name, perm)
}

// mkdirAll takes os.MkdirAll's steps: a fast path for a name that
// resolves, then the parent, then mkdir of the name itself, whose error
// it reports. The caller holds w.mu.
func (w *Workspace) mkdirAll(name string, perm os.FileMode) error {
	r, _, err := w.walk("mkdir", name, true)
	if err == nil {
		if r.isDir() {
			return nil
		}
		return pathErr("mkdir", name, errNotDir)
	}
	if parent := parentPath(name); parent != "" {
		if err := w.mkdirAll(parent, perm); err != nil {
			return err
		}
	}
	err = w.mkdir(name, perm)
	if err != nil {
		// A name like "new/.": mkdir finds it made, as os.MkdirAll's
		// Lstat does.
		if r, _, lerr := w.walk("mkdir", name, false); lerr == nil && r.isDir() {
			return nil
		}
	}
	return err
}

// Remove removes the file, symbolic link or empty directory name. A
// directory is empty when the merged view of it is: it may still hold
// names in the View, all of them removed.
func (w *Workspace) Remove(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, phys, base, err := w.parent("remove", name)
	if err != nil {
		return err
	}
	if base == "" {
		return pathErr("remove", name, errBusy)
	}
	c, err := w.child(d, base)
	if err != nil {
		return pathErr("remove", name, err)
	}
	if c.isDir() {
		empty, err := w.isEmpty(c)
		if err != nil {
			return pathErr("remove", name, err)
		}
		if !empty {
			return pathErr("remove", name, errNotEmpty)
		}
	}
	dir := w.materialize(phys)
	w.unlink(dir, base, false)
	w.dirChanged(dir)
	return nil
}

// RemoveAll removes name and everything below it, as os.RemoveAll does;
// a missing name is not an error. Removing a directory of the View is
// one whiteout, whatever its size.
func (w *Workspace) RemoveAll(name string) error {
	if name == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	d, phys, base, err := w.parent("removeall", name)
	if err != nil {
		if notExist(err) {
			return nil
		}
		return err
	}
	if base == "" {
		return pathErr("removeall", name, errBusy)
	}
	if _, err := w.child(d, base); err != nil {
		if notExist(err) {
			return nil
		}
		return pathErr("removeall", name, err)
	}
	dir := w.materialize(phys)
	w.unlink(dir, base, false)
	w.dirChanged(dir)
	return nil
}

// Rename renames oldpath to newpath, as os.Rename does: a file replaces
// a file or link at newpath, and a directory replaces an empty
// directory. Renaming a directory moves its whole subtree, changes and
// all, in constant time; its old name is hidden.
func (w *Workspace) Rename(oldpath, newpath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	fail := func(err error) error {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err}
	}
	od, ophys, oname, err := w.parent("rename", oldpath)
	if err != nil {
		return fail(err)
	}
	nd, nphys, nname, err := w.parent("rename", newpath)
	if err != nil {
		return fail(err)
	}
	if oname == "" || nname == "" {
		return fail(errBusy)
	}
	src, err := w.child(od, oname)
	if err != nil {
		return fail(err)
	}
	oldFull, newFull := path.Join(ophys, oname), path.Join(nphys, nname)
	if oldFull == newFull {
		return nil
	}
	dst, err := w.child(nd, nname)
	exists := err == nil
	if err != nil && !notExist(err) {
		return fail(err)
	}
	if src.isDir() {
		if strings.HasPrefix(newFull, oldFull+"/") {
			return fail(errInvalid)
		}
		if exists {
			if !dst.isDir() {
				return fail(errNotDir)
			}
			empty, err := w.isEmpty(dst)
			if err != nil {
				return fail(err)
			}
			if !empty {
				return fail(errNotEmpty)
			}
		}
	} else if exists && dst.isDir() {
		return fail(errIsDir)
	}
	odir, ndir := w.materialize(ophys), w.materialize(nphys)
	n := src.w
	if n == nil {
		n = w.adopt(src.b, oname, odir)
	}
	w.unlink(odir, oname, true)
	if exists {
		w.unlink(ndir, nname, false)
	}
	n.name = nname
	w.insert(ndir, n)
	w.dirChanged(odir)
	return nil
}

// Symlink creates newname as a symbolic link to oldname.
func (w *Workspace) Symlink(oldname, newname string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	fail := func(err error) error {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: err}
	}
	d, phys, base, err := w.parent("symlink", newname)
	if err != nil {
		return fail(err)
	}
	if base == "" {
		return fail(fs.ErrExist)
	}
	if _, err := w.child(d, base); err == nil {
		return fail(fs.ErrExist)
	} else if !notExist(err) {
		return fail(err)
	}
	n := &wnode{name: base, kind: kindLink, entry: c4m.Entry{
		Mode:      fs.ModeSymlink | 0o777,
		Timestamp: w.now(),
		Size:      int64(len(oldname)),
		Target:    oldname,
	}}
	w.insert(w.materialize(phys), n)
	return nil
}

// change applies a metadata change to name, following symbolic links
// when follow is set. It copies no content.
func (w *Workspace) change(op, name string, follow bool, set func(*wnode)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, phys, err := w.walk(op, name, follow)
	if err != nil {
		return err
	}
	n := w.materialize(phys)
	set(n)
	w.touch(n)
	return nil
}

// Chmod changes the mode of name, following symbolic links. Only the
// change is recorded; a file's content is not copied. A file's mode 0
// is c4m's null mode: the workspace reports it as 0, but a captured
// manifest, and a View or workspace made from it, reads it as 0644.
func (w *Workspace) Chmod(name string, mode os.FileMode) error {
	return w.change("chmod", name, true, func(n *wnode) {
		typ := modeOf(n.entry.Mode, n.kind == kindDir, n.kind == kindLink) & fs.ModeType
		n.entry.Mode = typ | mode&chmodBits
		n.noPerm = n.kind == kindFile && mode&chmodBits == 0
	})
}

// Chtimes changes the modification time of name, following symbolic
// links. c4m records no access time, so atime is ignored. A zero mtime
// leaves the time as it is, as os.Chtimes does.
func (w *Workspace) Chtimes(name string, atime, mtime time.Time) error {
	if mtime.IsZero() {
		return w.exists("chtimes", name, true)
	}
	return w.change("chtimes", name, true, func(n *wnode) { n.entry.Timestamp = mtime })
}

// exists checks that name resolves, for an operation that records
// nothing.
func (w *Workspace) exists(op, name string, follow bool) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, _, err := w.walk(op, name, follow)
	return err
}

// Chown checks that name exists and otherwise does nothing: c4m records
// no ownership.
func (w *Workspace) Chown(name string, uid, gid int) error {
	return w.exists("chown", name, true)
}

// Lchown is Chown without following a final symbolic link.
func (w *Workspace) Lchown(name string, uid, gid int) error {
	return w.exists("lchown", name, false)
}
