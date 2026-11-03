package c4fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// Changeset is what one Capture recorded: the change from the previous
// state (the base View, or the last capture) to the workspace as it was
// captured, as a c4m patch, and that new state as a whole.
//
// A changeset can be kept two ways, and both need only what it holds.
// Appended to the c4m file of the state it applies to, it extends that
// file's patch chain, which c4 log lists and c4 patch resolves; kept as
// a file of its own, it is a changeset that Resume, View.Apply, or
// Open (once stored) applies to its base. Either way, WriteTo writes it.
type Changeset struct {
	// Base is the C4 ID of the state the patch applies to: the patch's
	// first line. It is the previous capture's ID, or the base View's.
	Base c4.ID

	// ID is the C4 ID of the captured state (c4m.Manifest.ComputeC4ID
	// of Manifest): the patch's closing line.
	ID c4.ID

	// Patch holds the patch entries, nested, in c4m's patch semantics
	// as c4m.PatchDiff produces them: a new name adds, an entry
	// identical to the base's removes it, and any other entry replaces
	// it, a directory's children following it. It is empty when nothing
	// changed.
	Patch *c4m.Manifest

	// Files and Bytes count the files Capture hashed and put into the
	// store, and their content.
	Files int
	Bytes int64

	view *View
}

// Empty reports whether nothing changed.
func (c *Changeset) Empty() bool { return len(c.Patch.Entries) == 0 }

// WriteTo writes the changeset as c4 diff does: the base ID on a line of
// its own, the patch entries in canonical c4m form, and the closing ID.
// An empty changeset writes nothing, as c4 diff writes nothing for
// identical trees, so that appending it to a chain is a no-op.
func (c *Changeset) WriteTo(w io.Writer) (int64, error) {
	if c.Empty() {
		return 0, nil
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s\n", c.Base)
	if err := c4m.NewEncoder(&buf).Encode(c.Patch); err != nil {
		return 0, err
	}
	fmt.Fprintf(&buf, "%s\n", c.ID)
	return buf.WriteTo(w)
}

// Text returns what WriteTo writes.
func (c *Changeset) Text() []byte {
	var buf bytes.Buffer
	c.WriteTo(&buf)
	return buf.Bytes()
}

// View returns a read-only View of the captured state. It reads the
// content of files captured from the store Capture was given, and the
// rest from the base's store.
func (c *Changeset) View() *View { return c.view }

// Manifest returns the captured state as a whole c4m manifest. It walks
// the whole tree (see View.Manifest), so a caller that needs only the
// patch need not pay for it.
func (c *Changeset) Manifest() (*c4m.Manifest, error) { return c.view.Manifest() }

// Capture records the workspace into st and returns the change since
// the last Capture (or since the base View, the first time).
//
// Only files written since the last Capture are read: each is hashed as
// it is put into st, so content is read once. Files whose content did
// not change since then, including every file of the View and those
// whose only change is metadata, keep the C4 ID already known. Then the
// changed directories are identified, bottom up, as c4 identifies them,
// and the patch is the difference from the last captured state, found
// by walking only what changed.
//
// After Capture the workspace carries on unchanged: its files, open
// handles and scratch content are as they were. What moves is its head:
// the next Capture's patch applies to this capture's state, so a series
// of captures is a patch chain, each section relative to the one before
// (see Head). Capture holds the workspace's lock throughout, so writers
// wait for it; capture when the work is at a quiet point.
//
// If Capture fails part way, nothing is lost: content already put stays
// in the store, and the next Capture picks up where this one stopped.
func (w *Workspace) Capture(st store.Store) (*Changeset, error) {
	if st == nil {
		return nil, ErrNoStore
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, os.ErrClosed
	}
	if err := w.initHead(); err != nil {
		return nil, err
	}
	c := &capturer{w: w, st: st}
	root, err := c.build(w.root)
	if err != nil {
		return nil, err
	}
	patch := c4m.NewManifest()
	id := w.headID
	if root != w.head {
		if err := c.diff(w.head, root, 0, patch); err != nil {
			return nil, err
		}
		if id, err = w.v.nodeID(root); err != nil {
			return nil, err
		}
	}
	src := joinSources(w.headSrc, st)
	view := w.v.derive(root, src)
	view.id.Store(&id)
	cs := &Changeset{Base: w.headID, ID: id, Patch: patch, Files: c.files, Bytes: c.bytes, view: view}
	w.head, w.headID, w.headSrc = root, id, src
	return cs, nil
}

// capturer builds the captured tree: View nodes for what changed,
// sharing every node that did not.
type capturer struct {
	w     *Workspace
	st    store.Store
	files int
	bytes int64
}

// build returns the node for n's current state, reusing what the last
// Capture built if n has not changed since, and n's View node if n
// turns out to be no change at all.
func (c *capturer) build(n *wnode) (*node, error) {
	if !n.stale && n.cap != nil {
		return n.cap, nil
	}
	var out *node
	var err error
	switch n.kind {
	case kindDir:
		out, err = c.buildDir(n)
	case kindLink:
		out = c.buildLink(n)
	default:
		out, err = c.buildFile(n)
	}
	if err != nil {
		return nil, err
	}
	n.cap, n.stale = out, false
	return out, nil
}

func (c *capturer) buildFile(n *wnode) (*node, error) {
	if n.scratch != "" && n.dirty {
		id, size, err := c.put(n.scratch)
		if err != nil {
			return nil, fmt.Errorf("capture %s: %w", n.name, err)
		}
		n.entry.C4ID, n.entry.Size = id, size
		n.entry.HardLink = 0 // its content is its own now
		n.dirty = false
	}
	e := n.entry
	e.Name, e.Depth = n.name, 0
	e.Timestamp = c.stamp(n)
	if c.reuse(n, &e) {
		return n.base, nil
	}
	if e.Size < 0 && n.scratch == "" && n.base != nil && !e.C4ID.IsNil() {
		// A sequence member's size is not in its folded entry.
		e.Size = c.w.v.size(n.base)
	}
	return &node{name: n.name, entry: &e}, nil
}

func (c *capturer) buildLink(n *wnode) *node {
	e := n.entry
	e.Name, e.Depth = n.name, 0
	e.Timestamp = c.stamp(n)
	if c.reuse(n, &e) {
		return n.base
	}
	return &node{name: n.name, entry: &e}
}

// reuse reports whether n is exactly its View node, still in place.
func (c *capturer) reuse(n *wnode, e *c4m.Entry) bool {
	b := n.base
	if b == nil {
		return false
	}
	if b.seq != nil && b.entry.Size < 0 && e.Size >= 0 {
		// A sequence member's size is not in the folded entry: compare
		// the size its content has.
		if e.Size != c.w.v.size(b) {
			return false
		}
		cp := *e
		cp.Size = b.entry.Size
		e = &cp
	}
	if !identical(e, b.entry) {
		return false
	}
	if b.seq == nil {
		return true
	}
	// A sequence member folds back into its sequence only where it
	// came from.
	if n.parent == nil || n.parent.base == nil {
		return false
	}
	r, err := c.w.baseChild(n.parent.base, n.name)
	return err == nil && r.b == b
}

// stamp is the time Capture records for n: the View's, if n still has
// it, and otherwise n's time cut to the whole seconds c4m keeps.
func (c *capturer) stamp(n *wnode) time.Time {
	ts := n.entry.Timestamp
	if n.base != nil && ts.Equal(n.base.entry.Timestamp) {
		return n.base.entry.Timestamp
	}
	return ts.Truncate(time.Second).UTC()
}

func (c *capturer) buildDir(n *wnode) (*node, error) {
	w := c.w
	var base []*node
	if n.base != nil {
		var err error
		if base, err = w.v.kids(n.base); err != nil {
			return nil, err
		}
	}
	kids := make([]*node, 0, len(base)+len(n.kids))
	for _, b := range base {
		if _, ok := n.kids[b.name]; !ok {
			kids = append(kids, b)
		}
	}
	for _, k := range n.kids {
		if k == nil {
			continue
		}
		bn, err := c.build(k)
		if err != nil {
			return nil, err
		}
		kids = append(kids, bn)
	}
	slices.SortFunc(kids, func(a, b *node) int { return strings.Compare(a.name, b.name) })
	kids = c.unfoldBroken(kids)
	unchanged := n.base != nil && slices.Equal(kids, base)
	if n == w.root {
		if unchanged {
			return n.base, nil
		}
		return newRoot(kids), nil
	}
	if unchanged && n.name == n.base.name && n.entry.Mode == n.base.entry.Mode &&
		n.entry.Timestamp.Equal(n.base.entry.Timestamp) {
		return n.base, nil
	}
	items := listItems(kids)
	lines, err := w.v.canonListing(items)
	if err != nil {
		return nil, err
	}
	mode := n.entry.Mode
	if mode != 0 {
		mode |= os.ModeDir
	}
	e := &c4m.Entry{Name: n.name + "/", Mode: mode, Timestamp: c.stamp(n), Size: -1, C4ID: listingID(lines)}
	resolveDirMeta(e, lines)
	d := loadedDir(kids)
	d.items.Store(&items)
	return &node{name: n.name, entry: e, dir: d}, nil
}

// unfoldBroken turns the members of a sequence that is no longer whole
// in a directory (one was changed, removed or moved) into plain files,
// with their sizes, so the directory lists them one by one.
func (c *capturer) unfoldBroken(kids []*node) []*node {
	var count map[*c4m.Entry]int
	for _, k := range kids {
		if k.seq != nil {
			if count == nil {
				count = make(map[*c4m.Entry]int)
			}
			count[k.seq]++
		}
	}
	if count == nil {
		return kids
	}
	broken := false
	for seq, n := range count {
		if n != seqLen(seq) {
			broken = true
			break
		}
	}
	if !broken {
		return kids
	}
	out := make([]*node, len(kids))
	for i, k := range kids {
		out[i] = k
		if k.seq == nil || count[k.seq] == seqLen(k.seq) {
			continue
		}
		e := *k.entry
		if e.Size < 0 && !e.C4ID.IsNil() {
			e.Size = c.w.v.size(k)
		}
		out[i] = &node{name: k.name, entry: &e}
	}
	return out
}

// smallFile is the largest file put reads whole into memory: it is
// hashed there, skipped if the store has it, and otherwise written with
// the store's Create, so no store needs to stage it in a temporary file
// to learn its ID. Larger files stream through the store's Put.
const smallFile = 1 << 20

// put stores a scratch file's content, hashing it as it goes. The ID is
// always that of the bytes stored, even if a writer changes the file
// meanwhile.
func (c *capturer) put(p string) (c4.ID, int64, error) {
	f, err := c.w.scratch.Open(p)
	if err != nil {
		return c4.ID{}, 0, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, smallFile+1))
	if err != nil {
		return c4.ID{}, 0, err
	}
	if len(data) <= smallFile {
		id := c4.Identify(bytes.NewReader(data))
		if err := c.store(id, data); err != nil {
			return c4.ID{}, 0, err
		}
		c.files++
		c.bytes += int64(len(data))
		return id, int64(len(data)), nil
	}
	cr := &countingReader{r: io.MultiReader(bytes.NewReader(data), f)}
	id, err := c.st.Put(cr)
	if err != nil {
		return c4.ID{}, 0, err
	}
	c.files++
	c.bytes += cr.n
	return id, cr.n, nil
}

// store writes data, whose ID is id, unless the store has it already.
func (c *capturer) store(id c4.ID, data []byte) error {
	if c.st.Has(id) {
		return nil
	}
	w, err := c.st.Create(id)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil
		}
		return err
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.n += int64(n)
	return n, err
}

// diff appends the patch from directory old to directory nw, as
// c4m.PatchDiff would write it, at depth. Subtrees that are the same
// node in both are skipped without being visited.
func (c *capturer) diff(old, nw *node, depth int, patch *c4m.Manifest) error {
	v := c.w.v
	a, err := v.listing(old)
	if err != nil {
		return err
	}
	b, err := v.listing(nw)
	if err != nil {
		return err
	}
	for i, j := 0, 0; i < len(a) || j < len(b); {
		cmp := 0
		switch {
		case i == len(a):
			cmp = 1
		case j == len(b):
			cmp = -1
		default:
			cmp = compareEntries(a[i].e, b[j].e)
		}
		switch {
		case cmp < 0: // removed: the old entry, exactly
			emit(patch, a[i].e, depth)
			i++
		case cmp > 0: // added, with everything below it
			if err := c.add(b[j], depth, patch); err != nil {
				return err
			}
			j++
		default:
			if err := c.change(a[i], b[j], depth, patch); err != nil {
				return err
			}
			i++
			j++
		}
	}
	return nil
}

func (c *capturer) change(o, n item, depth int, patch *c4m.Manifest) error {
	if o.n != nil && o.n == n.n {
		return nil
	}
	if o.n == nil && n.n == nil && slices.Equal(o.members, n.members) {
		return nil
	}
	if identical(o.e, n.e) {
		if !n.isDir() || !o.isDir() {
			return nil
		}
		// The same entry, different children: possible only where
		// the directory's C4 ID is null.
		sub := c4m.NewManifest()
		if err := c.diff(o.n, n.n, depth+1, sub); err != nil {
			return err
		}
		if len(sub.Entries) > 0 {
			emit(patch, n.e, depth)
			patch.Entries = append(patch.Entries, sub.Entries...)
		}
		return nil
	}
	emit(patch, n.e, depth)
	if o.isDir() && n.isDir() {
		return c.diff(o.n, n.n, depth+1, patch)
	}
	return nil
}

func (c *capturer) add(it item, depth int, patch *c4m.Manifest) error {
	emit(patch, it.e, depth)
	if it.n == nil {
		addRangeData(patch, it)
		return nil
	}
	if it.n.dir != nil {
		return c.w.v.appendTree(patch, it.n, depth+1)
	}
	return nil
}

func emit(patch *c4m.Manifest, e *c4m.Entry, depth int) {
	cp := *e
	cp.Depth = depth
	patch.Entries = append(patch.Entries, &cp)
}

// ErrBaseMismatch reports a patch whose base ID is not the ID of the
// tree it is applied to.
var ErrBaseMismatch = errors.New("c4fs: patch does not apply to this tree")
