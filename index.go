package c4fs

import (
	"bytes"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

// emptyID is the C4 ID of zero bytes: an empty file, and the ID c4m
// gives an empty directory.
var emptyID = c4.Identify(bytes.NewReader(nil))

// maxBaseHops bounds how many external base references (first-line bare
// C4 IDs) are followed when resolving a patch chain.
const maxBaseHops = 256

// node is one name in the tree. Its fields are fixed once the View is
// built, apart from a directory's lazily loaded children and the memo.
type node struct {
	name  string     // base name, without a directory's trailing "/"
	entry *c4m.Entry // the c4m entry; shared, never modified
	dir   *dirState  // non-nil for directories
	seq   *c4m.Entry // for a sequence member, the folded entry it came from
	memo  atomic.Pointer[memo]

	// canon is a directory's entry with null size and time resolved
	// from its children, as c4m canonicalises them for identity; line
	// is the canonical entry and its c4m line, once formatted.
	canon atomic.Pointer[c4m.Entry]
	line  atomic.Pointer[canonical]
}

// memo caches what a node's content resolution learned.
type memo struct {
	size int64  // resolved size, or -1
	path string // a local file holding the content, or ""
}

// dirState holds a directory's children. A directory listed with its
// children inline is loaded when built; one given only by C4 ID (a
// one-level directory record, as c4 id -s stores them) loads from the
// store on first use.
type dirState struct {
	mu     sync.Mutex
	loaded atomic.Bool
	kids   []*node // sorted by name, unique; immutable once loaded
	record c4.ID   // directory record to load from while !loaded

	items atomic.Pointer[[]item] // kids as c4m lists them, once sorted
}

func newNode(e *c4m.Entry) *node {
	n := &node{name: strings.TrimSuffix(e.Name, "/"), entry: e}
	if e.IsDir() {
		n.dir = &dirState{}
	}
	return n
}

func (n *node) isSymlink() bool {
	e := n.entry
	return n.dir == nil && (e.Mode&fs.ModeSymlink != 0 || e.Target != "")
}

// mode is the entry's mode with c4m's null mode replaced by a default:
// 0644 for files, 0755 for directories, 0777 for symlinks.
func (n *node) mode() fs.FileMode {
	return modeOf(n.entry.Mode, n.dir != nil, n.isSymlink())
}

// zeroMode is the bit c4 releases after v1.0.21 set for a known zero
// mode (c4m.ZeroMode, written "----------"), to tell it from the null
// mode. No os.FileMode bit uses it, so it is never reported.
const zeroMode fs.FileMode = 1 << 18

// modeOf presents a c4m mode, replacing c4m's null mode (0) with a
// default and making the type bits agree with what the name is. A known
// zero mode is reported as 0, with the type bits.
func modeOf(m fs.FileMode, dir, link bool) fs.FileMode {
	null := m == 0
	m &^= zeroMode
	switch {
	case dir:
		if null {
			return fs.ModeDir | 0o755
		}
		return m&^fs.ModeType | fs.ModeDir
	case link:
		if null {
			return fs.ModeSymlink | 0o777
		}
		return m&^fs.ModeType | fs.ModeSymlink
	case null:
		return 0o644
	}
	return m
}

func validName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		!strings.ContainsAny(name, "/\x00")
}

// buildRaw turns a depth-ordered c4m entry list into nodes. Each
// directory's children are left raw: in manifest order, possibly with
// duplicates and with sequence entries still folded. present finishes
// them.
func buildRaw(entries []*c4m.Entry) ([]*node, error) {
	top := &node{dir: &dirState{}}
	stack := []*node{top}
	for _, e := range entries {
		d := e.Depth
		if d < 0 || d >= len(stack) {
			return nil, fmt.Errorf("%w: entry %q at depth %d has no parent directory", ErrInvalidManifest, e.Name, d)
		}
		stack = stack[:d+1]
		n := newNode(e)
		if !validName(n.name) {
			return nil, fmt.Errorf("%w: bad name %q", ErrInvalidManifest, e.Name)
		}
		parent := stack[d].dir
		parent.kids = append(parent.kids, n)
		if n.dir != nil {
			stack = append(stack, n)
		}
	}
	return top.dir.kids, nil
}

// present finishes a raw child list: sequences expand into their member
// files, names sort, a repeated name keeps its last entry (c4m's rule),
// and every directory below is finished the same way.
func (v *View) present(raw []*node, rd map[c4.ID]string) []*node {
	kids := make([]*node, 0, len(raw))
	for _, n := range raw {
		if n.dir == nil && n.entry.IsSequence {
			if members := v.expandSequence(n.entry, rd); members != nil {
				kids = append(kids, members...)
				continue
			}
		}
		if n.dir != nil {
			v.finishDir(n, rd)
		}
		kids = append(kids, n)
	}
	return sortUnique(kids)
}

// finishDir presents a directory's inline children. A directory with
// none is empty if its ID says so (null, or the empty ID) and is
// otherwise a one-level record to load from the store on first use.
func (v *View) finishDir(n *node, rd map[c4.ID]string) {
	d := n.dir
	if len(d.kids) > 0 {
		d.kids = v.present(d.kids, rd)
		d.loaded.Store(true)
		return
	}
	d.kids = nil
	if id := n.entry.C4ID; !id.IsNil() && id != emptyID {
		d.record = id
		return
	}
	d.loaded.Store(true)
}

// sortUnique sorts by name; among equal names the last one wins.
func sortUnique(kids []*node) []*node {
	slices.SortStableFunc(kids, func(a, b *node) int { return strings.Compare(a.name, b.name) })
	out := kids[:0]
	for i, n := range kids {
		if i+1 < len(kids) && kids[i+1].name == n.name {
			continue
		}
		out = append(out, n)
	}
	clear(kids[len(out):])
	return slices.Clip(out)
}

func find(kids []*node, name string) (int, bool) {
	return slices.BinarySearchFunc(kids, name, func(n *node, name string) int {
		return strings.Compare(n.name, name)
	})
}

// kids returns a directory's children, loading its record on first use.
// A failed load is not remembered, so a transient store error can heal.
func (v *View) kids(n *node) ([]*node, error) {
	d := n.dir
	if d.loaded.Load() {
		return d.kids, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.loaded.Load() {
		return d.kids, nil
	}
	kids, err := v.fromID(d.record, 0)
	if err != nil {
		return nil, fmt.Errorf("directory record %s: %w", d.record, err)
	}
	d.kids = kids
	d.loaded.Store(true)
	return kids, nil
}

// chain is a manifest as stored: an optional external base and one or
// more sections of entries, each after the first a patch over the state
// before it.
type chain struct {
	base     c4.ID
	sections [][]*c4m.Entry
	rd       map[c4.ID]string // inline sequence ID lists
}

// fromChain builds the children of the tree a chain resolves to. Each
// section is applied in turn as a patch, the first over the external
// base when there is one.
func (v *View) fromChain(ch *chain, hops int) ([]*node, error) {
	sections := ch.sections
	var kids []*node
	if ch.base.IsNil() {
		if len(sections) == 0 {
			return nil, nil
		}
		raw, err := buildRaw(sections[0])
		if err != nil {
			return nil, err
		}
		kids = v.present(raw, ch.rd)
		sections = sections[1:]
	} else {
		if hops >= maxBaseHops {
			return nil, fmt.Errorf("%w: more than %d base references", ErrInvalidManifest, maxBaseHops)
		}
		var err error
		if kids, err = v.fromID(ch.base, hops+1); err != nil {
			return nil, fmt.Errorf("patch base %s: %w", ch.base, err)
		}
	}
	for _, s := range sections {
		raw, err := buildRaw(s)
		if err != nil {
			return nil, err
		}
		if kids, err = v.patchKids(kids, raw, ch.rd); err != nil {
			return nil, err
		}
	}
	return kids, nil
}

// fromID loads a manifest or directory record from the store.
func (v *View) fromID(id c4.ID, hops int) ([]*node, error) {
	if id == emptyID {
		return nil, nil
	}
	ch, err := v.loadChain(id)
	if err != nil {
		return nil, err
	}
	return v.fromChain(ch, hops)
}

// patchKids applies raw patch entries to a directory's children with
// c4m's patch semantics: a new name is added, an entry identical to the
// current one removes it, and any other entry replaces it, a directory
// recursing into its children. Only directories a patch passes through
// are loaded. The patch is persistent: cur and every node below it are
// left as they were, and the result shares each subtree the patch does
// not touch.
func (v *View) patchKids(cur []*node, raw []*node, rd map[c4.ID]string) ([]*node, error) {
	kids := slices.Clone(cur)
	gone := make([]bool, len(kids))
	var added []*node
	for _, p := range uniqueRaw(raw) {
		if p.dir == nil && p.entry.IsSequence {
			matched, same := false, false
			for i, k := range kids {
				if k.seq != nil && k.seq.Name == p.entry.Name && !gone[i] {
					matched, same = true, identical(k.seq, p.entry)
					gone[i] = true
				}
			}
			if !matched || !same {
				added = append(added, v.present([]*node{p}, rd)...)
			}
			continue
		}
		// Patch entries match by c4m name, in which a directory's
		// trailing "/" counts: "x/" and "x" are different entries, so a
		// type change is an addition plus a removal, as in c4m.
		i, found := find(kids, p.name)
		if found {
			b := kids[i]
			found = !gone[i] && b.seq == nil && b.entry.Name == p.entry.Name
		}
		if !found {
			added = append(added, v.present([]*node{p}, rd)...)
			continue
		}
		b := kids[i]
		gone[i] = true
		if identical(b.entry, p.entry) {
			continue
		}
		if b.dir == nil || p.dir == nil {
			added = append(added, v.present([]*node{p}, rd)...)
			continue
		}
		bk, err := v.kids(b)
		if err != nil {
			return nil, err
		}
		nk, err := v.patchKids(bk, p.dir.kids, rd)
		if err != nil {
			return nil, err
		}
		added = append(added, &node{name: b.name, entry: p.entry, dir: loadedDir(nk)})
	}
	// Survivors of the base first and the patch's entries after, so that
	// where names still collide (a sequence member and a plain file) the
	// patch wins under sortUnique's last-wins rule.
	out := make([]*node, 0, len(kids)+len(added))
	for i, k := range kids {
		if !gone[i] {
			out = append(out, k)
		}
	}
	out = append(out, added...)
	return sortUnique(out), nil
}

// loadedDir returns the state of a directory whose children are known.
func loadedDir(kids []*node) *dirState {
	d := &dirState{kids: kids}
	d.loaded.Store(true)
	return d
}

// uniqueRaw drops all but the last of repeated c4m names, as c4m's own
// patch tree does.
func uniqueRaw(raw []*node) []*node {
	seen := make(map[string]int, len(raw))
	out := make([]*node, 0, len(raw))
	for _, n := range raw {
		key := n.entry.Name
		if i, ok := seen[key]; ok {
			out[i] = n
			continue
		}
		seen[key] = len(out)
		out = append(out, n)
	}
	return out
}

// identical is c4m's patch identity: every compared field equal.
func identical(a, b *c4m.Entry) bool {
	return a.Name == b.Name &&
		a.Mode == b.Mode &&
		a.Timestamp.Equal(b.Timestamp) &&
		a.Size == b.Size &&
		a.C4ID == b.C4ID &&
		a.Target == b.Target &&
		a.HardLink == b.HardLink &&
		a.FlowDirection == b.FlowDirection &&
		a.FlowTarget == b.FlowTarget
}

// expandSequence unfolds a sequence entry such as frame.[0001-0100].exr
// into one file per member. Member IDs come from the manifest's inline
// range data or the ID-list object in the store; if neither has it the
// members are listed with null IDs. A name that does not parse as a
// sequence returns nil and stays a plain file.
func (v *View) expandSequence(e *c4m.Entry, rd map[c4.ID]string) []*node {
	names, err := c4m.ExpandSequencePattern(e.Name)
	if err != nil || len(names) == 0 {
		return nil
	}
	ids := make([]c4.ID, len(names))
	if !e.C4ID.IsNil() {
		if list, err := v.idList(e.C4ID, rd); err == nil && len(list) == len(names) {
			ids = list
		}
	}
	out := make([]*node, len(names))
	for i, name := range names {
		size := int64(-1)
		if len(names) == 1 {
			size = e.Size
		}
		m := &c4m.Entry{
			Mode:      e.Mode,
			Timestamp: e.Timestamp,
			Size:      size,
			Name:      name,
			C4ID:      ids[i],
			Depth:     e.Depth,
		}
		out[i] = &node{name: name, entry: m, seq: e}
	}
	return out
}

// idList reads a sequence's member IDs: bare concatenated 90-character
// C4 IDs, whitespace ignored.
func (v *View) idList(id c4.ID, rd map[c4.ID]string) ([]c4.ID, error) {
	text, ok := rd[id]
	if !ok {
		data, err := v.readVerified(id)
		if err != nil {
			return nil, err
		}
		text = string(data)
	}
	text = strings.Join(strings.Fields(text), "")
	if len(text) == 0 || len(text)%90 != 0 {
		return nil, fmt.Errorf("%w: ID list %s", ErrNotManifest, id)
	}
	ids := make([]c4.ID, 0, len(text)/90)
	for i := 0; i < len(text); i += 90 {
		mid, err := c4.Parse(text[i : i+90])
		if err != nil {
			return nil, fmt.Errorf("%w: ID list %s: %v", ErrNotManifest, id, err)
		}
		ids = append(ids, mid)
	}
	return ids, nil
}
