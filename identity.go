package c4fs

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

// item is one c4m entry in a directory's listing: a node, or a folded
// sequence standing for its member nodes.
type item struct {
	e       *c4m.Entry // the node's entry, or the folded sequence entry
	n       *node      // nil for a folded sequence
	members []*node    // a folded sequence's members, in name order
}

func (it item) isDir() bool { return it.n != nil && it.n.dir != nil }

// listItems returns a directory's children as c4m lists them: members of
// a sequence folded back into the sequence entry they came from, files
// before directories, and names in c4m's natural order. A sequence some
// of whose members are gone (a patch replaced one, say) is listed as
// its remaining members.
func listItems(kids []*node) []item {
	out := make([]item, 0, len(kids))
	var groups map[*c4m.Entry]int
	for _, k := range kids {
		if k.seq == nil {
			out = append(out, item{e: k.entry, n: k})
			continue
		}
		if i, ok := groups[k.seq]; ok {
			out[i].members = append(out[i].members, k)
			continue
		}
		if groups == nil {
			groups = make(map[*c4m.Entry]int)
		}
		groups[k.seq] = len(out)
		out = append(out, item{e: k.seq, members: []*node{k}})
	}
	if groups != nil {
		out = unfoldPartial(out)
	}
	sortItems(out)
	return out
}

// listing returns a directory node's listItems, sorting them the first
// time and remembering them: a node never changes, and c4m's natural
// order is not cheap to sort by.
func (v *View) listing(n *node) ([]item, error) {
	if l := n.dir.items.Load(); l != nil {
		return *l, nil
	}
	kids, err := v.kids(n)
	if err != nil {
		return nil, err
	}
	items := listItems(kids)
	n.dir.items.Store(&items)
	return items, nil
}

// unfoldPartial lists an incomplete sequence's members one by one.
func unfoldPartial(items []item) []item {
	out := items[:0:0]
	for _, it := range items {
		if it.n != nil || len(it.members) == seqLen(it.e) {
			out = append(out, it)
			continue
		}
		for _, m := range it.members {
			out = append(out, item{e: m.entry, n: m})
		}
	}
	return out
}

// seqLen is how many member files a folded sequence entry stands for.
func seqLen(e *c4m.Entry) int {
	names, err := c4m.ExpandSequencePattern(e.Name)
	if err != nil {
		return 0
	}
	return len(names)
}

// sortItems orders a listing as c4m does: files before directories,
// then natural order by name.
func sortItems(items []item) {
	slices.SortFunc(items, func(a, b item) int { return compareEntries(a.e, b.e) })
}

func compareEntries(a, b *c4m.Entry) int {
	if a.Name == b.Name {
		return 0
	}
	if ad, bd := a.IsDir(), b.IsDir(); ad != bd {
		if ad {
			return 1
		}
		return -1
	}
	switch {
	case naturalLess(a.Name, b.Name):
		return -1
	case naturalLess(b.Name, a.Name):
		return 1
	}
	return strings.Compare(a.Name, b.Name)
}

// naturalLess is c4m.NaturalLess without its allocations: names split
// into runs of digits and of other characters, digit runs compare as
// integers of any length (equal values, shorter spelling first), other
// runs as strings, text before digits, and fewer runs first. c4m splits
// by rune, replacing invalid UTF-8; for such names it is used itself.
func naturalLess(a, b string) bool {
	if !utf8.ValidString(a) || !utf8.ValidString(b) {
		return c4m.NaturalLess(a, b)
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		ea, da := run(a, i)
		eb, db := run(b, j)
		ta, tb := a[i:ea], b[j:eb]
		switch {
		case da && db:
			if c := compareDigitRuns(ta, tb); c != 0 {
				return c < 0
			}
		case da != db:
			return db
		case ta != tb:
			return ta < tb
		}
		i, j = ea, eb
	}
	return i == len(a) && j < len(b)
}

// run returns the end of the run of digits, or of non-digits, starting
// at s[i], and whether it is digits.
func run(s string, i int) (int, bool) {
	digit := isDigit(s[i])
	for i++; i < len(s) && isDigit(s[i]) == digit; i++ {
	}
	return i, digit
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

// compareDigitRuns orders runs of digits as c4m does: by value, then
// the shorter spelling first.
func compareDigitRuns(a, b string) int {
	ta, tb := strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
	switch {
	case len(ta) != len(tb):
		return cmpInt(len(ta), len(tb))
	case ta != tb:
		return strings.Compare(ta, tb)
	}
	return cmpInt(len(a), len(b))
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// canonical is one entry of a directory listing as c4m identifies it: the
// entry, with a directory's null size and time resolved, and its
// canonical c4m line.
type canonical struct {
	e    *c4m.Entry
	line string
}

// canonEntry returns the entry c4m identifies a node by: its own, except
// that a directory's null size and time are resolved from its children
// as c4m.PropagateMetadata resolves them. The result is remembered.
func (v *View) canonEntry(n *node) (*c4m.Entry, error) {
	e := n.entry
	if n.dir == nil || (e.Size >= 0 && !isNullTime(e)) {
		return e, nil
	}
	if c := n.canon.Load(); c != nil {
		return c, nil
	}
	items, err := v.listing(n)
	if err != nil {
		return nil, err
	}
	lines, err := v.canonListing(items)
	if err != nil {
		return nil, err
	}
	r := *e
	resolveDirMeta(&r, lines)
	n.canon.Store(&r)
	return &r, nil
}

// canonListing returns the canonical entries and lines of a listing.
// A node's line is formatted once and remembered: formatting a C4 ID
// is not cheap.
func (v *View) canonListing(items []item) ([]canonical, error) {
	out := make([]canonical, len(items))
	for i, it := range items {
		if it.n == nil {
			out[i] = canonical{it.e, canonLine(it.e)}
			continue
		}
		if l := it.n.line.Load(); l != nil {
			out[i] = canonical{l.e, l.line}
			continue
		}
		e, err := v.canonEntry(it.n)
		if err != nil {
			return nil, err
		}
		out[i] = canonical{e, canonLine(e)}
		it.n.line.Store(&out[i])
	}
	return out, nil
}

func isNullTime(e *c4m.Entry) bool { return e.Timestamp.Equal(c4m.NullTimestamp()) }

// resolveDirMeta fills a directory entry's null size and time from its
// children's canonical entries, as c4m.PropagateMetadata does: an empty
// directory is 0 bytes with a null time; otherwise the size is the sum
// of the children's sizes and of their canonical c4m lines, and the time
// is the latest child's, each null if any child's is null.
func resolveDirMeta(e *c4m.Entry, kids []canonical) {
	if e.Size < 0 {
		e.Size = dirSize(kids)
	}
	if isNullTime(e) {
		e.Timestamp = dirTime(kids)
	}
}

func dirSize(kids []canonical) int64 {
	var sum int64
	for _, k := range kids {
		if k.e.Size < 0 {
			return -1
		}
		sum += k.e.Size + int64(len(k.line)) + 1
	}
	return sum
}

func dirTime(kids []canonical) time.Time {
	null := c4m.NullTimestamp()
	if len(kids) == 0 {
		return null
	}
	var latest time.Time
	for _, k := range kids {
		if isNullTime(k.e) {
			return null
		}
		if k.e.Timestamp.After(latest) {
			latest = k.e.Timestamp
		}
	}
	return latest
}

// listingID is the C4 ID of a directory listing: its canonical c4m
// lines, which is how c4 identifies a directory and, for the top level,
// a whole manifest (c4m.Manifest.ComputeC4ID).
func listingID(lines []canonical) c4.ID {
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l.line)
		buf.WriteByte('\n')
	}
	return c4.Identify(&buf)
}

// ID returns the C4 ID of the tree the View presents, as c4 computes it
// for a manifest (c4m.Manifest.ComputeC4ID): the ID of the canonical
// listing of its top-level entries. It is the ID `c4 diff` writes as a
// patch's base and closing lines, and the ID of the root directory
// record `c4 id -s` stores, which Open accepts. Directories with null
// sizes or times are resolved from their children first, which loads
// their records if they have not been visited. The result is remembered.
func (v *View) ID() (c4.ID, error) {
	if id := v.id.Load(); id != nil {
		return *id, nil
	}
	id, err := v.nodeID(v.root)
	if err != nil {
		return c4.ID{}, err
	}
	v.id.Store(&id)
	return id, nil
}

// nodeID is the listing ID of a directory node's children.
func (v *View) nodeID(dir *node) (c4.ID, error) {
	items, err := v.listing(dir)
	if err != nil {
		return c4.ID{}, err
	}
	lines, err := v.canonListing(items)
	if err != nil {
		return c4.ID{}, err
	}
	return listingID(lines), nil
}

// Manifest returns the whole tree the View presents as a c4m manifest,
// in c4m order, with sequences folded as they were in the source and
// their ID lists as range data where the members' IDs are known.
// Entries are copies; the View is unaffected by changes to them. A tree
// opened by a directory record loads every record it has not yet
// visited, so this reads the whole tree.
func (v *View) Manifest() (*c4m.Manifest, error) {
	m := c4m.NewManifest()
	if err := v.appendTree(m, v.root, 0); err != nil {
		return nil, err
	}
	return m, nil
}

func (v *View) appendTree(m *c4m.Manifest, dir *node, depth int) error {
	items, err := v.listing(dir)
	if err != nil {
		return err
	}
	for _, it := range items {
		e := *it.e
		e.Depth = depth
		m.Entries = append(m.Entries, &e)
		if it.n == nil {
			addRangeData(m, it)
			continue
		}
		if it.n.dir != nil {
			if err := v.appendTree(m, it.n, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// addRangeData records a folded sequence's member IDs inline, as c4m
// range data, when they are all known and hash to the sequence's ID.
func addRangeData(m *c4m.Manifest, it item) {
	seq := it.e
	if seq.C4ID.IsNil() || len(it.members) < 2 {
		return
	}
	names, err := c4m.ExpandSequencePattern(seq.Name)
	if err != nil || len(names) != len(it.members) {
		return
	}
	byName := make(map[string]c4.ID, len(it.members))
	for _, k := range it.members {
		byName[k.name] = k.entry.C4ID
	}
	var b strings.Builder
	for _, name := range names {
		id := byName[name]
		if id.IsNil() {
			return
		}
		b.WriteString(id.String())
	}
	text := b.String()
	if c4.Identify(strings.NewReader(text)) != seq.C4ID {
		return
	}
	if m.RangeData == nil {
		m.RangeData = make(map[c4.ID]string)
	}
	m.RangeData[seq.C4ID] = text
}

// canonLine is e.Canonical(), with the C4 ID formatted by idString
// rather than c4.ID.String, which divides a big.Int 88 times. c4m puts
// the ID (or "-" for none) last on the line, so the rest is c4m's own.
func canonLine(e *c4m.Entry) string {
	if e.C4ID.IsNil() {
		return e.Canonical()
	}
	cp := *e
	cp.C4ID = c4.ID{}
	line := cp.Canonical()
	return line[:len(line)-1] + idString(e.C4ID)
}

// idString formats a C4 ID exactly as c4.ID.String does: "c4" and the
// ID's 512 bits as 88 base-58 digits, most significant first, padded
// with the zero digit '1'. It divides by 58^10 in 64-bit limbs.
func idString(id c4.ID) string {
	const (
		charset = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
		chunk   = 430804206899405824 // 58^10, the largest power of 58 in a uint64
	)
	var limbs [8]uint64
	for i := range limbs {
		limbs[i] = binary.BigEndian.Uint64(id[i*8:])
	}
	var out [90]byte
	out[0], out[1] = 'c', '4'
	for pos := 89; pos > 1; {
		var rem uint64
		for i := range limbs {
			limbs[i], rem = bits.Div64(rem, limbs[i], chunk)
		}
		for k := 0; k < 10 && pos > 1; k++ {
			out[pos] = charset[rem%58]
			rem /= 58
			pos--
		}
	}
	return string(out[:])
}
