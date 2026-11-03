package c4fs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
)

// Apply returns a View of the tree patch makes of v's. patch is what
// Changeset.WriteTo writes, or c4 diff: a base ID line, patch entries,
// and a closing ID line; or several of them, one after another, as a
// series of captures appended together. The first base ID must be v's
// (ErrBaseMismatch otherwise), each later one the state before it, and
// a closing ID the result's (ErrIDMismatch otherwise). An empty patch
// leaves the tree as it is.
//
// The new View reads content from v's store and then from src, which
// holds what the patch added (src may be nil if v's store has it). v is
// unchanged, and the new View shares every subtree the patch does not
// touch.
func (v *View) Apply(patch io.Reader, src store.Source) (*View, error) {
	data, err := io.ReadAll(patch)
	if err != nil {
		return nil, err
	}
	sections, err := c4m.DecodePatchChain(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotManifest, err)
	}
	nv := v.derive(v.root, joinSources(v.src, src))
	cur, err := v.ID()
	if err != nil {
		return nil, err
	}
	kids, err := v.kids(v.root)
	if err != nil {
		return nil, err
	}
	rd := rangeData(data)
	for i, s := range sections {
		if s.BaseID.IsNil() {
			return nil, fmt.Errorf("%w: section %d has no base ID line", ErrNotManifest, i+1)
		}
		if s.BaseID != cur {
			return nil, fmt.Errorf("%w: section %d applies to %s, the tree is %s", ErrBaseMismatch, i+1, s.BaseID, cur)
		}
		raw, err := buildRaw(s.Entries)
		if err != nil {
			return nil, err
		}
		if kids, err = nv.patchKids(kids, raw, rd); err != nil {
			return nil, err
		}
		if cur, err = nv.nodeID(newRoot(kids)); err != nil {
			return nil, err
		}
	}
	if closing, ok := closingID(data); ok && len(sections) > 0 && closing != cur {
		return nil, fmt.Errorf("%w: the patch ends at %s, applying it gives %s", ErrIDMismatch, closing, cur)
	}
	nv.setRoot(kids)
	nv.id.Store(&cur)
	return nv, nil
}

// closingID returns the patch stream's last line if it is a bare C4 ID.
func closingID(data []byte) (c4.ID, bool) {
	data = bytes.TrimRight(data, " \t\r\n")
	line := data[bytes.LastIndexByte(data, '\n')+1:]
	line = bytes.TrimSpace(line)
	if len(line) != 90 {
		return c4.ID{}, false
	}
	id, err := c4.Parse(string(line))
	return id, err == nil
}

// Resume reconstructs a workspace from what Capture produced: the base
// View the first capture applied to, the changesets since (as
// Changeset.WriteTo wrote them, appended in order), and the store their
// content was captured into. The workspace starts with no changes over
// the captured state, an empty scratch, and its head at the last
// capture, so its next Capture continues the same chain.
func Resume(base *View, patch io.Reader, src store.Source, scratch absfs.FileSystem) (*Workspace, error) {
	v, err := base.Apply(patch, src)
	if err != nil {
		return nil, err
	}
	return NewWorkspace(v, scratch)
}
