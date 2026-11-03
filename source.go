package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"reflect"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/store"
)

// sources reads from several stores in order: the first that has an
// object serves it. A View over a captured or resumed tree uses one, so
// that the base's content comes from the base's store and the captured
// content from the store it was captured into.
type sources []store.Source

// joinSources returns a source reading a, then b. Nil sources and a
// repeat of the same store are dropped.
func joinSources(a, b store.Source) store.Source {
	var out sources
	var add func(store.Source)
	add = func(s store.Source) {
		if s == nil {
			return
		}
		if ss, ok := s.(sources); ok {
			for _, x := range ss {
				add(x)
			}
			return
		}
		for _, x := range out {
			if sameSource(x, s) {
				return
			}
		}
		out = append(out, s)
	}
	add(a)
	add(b)
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return out
}

// sameSource reports whether a and b are the same store value, without
// panicking on store types that are not comparable.
func sameSource(a, b store.Source) bool {
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	return ta == tb && ta.Comparable() && a == b
}

// Open returns the first store's reader for id. If none has it, the
// error is one that is not an absence when any store reported one, so a
// network failure is not mistaken for missing content.
func (s sources) Open(id c4.ID) (io.ReadCloser, error) {
	var notExist, other error
	for _, src := range s {
		rc, err := src.Open(id)
		if err == nil {
			return rc, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			notExist = err
		} else if other == nil {
			other = err
		}
	}
	if other != nil {
		return nil, other
	}
	return nil, notExist
}

// Has reports whether any store has id. A store that cannot say is
// asked to open it.
func (s sources) Has(id c4.ID) bool {
	for _, src := range s {
		if h, ok := src.(haser); ok {
			if h.Has(id) {
				return true
			}
			continue
		}
		if rc, err := src.Open(id); err == nil {
			rc.Close()
			return true
		}
	}
	return false
}

// ContentPath returns a local file holding id from the first store that
// keeps objects as local files and has it.
func (s sources) ContentPath(id c4.ID) (string, bool) {
	for _, src := range s {
		if cp, ok := src.(contentPather); ok {
			if p, ok := cp.ContentPath(id); ok {
				return p, true
			}
		}
	}
	return "", false
}
