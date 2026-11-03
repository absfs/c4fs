package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

func TestSequencePatches(t *testing.T) {
	st := newTreeStore(t)
	list := func(bodies ...string) (string, c4.ID) {
		var ids []string
		for _, b := range bodies {
			ids = append(ids, putBytes(t, st, []byte(b)).String())
		}
		l := strings.Join(ids, "")
		return l, putBytes(t, st, []byte(l))
	}
	_, s1 := list("1", "2", "3")
	_, s2 := list("one", "two")
	_, s3 := list("x", "y")
	base := parse(t, strings.Join([]string{
		"-rw-r--r-- 2025-01-01T00:00:00Z 3 a.[1-3].x " + s1.String(),
		"-rw-r--r-- 2025-01-01T00:00:00Z 2 b.[1-2].x " + s3.String(),
		"-rw-r--r-- 2025-01-01T00:00:00Z 1 c.1.x " + idOf("plain").String(),
		"",
	}, "\n"))
	if !base.Entries[0].IsSequence {
		t.Fatal("fixture not a sequence")
	}
	bid := putBytes(t, st, mustMarshal(t, base))
	text := bid.String() + "\n" + strings.Join([]string{
		// identical: removes the whole sequence
		"-rw-r--r-- 2025-01-01T00:00:00Z 3 a.[1-3].x " + s1.String(),
		// changed: replaces its members
		"-rw-r--r-- 2025-01-01T00:00:00Z 6 b.[1-2].x " + s2.String(),
		// new sequence whose member collides with a plain file: the
		// patch's member wins
		"-rw-r--r-- 2025-01-01T00:00:00Z 2 c.[1-2].x " + s3.String(),
		"",
	}, "\n")
	v := mustOpen(t, st, putBytes(t, st, []byte(text)))
	if got := names(t, v, "/"); !reflect.DeepEqual(got, []string{"b.1.x", "b.2.x", "c.1.x", "c.2.x"}) {
		t.Fatalf("names = %v", got)
	}
	for name, want := range map[string]string{"/b.1.x": "one", "/b.2.x": "two", "/c.1.x": "x", "/c.2.x": "y"} {
		if got := mustRead(t, v, name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	// A plain patch entry named like a member replaces just that member.
	text2 := bid.String() + "\n-rw-r--r-- 2025-01-01T00:00:00Z 5 a.2.x " + putBytes(t, st, []byte("patch")).String() + "\n"
	v = mustOpen(t, st, putBytes(t, st, []byte(text2)))
	if got := mustRead(t, v, "/a.2.x"); got != "patch" {
		t.Errorf("a.2.x = %q", got)
	}
	if got := mustRead(t, v, "/a.3.x"); got != "3" {
		t.Errorf("a.3.x = %q", got)
	}
}

func TestRangeData(t *testing.T) {
	a, b := idOf("a").String(), idOf("b").String()
	bad := "c4" + strings.Repeat("0", 88)
	data := strings.Join([]string{
		a + b,            // valid list
		a,                // a bare ID, not a list
		a + bad,          // invalid chunk
		"xx" + a[2:] + b, // wrong prefix
		"",
	}, "\n")
	rd := rangeData([]byte(data))
	if len(rd) != 1 || rd[idOf(a+b)] != a+b {
		t.Errorf("rangeData = %v", rd)
	}
	if rangeData([]byte("nothing here")) != nil {
		t.Error("rangeData of no lists is not nil")
	}
}

func TestChainEdgeCases(t *testing.T) {
	st := newTreeStore(t)
	// A stream holding nothing but a bare ID resolves to nothing.
	lone := putBytes(t, st, []byte(idOf("x").String()+"\n"))
	v := mustOpen(t, st, lone)
	if got := names(t, v, "/"); len(got) != 0 {
		t.Errorf("lone ID = %v", got)
	}
	// A bad later section is reported.
	text := "-rw-r--r-- - 1 a -\n" + idOf("ckpt").String() + "\n  -rw-r--r-- - 1 b -\n"
	if _, err := Open(st, putBytes(t, st, []byte(text))); !errors.Is(err, ErrInvalidManifest) {
		t.Errorf("bad section = %v", err)
	}
	// A bad first section is reported.
	if _, err := Open(st, putBytes(t, st, []byte("  -rw-r--r-- - 1 a -\n"))); !errors.Is(err, ErrInvalidManifest) {
		t.Errorf("bad first section = %v", err)
	}
	// A base that is not in the store is reported.
	if _, err := Open(st, putBytes(t, st, []byte(idOf("gone").String()+"\n-rw-r--r-- - 1 a -\n"))); !errors.Is(err, ErrContentMissing) {
		t.Errorf("missing base = %v", err)
	}
}

// failingReaderAt is random-access content that fails partway.
type failingReaderAt struct{ body []byte }

func (s failingReaderAt) Open(c4.ID) (io.ReadCloser, error) {
	return struct {
		io.ReaderAt
		io.Seeker
		io.Reader
		io.Closer
	}{brokenAt{}, strings.NewReader(string(s.body)), strings.NewReader(""), io.NopCloser(nil)}, nil
}

type brokenAt struct{}

func (brokenAt) ReadAt([]byte, int64) (int, error) { return 0, errors.New("medium error") }

// shortReaderAt returns fewer bytes than asked without an error.
type shortReaderAt struct{ body []byte }

func (s shortReaderAt) Open(c4.ID) (io.ReadCloser, error) {
	return struct {
		io.ReaderAt
		io.Seeker
		io.Reader
		io.Closer
	}{shortAt(s), strings.NewReader(string(s.body)), strings.NewReader(""), io.NopCloser(nil)}, nil
}

type shortAt struct{ body []byte }

func (s shortAt) ReadAt(b []byte, off int64) (int, error) {
	return copy(b[:min(len(b), 1)], s.body[off:]), nil
}

func TestReadErrors(t *testing.T) {
	m := oneFile(t, "f", "content")
	v, _ := New(m, failingReaderAt{[]byte("content")})
	f, _ := v.Open("/f")
	defer f.Close()
	if _, err := f.Read(make([]byte, 4)); err == nil || err == io.EOF {
		t.Errorf("Read = %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 4), 0); err == nil || err == io.EOF {
		t.Errorf("ReadAt = %v", err)
	}
	v, _ = New(m, shortReaderAt{[]byte("content")})
	g, _ := v.Open("/f")
	defer g.Close()
	if n, err := g.ReadAt(make([]byte, 4), 0); n != 1 || err != io.EOF {
		t.Errorf("short ReadAt = %d %v", n, err)
	}
}

func TestDefaultCacheDirFallback(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("LocalAppData", "")
	if _, err := os.UserCacheDir(); err != nil {
		if got := DefaultCacheDir(); got != filepath.Join(os.TempDir(), "c4fs") {
			t.Errorf("fallback = %q", got)
		}
	}
}

func TestBuildRawNesting(t *testing.T) {
	// A file at depth 0 after a directory closes that directory.
	m := &c4m.Manifest{Entries: []*c4m.Entry{
		{Name: "d/", Mode: fs.ModeDir, Timestamp: c4m.NullTimestamp()},
		{Name: "e/", Depth: 1, Mode: fs.ModeDir, Timestamp: c4m.NullTimestamp()},
		{Name: "deep", Depth: 2, Timestamp: c4m.NullTimestamp()},
		{Name: "top", Timestamp: c4m.NullTimestamp()},
	}}
	v := mustNew(t, m, nil)
	if got := names(t, v, "/"); !reflect.DeepEqual(got, []string{"d", "top"}) {
		t.Errorf("root = %v", got)
	}
	if got := names(t, v, "/d/e"); !reflect.DeepEqual(got, []string{"deep"}) {
		t.Errorf("d/e = %v", got)
	}
}
