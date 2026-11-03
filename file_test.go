package c4fs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

func TestFileReadSeek(t *testing.T) {
	v := newFixture(t).views(t)["record"]
	f, err := v.Open("/docs/guide.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.Name() != "/docs/guide.txt" {
		t.Errorf("Name = %q", f.Name())
	}
	body := fixtureFiles["docs/guide.txt"]
	// Seek from the end needs only the c4m size.
	if pos, err := f.Seek(-6, io.SeekEnd); err != nil || pos != int64(len(body)-6) {
		t.Fatalf("seek end = %d %v", pos, err)
	}
	buf := make([]byte, 100)
	if n, err := f.Read(buf); n != 6 || err != nil || string(buf[:n]) != "guide " {
		t.Errorf("read tail = %d %v %q", n, err, buf[:n])
	}
	if n, err := f.Read(buf); n != 0 || err != io.EOF {
		t.Errorf("read at end = %d %v", n, err)
	}
	if pos, _ := f.Seek(6, io.SeekStart); pos != 6 {
		t.Errorf("seek start = %d", pos)
	}
	if pos, _ := f.Seek(6, io.SeekCurrent); pos != 12 {
		t.Errorf("seek current = %d", pos)
	}
	if n, _ := f.Read(buf[:6]); string(buf[:n]) != "guide " {
		t.Errorf("read mid = %q", buf[:n])
	}
	if _, err := f.Seek(-1, io.SeekStart); !errors.Is(err, errInvalid) {
		t.Errorf("negative seek = %v", err)
	}
	if _, err := f.Seek(0, 7); !errors.Is(err, errInvalid) {
		t.Errorf("bad whence = %v", err)
	}
	if pos, _ := f.Seek(1e6, io.SeekStart); pos != 1e6 {
		t.Errorf("seek past end = %d", pos)
	}
	if n, err := f.Read(buf); n != 0 || err != io.EOF {
		t.Errorf("read past end = %d %v", n, err)
	}
	if n, err := f.Read(nil); n != 0 || err != nil {
		t.Errorf("empty read = %d %v", n, err)
	}
	if n, err := f.ReadAt(buf[:0], 1e6); n != 0 || err != nil {
		t.Errorf("empty ReadAt past end = %d %v", n, err)
	}
	if n, err := f.ReadAt(buf, 1e6); n != 0 || err != io.EOF {
		t.Errorf("ReadAt past end = %d %v", n, err)
	}
	if _, err := f.ReadAt(buf, -1); !errors.Is(err, errInvalid) {
		t.Errorf("negative ReadAt = %v", err)
	}
	if fi, err := f.Stat(); err != nil || fi.Name() != "guide.txt" || fi.Size() != int64(len(body)) {
		t.Errorf("Stat = %v %v", fi, err)
	}
	if err := f.Sync(); err != nil {
		t.Errorf("Sync = %v", err)
	}
	// Parallel ReadAt is safe and correct.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := make([]byte, 6)
			off := int64(i * 6 * 10)
			if n, err := f.ReadAt(b, off); n != 6 || err != nil || string(b) != "guide " {
				t.Errorf("parallel ReadAt(%d) = %d %v %q", off, n, err, b)
			}
		}(i)
	}
	wg.Wait()
	if _, err := f.Readdir(1); !errors.Is(err, errNotDir) {
		t.Errorf("Readdir on file = %v", err)
	}
}

func TestFileClosed(t *testing.T) {
	v := newFixture(t).views(t)["manifest"]
	for _, name := range []string{"/README.md", "/src"} {
		f, err := v.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		checks := map[string]error{}
		_, checks["read"] = f.Read(make([]byte, 1))
		_, checks["readat"] = f.ReadAt(make([]byte, 1), 0)
		_, checks["seek"] = f.Seek(0, io.SeekStart)
		_, checks["stat"] = f.Stat()
		_, checks["readdir"] = f.Readdir(-1)
		_, checks["write"] = f.Write([]byte("x"))
		checks["sync"] = f.Sync()
		checks["close"] = f.Close()
		for op, err := range checks {
			if !errors.Is(err, os.ErrClosed) {
				t.Errorf("%s %s after close = %v", name, op, err)
			}
		}
	}
}

func TestDirectoryPagination(t *testing.T) {
	v := newFixture(t).views(t)["record"]
	want := names(t, v, "/")
	open := func() interface {
		ReadDir(int) ([]fs.DirEntry, error)
		Readdir(int) ([]os.FileInfo, error)
		Readdirnames(int) ([]string, error)
		Seek(int64, int) (int64, error)
		Read([]byte) (int, error)
		ReadAt([]byte, int64) (int, error)
		Close() error
	} {
		f, err := v.Open("/")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}

	// n > 0: batches, then io.EOF with nothing.
	f := open()
	var got []string
	for {
		ents, err := f.ReadDir(3)
		for _, e := range ents {
			got = append(got, e.Name())
		}
		if err == io.EOF {
			if len(ents) != 0 {
				t.Errorf("EOF with %d entries", len(ents))
			}
			break
		}
		if err != nil || len(ents) == 0 || len(ents) > 3 {
			t.Fatalf("ReadDir(3) = %d %v", len(ents), err)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("paged = %v, want %v", got, want)
	}
	// n <= 0 after the end: nothing, and no error.
	if ents, err := f.ReadDir(-1); len(ents) != 0 || err != nil {
		t.Errorf("ReadDir(-1) at end = %d %v", len(ents), err)
	}
	// Seek to the start restarts; other seeks are refused.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Errorf("rewind = %v", err)
	}
	if _, err := f.Seek(1, io.SeekStart); !errors.Is(err, errInvalid) {
		t.Errorf("dir seek = %v", err)
	}
	if all, err := f.Readdirnames(0); err != nil || !reflect.DeepEqual(all, want) {
		t.Errorf("Readdirnames(0) = %v %v", all, err)
	}

	// The three listing calls share one position.
	f = open()
	a, _ := f.Readdirnames(2)
	b, _ := f.Readdir(2)
	c, _ := f.ReadDir(-1)
	got = append(a, b[0].Name(), b[1].Name())
	for _, e := range c {
		got = append(got, e.Name())
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mixed = %v", got)
	}
	if n, err := f.Readdirnames(1); len(n) != 0 || err != io.EOF {
		t.Errorf("Readdirnames at end = %v %v", n, err)
	}
	if _, err := f.Read(make([]byte, 1)); !errors.Is(err, errIsDir) {
		t.Errorf("Read dir = %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, errIsDir) {
		t.Errorf("ReadAt dir = %v", err)
	}
	if b[0].Sys() == nil || c[0].Type() != c[0].(fs.FileInfo).Mode().Type() {
		t.Error("entries lack metadata")
	}
}

func TestDirectoryListingErrors(t *testing.T) {
	f := newFixture(t)
	lib := f.m.GetEntry("src/lib/")
	f.st.Remove(lib.C4ID)
	v, _ := Open(f.st, f.root)
	d, err := v.Open("/src/lib")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Readdirnames(-1); !errors.Is(err, ErrContentMissing) {
		t.Errorf("listing a missing record = %v", err)
	}
}

func TestSymlinks(t *testing.T) {
	m := parse(t, strings.Join([]string{
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 loop1 -> loop2 -",
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 loop2 -> loop1 -",
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 self -> self -",
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 toroot -> / -",
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 escape -> ../../../d/f -",
		"lrwxrwxrwx 2025-01-01T00:00:00Z 4 chain -> d/inner -",
		"drwxr-xr-x 2025-01-01T00:00:00Z 1 d/ -",
		"  -rw-r--r-- 2025-01-01T00:00:00Z 1 f " + emptyID.String(),
		"  lrwxrwxrwx 2025-01-01T00:00:00Z 4 inner -> ./f -",
		"  lrwxrwxrwx 2025-01-01T00:00:00Z 4 parent -> .. -",
		"",
	}, "\n"))
	v, err := New(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"/loop1", "/self", "/loop1/x"} {
		if _, err := v.Stat(name); !errors.Is(err, errLoop) {
			t.Errorf("Stat(%s) = %v", name, err)
		}
	}
	if fi, err := v.Lstat("/loop1"); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("Lstat loop = %v %v", fi, err)
	}
	follow := map[string]string{
		"/toroot/d/f":     "f",
		"/escape":         "f",
		"/chain":          "f",
		"/d/parent/d/f":   "f",
		"/d/parent":       "/",
		"/toroot":         "/",
		"/d/inner":        "f",
		"/chain/../d/f":   "f",
		"/d/../toroot/d/": "d",
	}
	for name, want := range follow {
		n, err := v.lookup("stat", name, true)
		if err != nil {
			t.Errorf("lookup(%s) = %v", name, err)
			continue
		}
		if got := n.entry.Name; strings.TrimSuffix(got, "/") != want && !(want == "/" && n == v.root) {
			t.Errorf("lookup(%s) = %q, want %q", name, got, want)
		}
	}
	if target, err := v.Readlink("/d/inner"); err != nil || target != "./f" {
		t.Errorf("Readlink = %q %v", target, err)
	}
	if _, err := v.Readlink("/d/f"); !errors.Is(err, errInvalid) {
		t.Errorf("Readlink file = %v", err)
	}
	if _, err := v.Readlink("/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Readlink missing = %v", err)
	}
	if got := names(t, v, "/toroot/d"); !reflect.DeepEqual(got, []string{"f", "inner", "parent"}) {
		t.Errorf("ReadDir through link = %v", got)
	}
}

func TestSequences(t *testing.T) {
	f := &fixture{dir: t.TempDir(), st: newTreeStore(t)}
	files := map[string]string{"plain.txt": "plain"}
	for i, n := range []string{"0001", "0002", "0003", "0004"} {
		files["shots/frame."+n+".exr"] = strings.Repeat("x", i+1)
	}
	writeTree(t, f.dir, files)
	f.m, f.self, f.root = snapshot(t, f.dir, f.st, true)
	seq := f.m.Entries[len(f.m.Entries)-1]
	if !seq.IsSequence {
		t.Fatalf("fixture not folded: %v", f.m.Entries)
	}
	for how, v := range map[string]*View{"manifest": mustNew(t, f.m, f.st), "record": mustOpen(t, f.st, f.root)} {
		t.Run(how, func(t *testing.T) {
			want := []string{"frame.0001.exr", "frame.0002.exr", "frame.0003.exr", "frame.0004.exr"}
			if got := names(t, v, "/shots"); !reflect.DeepEqual(got, want) {
				t.Fatalf("members = %v", got)
			}
			for i, n := range want {
				body := strings.Repeat("x", i+1)
				if got := mustRead(t, v, "/shots/"+n); got != body {
					t.Errorf("%s = %q", n, got)
				}
				fi, _ := v.Stat("/shots/" + n)
				if fi.Size() != int64(i+1) || fi.Mode() != seq.Mode || !fi.ModTime().Equal(seq.Timestamp) {
					t.Errorf("%s stat = %d %v %v", n, fi.Size(), fi.Mode(), fi.ModTime())
				}
				if e := fi.Sys().(*c4m.Entry); e.C4ID != idOf(body) {
					t.Errorf("%s id = %s", n, e.C4ID)
				}
			}
		})
	}

	// Range data inline in the manifest is used without the store.
	ids := make([]string, 4)
	for i := range ids {
		ids[i] = idOf(strings.Repeat("x", i+1)).String()
	}
	list := strings.Join(ids, "")
	m := parse(t, "-rw-r--r-- - 10 f.[0001-0004].exr "+idOf(list).String()+"\n"+list+"\n")
	if len(m.RangeData) != 1 {
		t.Fatalf("range data not parsed: %v", m.RangeData)
	}
	v := mustNew(t, m, nil)
	fi, err := v.Stat("/f.0003.exr")
	if err != nil {
		t.Fatal(err)
	}
	if e := fi.Sys().(*c4m.Entry); e.C4ID.String() != ids[2] {
		t.Errorf("inline member id = %s", e.C4ID)
	}

	// Without an ID list the members list, with null IDs.
	m = parse(t, "-rw-r--r-- - 10 g.[1-2].exr "+idOf("no list").String()+"\n")
	v = mustNew(t, m, nil)
	if got := names(t, v, "/"); !reflect.DeepEqual(got, []string{"g.1.exr", "g.2.exr"}) {
		t.Errorf("unlisted members = %v", got)
	}
	if _, err := v.ReadFile("/g.1.exr"); !errors.Is(err, ErrNoID) {
		t.Errorf("read unlisted member = %v", err)
	}
	// A single-member sequence keeps the entry's size.
	m = parse(t, "-rw-r--r-- - 7 h.[5].exr -\n")
	v = mustNew(t, m, nil)
	if fi, _ := v.Stat("/h.5.exr"); fi == nil || fi.Size() != 7 {
		t.Errorf("single member = %v", fi)
	}
	// A malformed ID list is ignored.
	st := newTreeStore(t)
	bad := putBytes(t, st, []byte("not an id list"))
	notID := putBytes(t, st, []byte(strings.Repeat("c", 90)))
	for _, id := range []string{bad.String(), notID.String()} {
		m = parse(t, "-rw-r--r-- - 10 k.[1-2].exr "+id+"\n")
		v = mustNew(t, m, st)
		if fi, _ := v.Stat("/k.1.exr"); fi == nil || !fi.Sys().(*c4m.Entry).C4ID.IsNil() {
			t.Errorf("bad list gave member %v", fi)
		}
	}
}

func mustOpen(t testing.TB, src store.Source, id c4.ID) *View {
	t.Helper()
	v, err := Open(src, id)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
