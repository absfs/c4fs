package c4fs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

var fixtureFiles = map[string]string{
	"README.md":       "hello\n",
	"empty.txt":       "",
	"src/main.go":     "package main\n",
	"src/lib/a.go":    "package lib // a\n",
	"src/lib/b.go":    "package lib // b\n",
	"docs/guide.txt":  strings.Repeat("guide ", 1000),
	"emptydir/":       "",
	"link":            "->src/main.go",
	"dirlink":         "->src",
	"abslink":         "->/src/lib/a.go",
	"dangling":        "->nowhere",
	"src/lib/up":      "->../main.go",
	"name with space": "spaced",
}

type fixture struct {
	dir   string
	st    *store.TreeStore
	m     *c4m.Manifest
	self  c4.ID
	root  c4.ID
	cache *Cache
}

func newFixture(t testing.TB) *fixture {
	t.Helper()
	f := &fixture{dir: t.TempDir(), st: newTreeStore(t), cache: newTestCache(t)}
	writeTree(t, f.dir, fixtureFiles)
	f.m, f.self, f.root = snapshot(t, f.dir, f.st, false)
	return f
}

// views returns the three ways to open the same tree.
func (f *fixture) views(t testing.TB) map[string]*View {
	t.Helper()
	byManifest, err := New(f.m, f.st, WithCache(f.cache))
	if err != nil {
		t.Fatal(err)
	}
	bySelf, err := Open(f.st, f.self, WithCache(f.cache))
	if err != nil {
		t.Fatal(err)
	}
	byRoot, err := Open(f.st, f.root, WithCache(f.cache))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*View{"manifest": byManifest, "self": bySelf, "record": byRoot}
}

func TestViewReadsTree(t *testing.T) {
	f := newFixture(t)
	for how, v := range f.views(t) {
		t.Run(how, func(t *testing.T) {
			want := []string{"README.md", "abslink", "dangling", "dirlink", "docs", "empty.txt", "emptydir", "link", "name with space", "src"}
			if got := names(t, v, "/"); !reflect.DeepEqual(got, want) {
				t.Errorf("root = %v, want %v", got, want)
			}
			if got := names(t, v, "/src/lib"); !reflect.DeepEqual(got, []string{"a.go", "b.go", "up"}) {
				t.Errorf("lib = %v", got)
			}
			if got := names(t, v, "/emptydir"); len(got) != 0 {
				t.Errorf("emptydir = %v", got)
			}
			for name, body := range fixtureFiles {
				if strings.HasSuffix(name, "/") || strings.HasPrefix(body, "->") {
					continue
				}
				if got := mustRead(t, v, "/"+name); got != body {
					t.Errorf("%s = %q, want %q", name, got, body)
				}
			}
			if got := mustRead(t, v, "/src/lib/up"); got != "package main\n" {
				t.Errorf("up = %q", got)
			}
			if got := mustRead(t, v, "/dirlink/lib/b.go"); got != fixtureFiles["src/lib/b.go"] {
				t.Errorf("through dirlink = %q", got)
			}
			if got := mustRead(t, v, "/abslink"); got != fixtureFiles["src/lib/a.go"] {
				t.Errorf("abslink = %q", got)
			}
		})
	}
}

func TestStatMetadata(t *testing.T) {
	f := newFixture(t)
	disk, err := os.Lstat(filepath.Join(f.dir, "src", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	// The View reports what the scan recorded, which is what the host
	// reports: on Windows a directory is drwxrwxrwx and a symbolic link
	// has size 0; elsewhere they are drwxr-xr-x and the target's length.
	diskDir, err := os.Lstat(filepath.Join(f.dir, "src"))
	if err != nil {
		t.Fatal(err)
	}
	diskLink, err := os.Lstat(filepath.Join(f.dir, "link"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && (diskDir.Mode() != fs.ModeDir|0o755 || diskLink.Size() != int64(len("src/main.go"))) {
		t.Fatalf("disk: dir %v, link size %d", diskDir.Mode(), diskLink.Size())
	}
	for how, v := range f.views(t) {
		t.Run(how, func(t *testing.T) {
			fi, err := v.Stat("/src/main.go")
			if err != nil {
				t.Fatal(err)
			}
			if fi.Name() != "main.go" || fi.Size() != 13 || fi.IsDir() || fi.Mode() != disk.Mode() {
				t.Errorf("stat = %s %d %v %v", fi.Name(), fi.Size(), fi.Mode(), fi.IsDir())
			}
			if !fi.ModTime().Truncate(time.Second).Equal(disk.ModTime().Truncate(time.Second)) {
				t.Errorf("mtime = %v, disk %v", fi.ModTime(), disk.ModTime())
			}
			e, ok := fi.Sys().(*c4m.Entry)
			if !ok || e.C4ID != idOf("package main\n") {
				t.Errorf("Sys = %#v", fi.Sys())
			}
			di, err := v.Stat("/src")
			if err != nil || !di.IsDir() || di.Mode() != diskDir.Mode() {
				t.Errorf("dir stat = %v %v", di, err)
			}
			ri, err := v.Stat("/")
			if err != nil || !ri.IsDir() || ri.Name() != "/" {
				t.Errorf("root stat = %v %v", ri, err)
			}
			li, err := v.Lstat("/link")
			if err != nil || li.Mode()&fs.ModeSymlink == 0 || li.Size() != diskLink.Size() {
				t.Errorf("lstat link = %v %v", li, err)
			}
			si, err := v.Stat("/link")
			if err != nil || si.Mode()&fs.ModeSymlink != 0 || si.Size() != 13 || si.Name() != "link" {
				t.Errorf("stat link = %v %v", si, err)
			}
		})
	}
}

func TestRootMetadata(t *testing.T) {
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	m := parse(t, "-rw-r--r-- 2025-01-02T03:04:05Z 3 a -\n-rw-r--r-- 2024-01-01T00:00:00Z 4 b -\n")
	v, err := New(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	ri, _ := v.Stat("/")
	if ri.Size() != 7 || !ri.ModTime().Equal(ts) {
		t.Errorf("root = %d %v", ri.Size(), ri.ModTime())
	}
	m = parse(t, "- - - a -\n-rw-r--r-- 2024-01-01T00:00:00Z 4 b -\n")
	v, _ = New(m, nil)
	ri, _ = v.Stat("/")
	if ri.Size() != 0 || !ri.ModTime().Equal(c4m.NullTimestamp()) {
		t.Errorf("null root = %d %v", ri.Size(), ri.ModTime())
	}
}

func TestNullValues(t *testing.T) {
	st := newTreeStore(t)
	body := "measured"
	id := putBytes(t, st, []byte(body))
	m := parse(t, strings.Join([]string{
		"- - - nullfile " + id.String(),
		"---------- 2025-01-01T00:00:00Z 0 zeromode -",
		"- - - nullid -",
		"- - - unknown " + idOf("absent").String(),
		"- - - d/",
		"lrwxrwxrwx - - l -> nullfile -",
		"",
	}, "\n"))
	v, err := New(m, st, WithCache(newTestCache(t)))
	if err != nil {
		t.Fatal(err)
	}
	fi, err := v.Stat("/nullfile")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode() != 0o644 || !fi.ModTime().Equal(time.Unix(0, 0)) || fi.Size() != int64(len(body)) {
		t.Errorf("null file = %v %v %d", fi.Mode(), fi.ModTime(), fi.Size())
	}
	if fi.Size() != int64(len(body)) {
		t.Error("memoised size changed")
	}
	if got := mustRead(t, v, "/nullfile"); got != body {
		t.Errorf("read = %q", got)
	}
	// "----------" decodes to c4m's null mode too.
	if fi, _ := v.Stat("/zeromode"); fi.Mode() != 0o644 {
		t.Errorf("zero mode = %v", fi.Mode())
	}
	if fi, _ := v.Stat("/nullid"); fi.Size() != 0 {
		t.Errorf("null id size = %d", fi.Size())
	}
	if _, err := v.ReadFile("/nullid"); !errors.Is(err, ErrNoID) {
		t.Errorf("read null id = %v", err)
	}
	if fi, _ := v.Stat("/unknown"); fi.Size() != 0 {
		t.Errorf("unresolvable size = %d", fi.Size())
	}
	if di, _ := v.Stat("/d"); di.Mode() != fs.ModeDir|0o755 || di.Size() != 0 {
		t.Errorf("null dir = %v %d", di.Mode(), di.Size())
	}
	if li, _ := v.Lstat("/l"); li.Size() != int64(len("nullfile")) {
		t.Errorf("null symlink size = %d", li.Size())
	}
}

func TestModeDefaults(t *testing.T) {
	m := parse(t, strings.Join([]string{
		"-rwxr-x--- 2025-01-01T00:00:00Z 0 exec -",
		"prw-r--r-- 2025-01-01T00:00:00Z 0 fifo -",
		"d--------- 2025-01-01T00:00:00Z 0 locked/ -",
		"- 2025-01-01T00:00:00Z 0 nulllink -> exec -",
		"",
	}, "\n"))
	v, err := New(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]fs.FileMode{
		"/exec":   0o750,
		"/fifo":   fs.ModeNamedPipe | 0o644,
		"/locked": fs.ModeDir,
	}
	for name, want := range cases {
		if fi, err := v.Lstat(name); err != nil || fi.Mode() != want {
			t.Errorf("%s = %v %v, want %v", name, fi.Mode(), err, want)
		}
	}
	if fi, err := v.Lstat("/nulllink"); err != nil || fi.Mode() != fs.ModeSymlink|0o777 {
		t.Errorf("null-mode link = %v %v", fi, err)
	}
	if b, err := v.ReadFile("/fifo"); err != nil || len(b) != 0 {
		t.Errorf("fifo read = %q %v", b, err)
	}
	ents, _ := v.ReadDir("/")
	for _, e := range ents {
		if e.Name() == "fifo" && e.Type() != fs.ModeNamedPipe {
			t.Errorf("fifo type = %v", e.Type())
		}
		if info, err := e.Info(); err != nil || info.Name() != e.Name() {
			t.Errorf("info = %v %v", info, err)
		}
		if !strings.Contains(e.(fs.FileInfo).Name(), e.Name()) || e.(interface{ String() string }).String() == "" {
			t.Error("String")
		}
	}
}

func TestDuplicatesLastWins(t *testing.T) {
	m := &c4m.Manifest{Entries: []*c4m.Entry{
		{Name: "x", Size: 1, Mode: 0o644, Timestamp: c4m.NullTimestamp()},
		{Name: "d/", Mode: fs.ModeDir | 0o755, Timestamp: c4m.NullTimestamp()},
		{Name: "y", Size: 2, Depth: 1, Timestamp: c4m.NullTimestamp()},
		{Name: "x", Size: 9, Mode: 0o600, Timestamp: c4m.NullTimestamp()},
	}}
	v, err := New(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fi, _ := v.Stat("/x"); fi.Size() != 9 || fi.Mode() != 0o600 {
		t.Errorf("x = %d %v", fi.Size(), fi.Mode())
	}
	if got := names(t, v, "/"); !reflect.DeepEqual(got, []string{"d", "x"}) {
		t.Errorf("names = %v", got)
	}
}

func TestInvalidManifests(t *testing.T) {
	ts := c4m.NullTimestamp()
	cases := map[string][]*c4m.Entry{
		"depth jump":  {{Name: "a", Depth: 1, Timestamp: ts}},
		"under file":  {{Name: "a", Timestamp: ts}, {Name: "b", Depth: 1, Timestamp: ts}},
		"slash name":  {{Name: "a/b", Timestamp: ts}},
		"dot":         {{Name: ".", Timestamp: ts}},
		"dotdot dir":  {{Name: "../", Timestamp: ts}},
		"empty":       {{Name: "", Timestamp: ts}},
		"negative":    {{Name: "a", Depth: -1, Timestamp: ts}},
		"nul in name": {{Name: "a\x00b", Timestamp: ts}},
	}
	for name, entries := range cases {
		if _, err := New(&c4m.Manifest{Entries: entries}, nil); !errors.Is(err, ErrInvalidManifest) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestEmptyManifest(t *testing.T) {
	v, err := New(c4m.NewManifest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(t, v, "/"); len(got) != 0 {
		t.Errorf("names = %v", got)
	}
	st := newTreeStore(t)
	v, err = Open(st, emptyID)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(t, v, "/"); len(got) != 0 {
		t.Errorf("names = %v", got)
	}
}

func TestLookupErrors(t *testing.T) {
	v := newFixture(t).views(t)["manifest"]
	cases := map[string]error{
		"":                   fs.ErrNotExist,
		"/nope":              fs.ErrNotExist,
		"/README.md/x":       errNotDir,
		"/dangling":          fs.ErrNotExist,
		"/src/lib/a.go/more": errNotDir,
	}
	for name, want := range cases {
		_, err := v.Stat(name)
		var pe *fs.PathError
		if !errors.Is(err, want) || !errors.As(err, &pe) || pe.Op != "stat" || pe.Path != name {
			t.Errorf("Stat(%q) = %v, want %v", name, err, want)
		}
	}
	if _, err := v.Lstat("/dangling"); err != nil {
		t.Errorf("Lstat dangling = %v", err)
	}
	if _, err := v.ReadDir("/README.md"); !errors.Is(err, errNotDir) {
		t.Errorf("ReadDir file = %v", err)
	}
	if _, err := v.ReadDir("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadDir missing = %v", err)
	}
	if _, err := v.ReadFile("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile missing = %v", err)
	}
}

func TestRelativePathsAndChdir(t *testing.T) {
	v := newFixture(t).views(t)["record"]
	if wd, _ := v.Getwd(); wd != "/" {
		t.Errorf("wd = %q", wd)
	}
	if err := v.Chdir("src/lib"); err != nil {
		t.Fatal(err)
	}
	if wd, _ := v.Getwd(); wd != "/src/lib" {
		t.Errorf("wd = %q", wd)
	}
	if got := mustRead(t, v, "a.go"); got != fixtureFiles["src/lib/a.go"] {
		t.Errorf("relative read = %q", got)
	}
	if got := mustRead(t, v, "../main.go"); got != fixtureFiles["src/main.go"] {
		t.Errorf("dotdot read = %q", got)
	}
	if err := v.Chdir("/README.md"); !errors.Is(err, errNotDir) {
		t.Errorf("chdir file = %v", err)
	}
	if err := v.Chdir("/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("chdir missing = %v", err)
	}
	if v.TempDir() != "/tmp" {
		t.Error(v.TempDir())
	}
	if fi, err := v.Stat("."); err != nil || fi.Name() != "." || !fi.IsDir() {
		t.Errorf("stat . = %v %v", fi, err)
	}
	if _, err := v.Stat("/src//lib/./a.go"); err != nil {
		t.Errorf("unclean path = %v", err)
	}
	if _, err := v.Stat("/../../src"); err != nil {
		t.Errorf("above root = %v", err)
	}
}
