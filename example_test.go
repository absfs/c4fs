package c4fs_test

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"strings"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/c4fs"
	"github.com/absfs/memfs"
	"github.com/absfs/unionfs"
)

// A View over a manifest held in memory, with content in a store.
func ExampleNew() {
	dir, _ := os.MkdirTemp("", "c4store")
	defer os.RemoveAll(dir)
	s, _ := store.NewTreeStore(dir)
	id, _ := s.Put(strings.NewReader("hello, world\n"))

	m, err := c4m.Unmarshal([]byte(
		"drwxr-xr-x 2025-01-01T00:00:00Z 13 docs/ -\n" +
			"  -rw-r--r-- 2025-01-01T00:00:00Z 13 hello.txt " + id.String() + "\n"))
	if err != nil {
		log.Fatal(err)
	}
	v, err := c4fs.New(m, s)
	if err != nil {
		log.Fatal(err)
	}
	b, _ := v.ReadFile("/docs/hello.txt")
	fmt.Print(string(b))
	// Output: hello, world
}

// A shared cache for stores that stream (S3, say): content hydrates into
// it, verified, the first time a file is read at random.
func ExampleNewCache() {
	dir, _ := os.MkdirTemp("", "c4fs-cache")
	defer os.RemoveAll(dir)
	cache, err := c4fs.NewCache(dir)
	if err != nil {
		log.Fatal(err)
	}
	s := store.NewRAM() // its readers stream, so reads hydrate
	id, _ := s.Put(strings.NewReader("cached\n"))
	m := c4m.NewManifest()
	m.AddEntry(&c4m.Entry{Name: "f", Mode: 0o644, Size: 7, Timestamp: c4m.NullTimestamp(), C4ID: id})
	v, _ := c4fs.New(m, s, c4fs.WithCache(cache))

	fmt.Println(cache.Has(id))
	v.ReadFile("/f")
	fmt.Println(cache.Has(id))
	// Output:
	// false
	// true
}

// Opening a stored manifest by ID, as c4 id -s prints it. Directories
// stored as one-level records load from the store when first visited.
func ExampleOpen() {
	dir, _ := os.MkdirTemp("", "c4store")
	defer os.RemoveAll(dir)
	s, _ := store.NewTreeStore(dir)

	file, _ := s.Put(strings.NewReader("package main\n"))
	m := c4m.NewManifest()
	m.AddEntry(&c4m.Entry{Name: "main.go", Mode: 0o644, Size: 13, Timestamp: c4m.NullTimestamp(), C4ID: file})
	text, _ := c4m.Marshal(m)
	id, _ := s.Put(strings.NewReader(string(text)))

	v, err := c4fs.Open(s, id)
	if err != nil {
		log.Fatal(err)
	}
	info, _ := v.Stat("/main.go")
	fmt.Println(info.Name(), info.Size(), info.Mode())
	fmt.Println(info.Sys().(*c4m.Entry).C4ID == file)
	// Output:
	// main.go 13 -rw-r--r--
	// true
}

// A View as the shared read-only lower of a workspace: writes go to the
// upper layer and the View is unchanged.
func Example_unionLower() {
	dir, _ := os.MkdirTemp("", "c4store")
	defer os.RemoveAll(dir)
	s, _ := store.NewTreeStore(dir)
	id, _ := s.Put(strings.NewReader("v1\n"))
	m := c4m.NewManifest()
	m.AddEntry(&c4m.Entry{Name: "VERSION", Mode: 0o644, Size: 3, Timestamp: c4m.NullTimestamp(), C4ID: id})
	lower, _ := c4fs.New(m, s)

	upper, _ := memfs.NewFS()
	ws := unionfs.New(unionfs.WithWritableLayer(upper), unionfs.WithReadOnlyLayer(lower)).SymlinkFileSystem()
	f, _ := ws.Create("/VERSION")
	f.Write([]byte("v2\n"))
	f.Close()

	inWorkspace, _ := ws.ReadFile("/VERSION")
	inView, _ := lower.ReadFile("/VERSION")
	fmt.Print(string(inWorkspace), string(inView))
	err := lower.Remove("/VERSION")
	fmt.Println(err)
	// Output:
	// v2
	// v1
	// remove /VERSION: read-only file system
}

// Listing a directory: entries come sorted by name, and their Sys value
// is the c4m entry.
func ExampleView_ReadDir() {
	m, _ := c4m.Unmarshal([]byte(
		"-rw-r--r-- 2025-01-01T00:00:00Z 0 b.txt -\n" +
			"-rw-r--r-- 2025-01-01T00:00:00Z 0 a.txt -\n" +
			"lrwxrwxrwx 2025-01-01T00:00:00Z 5 c -> a.txt -\n"))
	v, _ := c4fs.New(m, nil)
	ents, _ := v.ReadDir("/")
	for _, e := range ents {
		fmt.Println(e.Name(), e.Type()&fs.ModeSymlink != 0)
	}
	// Output:
	// a.txt false
	// b.txt false
	// c true
}
