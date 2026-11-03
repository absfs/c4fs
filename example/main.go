// Command example browses a tree the c4 CLI stored, without
// materialising it: c4 id -s writes the content and prints the tree's
// ID, and a c4fs.View reads both straight from the store.
//
//	c4 id -s mytree/                  # prints "stored: c4..."
//	go run ./example c4...            # list the whole tree
//	go run ./example c4... src/main.go  # print one file
//	go run ./example -w c4...         # write through a workspace union
//
// The store is the one the c4 CLI uses: C4_STORE, or ~/.c4/config.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/absfs/absfs"
	"github.com/absfs/c4fs"
	"github.com/absfs/memfs"
	"github.com/absfs/unionfs"
)

func main() {
	workspace := flag.Bool("w", false, "write a file through a unionfs workspace over the view")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: example [-w] <c4 id> [path]")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	id, err := c4.Parse(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	v, err := c4fs.OpenConfigured(id)
	if err != nil {
		log.Fatal(err)
	}

	switch {
	case *workspace:
		if err := demoWorkspace(v); err != nil {
			log.Fatal(err)
		}
	case flag.NArg() > 1:
		f, err := v.Open("/" + flag.Arg(1))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if _, err := io.Copy(os.Stdout, f); err != nil {
			log.Fatal(err)
		}
	default:
		if err := list(v, "/"); err != nil {
			log.Fatal(err)
		}
	}
}

// list prints every name under dir with its mode, size and the start of
// its C4 ID. Directories stored as records load as the walk reaches them.
func list(v *c4fs.View, dir string) error {
	ents, err := v.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		p := path.Join(dir, e.Name())
		info, err := e.Info()
		if err != nil {
			return err
		}
		id := "-"
		if ent, ok := info.Sys().(*c4m.Entry); ok && !ent.C4ID.IsNil() {
			id = ent.C4ID.String()[:12]
		}
		fmt.Printf("%v %10d %-12s %s\n", info.Mode(), info.Size(), id, p)
		if e.IsDir() {
			if err := list(v, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// demoWorkspace layers a writable in-memory upper over the view, as an
// agent workspace does, writes one file, and shows the view unchanged.
func demoWorkspace(v *c4fs.View) error {
	upper, err := memfs.NewFS()
	if err != nil {
		return err
	}
	ws := unionfs.New(unionfs.WithWritableLayer(upper), unionfs.WithReadOnlyLayer(v)).SymlinkFileSystem()
	if err := write(ws, "/NOTES.txt", "written in the workspace\n"); err != nil {
		return err
	}
	ents, err := ws.ReadDir("/")
	if err != nil {
		return err
	}
	fmt.Println("workspace:")
	for _, e := range ents {
		fmt.Println("  ", e.Name())
	}
	if _, err := v.Stat("/NOTES.txt"); err != nil {
		fmt.Println("view: NOTES.txt absent, as it should be")
	}
	return nil
}

func write(fsys absfs.FileSystem, name, body string) error {
	f, err := fsys.Create(name)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(body)); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
