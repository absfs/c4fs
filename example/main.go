// Command example browses a tree the c4 CLI stored, without
// materialising it: c4 id -s writes the content and prints the tree's
// ID, and a c4fs.View reads both straight from the store.
//
//	c4 id -s mytree/                  # prints "stored: c4..."
//	go run ./example c4...            # list the whole tree
//	go run ./example c4... src/main.go  # print one file
//	go run ./example -w c4...         # write in a workspace and capture
//
// The store is the one the c4 CLI uses: C4_STORE, or ~/.c4/config. The
// -w mode writes to it: the captured content of NOTES.txt, a few bytes,
// is put into that store, as c4 id -s would put it. Nothing else is
// changed, and the stored tree itself is never modified.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
	"github.com/absfs/absfs"
	"github.com/absfs/c4fs"
	"github.com/absfs/memfs"
)

// errUsage reports bad arguments, after the usage has been printed.
var errUsage = errors.New("usage")

func main() {
	os.Exit(exitCode(run(os.Args[1:], os.Stdout, os.Stderr), os.Stderr))
}

// exitCode reports err and returns the command's exit status: 0, 1 for
// a failure, 2 for bad arguments (whose usage run has printed).
func exitCode(err error, stderr io.Writer) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		return 2
	}
	fmt.Fprintln(stderr, "example:", err)
	return 1
}

// run is the command, writing what it prints to stdout and its usage to
// stderr.
func run(args []string, stdout, stderr io.Writer) error {
	fl := flag.NewFlagSet("example", flag.ContinueOnError)
	fl.SetOutput(stderr)
	workspace := fl.Bool("w", false, "write a file in a workspace over the view, then capture it into the c4 store")
	fl.Usage = func() {
		fmt.Fprintln(stderr, "usage: example [-w] <c4 id> [path]")
		fl.PrintDefaults()
	}
	if err := fl.Parse(args); err != nil {
		return errUsage
	}
	if fl.NArg() < 1 {
		fl.Usage()
		return errUsage
	}
	id, err := c4.Parse(fl.Arg(0))
	if err != nil {
		return err
	}
	v, err := c4fs.OpenConfigured(id)
	if err != nil {
		return err
	}
	switch {
	case *workspace:
		return demoWorkspace(stdout, v)
	case fl.NArg() > 1:
		f, err := v.Open("/" + fl.Arg(1))
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(stdout, f)
		return err
	}
	return list(stdout, v, "/")
}

// list prints every name under dir with its mode, size and the start of
// its C4 ID. Directories stored as records load as the walk reaches them.
func list(out io.Writer, v *c4fs.View, dir string) error {
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
		fmt.Fprintf(out, "%v %10d %-12s %s\n", info.Mode(), info.Size(), id, p)
		if e.IsDir() {
			if err := list(out, v, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// demoWorkspace makes a writable workspace over the view, as an agent
// gets, writes one file, and captures the change into the c4 CLI's
// store: it prints the changeset, which c4 log and c4 patch read, and
// shows the view itself unchanged.
func demoWorkspace(out io.Writer, v *c4fs.View) error {
	scratch, err := memfs.NewFS()
	if err != nil {
		return err
	}
	w, err := c4fs.NewWorkspace(v, scratch)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := write(w, "/NOTES.txt", "written in the workspace\n"); err != nil {
		return err
	}
	st, err := store.OpenStore()
	if err != nil {
		return err
	}
	cs, err := w.Capture(st)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "captured %s: %d file(s), %d bytes hashed\n", cs.ID, cs.Files, cs.Bytes)
	if _, err := cs.WriteTo(out); err != nil {
		return err
	}
	if _, err := v.Stat("/NOTES.txt"); err != nil {
		fmt.Fprintln(out, "view: NOTES.txt absent, as it should be")
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
