package c4fs

import (
	"os"
	"path"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/absfs/absfs"
	"github.com/absfs/absnfs"
	"github.com/absfs/memfs"
	"github.com/absfs/switchfs"
)

// fullRouter routes every path method through switchfs's router with
// the rewrite applied, as C4's NFS prototype does: the methods router
// (union_test.go) leaves to switchfs, which would not rewrite them.
type fullRouter struct{ *router }

func (r fullRouter) Chmod(p string, m os.FileMode) error {
	b, np := r.backend(p)
	return b.Chmod(np, m)
}
func (r fullRouter) Chtimes(p string, a, m time.Time) error {
	b, np := r.backend(p)
	return b.Chtimes(np, a, m)
}
func (r fullRouter) Chown(p string, u, g int) error {
	b, np := r.backend(p)
	return b.Chown(np, u, g)
}
func (r fullRouter) Lchown(p string, u, g int) error {
	b, np := r.backend(p)
	return b.Lchown(np, u, g)
}
func (r fullRouter) Truncate(p string, size int64) error {
	b, np := r.backend(p)
	return b.Truncate(np, size)
}
func (r fullRouter) Symlink(old, p string) error {
	b, np := r.backend(p)
	return b.Symlink(old, np)
}
func (r fullRouter) MkdirAll(p string, perm os.FileMode) error {
	b, np := r.backend(p)
	return b.MkdirAll(np, perm)
}
func (r fullRouter) RemoveAll(p string) error {
	b, np := r.backend(p)
	return b.RemoveAll(np)
}
func (r fullRouter) ReadFile(p string) ([]byte, error) {
	b, np := r.backend(p)
	return b.ReadFile(np)
}

// nfsServer is the prototype's server with c4fs workspaces in place of
// unions over a materialised base: one memfs root listing workspaces,
// switchfs routing /<name>/... to that workspace, all over one View.
type nfsServer struct {
	mu   sync.Mutex
	root *memfs.FileSystem
	sw   fullRouter
	base *View
	ws   map[string]*Workspace
}

func newNFSServer(t testing.TB, base *View) *nfsServer {
	t.Helper()
	root, err := memfs.NewFS()
	if err != nil {
		t.Fatal(err)
	}
	sw, err := switchfs.New(switchfs.WithDefault(root))
	if err != nil {
		t.Fatal(err)
	}
	return &nfsServer{root: root, sw: fullRouter{&router{SwitchFS: sw, def: root}}, base: base, ws: map[string]*Workspace{}}
}

// add creates a workspace: an empty scratch, a root entry and a route.
// Nothing is copied.
func (s *nfsServer) add(t testing.TB, name string) *Workspace {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	w := newWS(t, s.base)
	if err := s.root.Mkdir("/"+name, 0o755); err != nil {
		t.Fatal(err)
	}
	err := s.sw.Router().AddRoute(switchfs.Route{
		Pattern:  `^/` + regexp.QuoteMeta(name) + `(/|$)`,
		Type:     switchfs.PatternRegex,
		Backend:  w,
		Priority: 100,
		Rewriter: switchfs.StripPrefix("/" + name),
	})
	if err != nil {
		t.Fatal(err)
	}
	s.ws[name] = w
	return w
}

var _ absfs.SymlinkFileSystem = fullRouter{}

// TestNFSWorkspaces drives workspaces through absnfs's NFS operations,
// as the NFS server would on a client's requests: every write opens the
// file, writes at an offset, closes, and sets the time; nothing ever
// closes a file in the NFS sense. Then each workspace is captured.
func TestNFSWorkspaces(t *testing.T) {
	f := newFixture(t)
	v := mustOpen(t, f.st, f.root)
	srv := newNFSServer(t, v)
	alpha := srv.add(t, "alpha")
	beta := srv.add(t, "beta")
	nfs, err := absnfs.New(srv.sw, absnfs.ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(p string) *absnfs.NFSNode {
		t.Helper()
		n, err := nfs.Lookup(p)
		if err != nil {
			t.Fatalf("lookup %s: %v", p, err)
		}
		return n
	}

	// Reads by offset come from the View.
	data, err := nfs.Read(lookup("/alpha/docs/guide.txt"), 6, 6)
	if err != nil || string(data) != "guide " {
		t.Errorf("read = %q %v", data, err)
	}
	// Writes in pieces, as a client sends them.
	readme := lookup("/alpha/README.md")
	for i, piece := range []string{"HE", "LL", "O\n"} {
		if _, err := nfs.Write(readme, int64(2*i), []byte(piece)); err != nil {
			t.Fatal(err)
		}
	}
	if got := wsRead(t, alpha, "/README.md"); got != "HELLO\n" {
		t.Errorf("written = %q", got)
	}
	// Create, write, rename, remove, mkdir, symlink.
	src := lookup("/alpha/src")
	nf, err := nfs.Create(src, "gen.go", absnfs.NewNFSAttrs(0o644, 0, time.Now(), time.Now(), 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	nfs.Write(nf, 0, []byte("package main // generated\n"))
	if err := nfs.Rename(lookup("/alpha/src"), "lib", lookup("/alpha"), "lib"); err != nil {
		t.Fatal(err)
	}
	if err := nfs.Remove(lookup("/alpha"), "empty.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := nfs.Symlink(lookup("/alpha"), "golink", "src/gen.go", absnfs.NewNFSAttrs(os.ModeSymlink|0o777, 0, time.Now(), time.Now(), 0, 0)); err != nil {
		t.Fatal(err)
	}
	if target, err := nfs.Readlink(lookup("/alpha/golink")); err != nil || target != "src/gen.go" {
		t.Errorf("readlink = %q %v", target, err)
	}
	ents, err := nfs.ReadDir(lookup("/alpha"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range ents {
		got = append(got, path.Base(e.Name()))
	}
	want := []string{"README.md", "abslink", "dangling", "dirlink", "docs", "emptydir", "golink", "lib", "link", "name with space", "src"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("alpha lists %v\nwant %v", got, want)
	}
	// SETATTR: a mode change.
	node := lookup("/alpha/lib/a.go")
	attrs, _ := nfs.GetAttr(node)
	set := *attrs
	set.Mode = 0o600
	if err := nfs.SetAttr(node, &set); err != nil {
		t.Fatal(err)
	}

	// beta saw none of it.
	if got := wsRead(t, beta, "/README.md"); got != "hello\n" {
		t.Errorf("beta readme = %q", got)
	}
	if _, err := nfs.Lookup("/beta/lib"); err == nil {
		t.Error("beta sees alpha's rename")
	}

	csA := capture(t, alpha, f.st)
	csB := capture(t, beta, f.st)
	if !csB.Empty() {
		t.Errorf("beta changed:\n%s", csB.Text())
	}
	m := applyAll(t, f.m, csA)
	for name, wantID := range map[string]string{
		"README.md":  idOf("HELLO\n").String(),
		"src/gen.go": idOf("package main // generated\n").String(),
		"lib/a.go":   idOf(fixtureFiles["src/lib/a.go"]).String(),
	} {
		e := m.GetEntry(name)
		if e == nil || e.C4ID.String() != wantID {
			t.Errorf("%s = %+v", name, e)
		}
	}
	if e := m.GetEntry("lib/a.go"); e == nil || e.Mode.Perm() != 0o600 {
		t.Errorf("chmod over NFS = %+v", e)
	}
	if m.GetEntry("empty.txt") != nil || m.GetEntry("src/lib/") != nil {
		t.Error("removed names remain")
	}
	if csA.Files != 2 {
		t.Errorf("hashed %d files, want README.md and gen.go", csA.Files)
	}
}
