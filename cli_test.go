package c4fs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// c4CLI runs the real c4 command against a private store and home, and
// skips the test when no c4 binary is installed.
type c4CLI struct {
	t     *testing.T
	bin   string
	env   []string
	store string
}

func newC4CLI(t *testing.T) *c4CLI {
	t.Helper()
	bin, err := exec.LookPath("c4")
	if err != nil {
		t.Skip("c4 CLI not installed")
	}
	home := t.TempDir()
	st := filepath.Join(home, "store")
	return &c4CLI{t: t, bin: bin, store: st, env: append(os.Environ(), "HOME="+home, "C4_STORE="+st)}
}

func (c *c4CLI) run(args ...string) (stdout, stderr []byte) {
	c.t.Helper()
	cmd := exec.Command(c.bin, args...)
	cmd.Env = c.env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		c.t.Fatalf("c4 %v: %v\n%s", args, err, errb.Bytes())
	}
	return out.Bytes(), errb.Bytes()
}

var storedRE = regexp.MustCompile(`stored: (c4[1-9A-HJ-NP-Za-km-z]{88})`)

func (c *c4CLI) idStore(dir string, extra ...string) (*c4m.Manifest, c4.ID) {
	c.t.Helper()
	out, errOut := c.run(append(append([]string{"id", "-s"}, extra...), dir+"/")...)
	match := storedRE.FindSubmatch(errOut)
	if match == nil {
		c.t.Fatalf("no stored ID in %q", errOut)
	}
	id, err := c4.Parse(string(match[1]))
	if err != nil {
		c.t.Fatal(err)
	}
	return parse(c.t, string(out)), id
}

// TestCLIStore reads exactly what the installed c4 CLI writes: the
// store layout, the ID it prints, the root record, sequences, and a
// c4 diff patch whose base is in the store.
func TestCLIStore(t *testing.T) {
	cli := newC4CLI(t)
	dir := t.TempDir()
	files := map[string]string{}
	for k, v := range fixtureFiles {
		files[k] = v
	}
	for i, n := range []string{"0001", "0002", "0003"} {
		files["plates/shot."+n+".dpx"] = strings.Repeat("p", i+1)
	}
	writeTree(t, dir, files)
	m, self := cli.idStore(dir)
	_, seqSelf := cli.idStore(dir, "-S")

	st, err := store.NewTreeStore(cli.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("C4_STORE", cli.store)
	byConfig, err := OpenConfigured(self)
	if err != nil {
		t.Fatal(err)
	}
	views := map[string]*View{
		"printed ID":  mustOpen(t, st, self),
		"root record": mustOpen(t, st, m.ComputeC4ID()),
		"configured":  byConfig,
		"sequences":   mustOpen(t, st, seqSelf),
	}
	want := walk(t, mustNew(t, m, st))
	for how, v := range views {
		t.Run(how, func(t *testing.T) {
			for name, body := range files {
				if strings.HasSuffix(name, "/") || strings.HasPrefix(body, "->") {
					continue
				}
				if got := mustRead(t, v, "/"+name); got != body {
					t.Errorf("%s = %q, want %q", name, got, body)
				}
			}
			if how != "sequences" {
				if got := walk(t, v); !reflect.DeepEqual(got, want) {
					t.Errorf("tree differs:\n got %v\nwant %v", sorted(got), sorted(want))
				}
			}
			disk, _ := os.Stat(filepath.Join(dir, "src/main.go"))
			if fi, err := v.Stat("/src/main.go"); err != nil || fi.Mode() != disk.Mode() || fi.Size() != disk.Size() {
				t.Errorf("stat = %v %v", fi, err)
			}
		})
	}

	// c4 diff writes "<old root ID>, patch, <new root ID>"; stored, it
	// opens as the new tree with the old one as its base.
	oldC4m := filepath.Join(t.TempDir(), "old.c4m")
	os.WriteFile(oldC4m, mustMarshal(t, m), 0o644)
	writeTree(t, dir, map[string]string{"src/main.go": "package main // v2\n", "added.txt": "new"})
	os.Remove(filepath.Join(dir, "README.md"))
	newM, newSelf := cli.idStore(dir)
	newC4m := filepath.Join(t.TempDir(), "new.c4m")
	os.WriteFile(newC4m, mustMarshal(t, newM), 0o644)
	diff, _ := cli.run("diff", oldC4m, newC4m)
	if !bytes.HasPrefix(diff, []byte(m.ComputeC4ID().String()+"\n")) {
		t.Fatalf("diff does not open with the old root ID:\n%s", diff)
	}
	pid := putBytes(t, st, diff)
	v := mustOpen(t, st, pid)
	if got, want := walk(t, v), walk(t, mustOpen(t, st, newSelf)); !reflect.DeepEqual(got, want) {
		t.Errorf("diff view differs:\n got %v\nwant %v", sorted(got), sorted(want))
	}
	if got := mustRead(t, v, "/src/main.go"); got != "package main // v2\n" {
		t.Errorf("patched file = %q", got)
	}

	// The CLI reads it back the same way: c4 cat of the stored diff
	// returns the bytes we opened.
	out, _ := cli.run("cat", pid.String())
	if !bytes.Equal(out, diff) {
		t.Error("c4 cat disagrees with the stored diff")
	}
}
