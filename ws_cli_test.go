package c4fs

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/Avalanche-io/c4/store"
)

var logLineRE = regexp.MustCompile(`(?m)^(\d+)\s+(c4[1-9A-HJ-NP-Za-km-z]{88})\s`)

// TestCLIAcceptsChangesets proves that what Capture writes is what the
// installed c4 CLI reads: appended to the commit's c4m, c4 log lists
// each capture and c4 patch resolves the chain to the captured tree;
// kept apart, c4 log reads it after the base; and c4 patch builds the
// captured tree on disk from the store.
func TestCLIAcceptsChangesets(t *testing.T) {
	cli := newC4CLI(t)
	dir := t.TempDir()
	writeTree(t, dir, captureTree)
	m, self := cli.idStore(dir)
	st, err := store.NewTreeStore(cli.store)
	if err != nil {
		t.Fatal(err)
	}
	v := mustOpen(t, st, self)
	w := newWS(t, v)
	ops := wsOps{t, w}
	ops.write("src/new.go", "package main // new\n")
	ops.rename("src/lib", "lib")
	ops.remove("README.md")
	cs1 := capture(t, w, st)
	ops.appendTo("lib/a.go", "// more\n")
	ops.removeAll("gone")
	ops.chmod("docs/guide.txt", 0o600)
	cs2 := capture(t, w, st)

	tmp := t.TempDir()
	baseFile := filepath.Join(tmp, "base.c4m")
	os.WriteFile(baseFile, mustMarshal(t, m), 0o644)
	changes := append(cs1.Text(), cs2.Text()...)
	changesFile := filepath.Join(tmp, "changes.c4m")
	os.WriteFile(changesFile, changes, 0o644)
	chainFile := filepath.Join(tmp, "chain.c4m")
	os.WriteFile(chainFile, append(mustMarshal(t, m), changes...), 0o644)

	checkLog := func(out []byte) {
		t.Helper()
		lines := logLineRE.FindAllSubmatch(out, -1)
		if len(lines) != 3 {
			t.Fatalf("c4 log:\n%s", out)
		}
		for i, want := range []string{m.ComputeC4ID().String(), cs1.ID.String(), cs2.ID.String()} {
			if got := string(lines[i][2]); got != want {
				t.Errorf("c4 log line %d: %s, want %s", i+1, got, want)
			}
		}
	}
	out, _ := cli.run("log", chainFile)
	checkLog(out)
	out, _ = cli.run("log", baseFile, changesFile)
	checkLog(out)

	out, _ = cli.run("patch", chainFile)
	if got := parse(t, string(out)).ComputeC4ID(); got != cs2.ID {
		t.Errorf("c4 patch resolves to %s, want %s", got, cs2.ID)
	}
	out, _ = cli.run("patch", "-n", "2", chainFile)
	if got := parse(t, string(out)).ComputeC4ID(); got != cs1.ID {
		t.Errorf("c4 patch -n 2 resolves to %s, want %s", got, cs1.ID)
	}

	// Materialised from the store, the captured tree reads as the
	// workspace does.
	resolved := filepath.Join(tmp, "resolved.c4m")
	full, _ := cli.run("patch", chainFile)
	os.WriteFile(resolved, full, 0o644)
	outDir := filepath.Join(tmp, "out")
	os.Mkdir(outDir, 0o755)
	cli.run("patch", "--no-store", resolved, outDir)
	got := scanTree(t, outDir)
	for p, desc := range contents(t, w) {
		if strings.HasPrefix(desc, "d") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(outDir, filepath.FromSlash(p)))
		if err != nil {
			t.Errorf("%s: %v", p, err)
			continue
		}
		if !strings.HasSuffix(desc, " "+string(b)) {
			t.Errorf("%s on disk = %q, workspace %q", p, b, desc)
		}
	}
	if got.ComputeC4ID() != cs2.ID {
		t.Errorf("c4 patch materialised %s, captured %s", got.ComputeC4ID(), cs2.ID)
	}

	// The changeset, stored, is a tree c4fs opens by ID, and so does
	// the CLI's c4 cat.
	id := putBytes(t, st, changes)
	if a, b := walk(t, mustOpen(t, st, id)), walk(t, cs2.View()); !reflect.DeepEqual(a, b) {
		t.Errorf("stored changeset opens differently:\n%v\n%v", sorted(a), sorted(b))
	}
	cat, _ := cli.run("cat", id.String())
	if !bytes.Equal(cat, changes) {
		t.Error("c4 cat disagrees")
	}
}
