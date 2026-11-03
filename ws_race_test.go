package c4fs

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

// TestWorkspaceConcurrent runs writers, readers, renames and captures
// at once. Every changeset must apply to the one before it, and the
// chain must resume to exactly the workspace.
func TestWorkspaceConcurrent(t *testing.T) {
	f := newWSFixture(t)
	// memfs v1.1.0 is not safe for a write and a stat of one file at
	// once; a host folder is, and is what a server would use.
	w, err := NewWorkspace(f.v, folderScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	const writers, rounds = 8, 40
	var wg, others sync.WaitGroup
	stop := make(chan struct{})
	// Writers ask for a capture every few rounds. A capture holds the
	// write lock, so an unthrottled capturer starves the writers on few
	// CPUs; this keeps the run short whatever GOMAXPROCS is.
	tick := make(chan struct{}, 1)

	// Every writer writes its own bytes into one View file, at its own
	// offset: one copy-up, shared.
	guide := len(fixtureFiles["docs/guide.txt"])
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			dir := fmt.Sprintf("/g%d", g)
			if err := w.Mkdir(dir, 0o755); err != nil {
				t.Error(err)
				return
			}
			h, err := w.OpenFile("/docs/guide.txt", os.O_WRONLY, 0)
			if err != nil {
				t.Error(err)
				return
			}
			h.WriteAt([]byte{byte('A' + g)}, int64(g))
			h.Close()
			for i := 0; i < rounds; i++ {
				name := fmt.Sprintf("%s/f%d", dir, i)
				wsWrite(t, w, name, fmt.Sprintf("%d-%d", g, i))
				switch i % 4 {
				case 1:
					w.Rename(name, name+".renamed")
				case 2:
					w.Remove(name)
				case 3:
					w.Chmod(name, 0o600)
				}
				if i%10 == 9 {
					w.Rename(dir, dir+"x")
					w.Rename(dir+"x", dir)
				}
				if i%4 == 0 {
					select {
					case tick <- struct{}{}:
					default:
					}
				}
			}
		}(g)
	}
	// Readers walk the tree and read the View's files.
	for r := 0; r < 4; r++ {
		others.Add(1)
		go func() {
			defer others.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.ReadDir("/")
				w.ReadFile("/src/lib/a.go")
				w.Stat("/docs/guide.txt")
				if h, err := w.Open("/g0"); err == nil {
					h.Readdirnames(-1)
					h.Close()
				}
				runtime.Gosched()
			}
		}()
	}
	// A capturer captures throughout.
	var css []*Changeset
	var capMu sync.Mutex
	capDone := make(chan struct{})
	go func() {
		defer close(capDone)
		for {
			select {
			case <-stop:
				return
			case <-tick:
			}
			cs, err := w.Capture(f.st)
			if err != nil {
				t.Error(err)
				return
			}
			capMu.Lock()
			css = append(css, cs)
			capMu.Unlock()
		}
	}()
	// Wait for the writers, then stop the rest.
	wg.Wait()
	close(stop)
	others.Wait()
	<-capDone
	css = append(css, capture(t, w, f.st))

	got := wsRead(t, w, "/docs/guide.txt")
	for g := 0; g < writers; g++ {
		if got[g] != byte('A'+g) {
			t.Errorf("byte %d = %q", g, got[g])
		}
	}
	if len(got) != guide {
		t.Errorf("guide is %d bytes", len(got))
	}
	applyAll(t, f.m, css...)
	var chain bytes.Buffer
	for _, cs := range css {
		cs.WriteTo(&chain)
	}
	r, err := Resume(f.v, &chain, f.st, newScratch(t))
	if err != nil {
		t.Fatal(err)
	}
	if a, b := contents(t, r), contents(t, w); !reflect.DeepEqual(a, b) {
		t.Errorf("resumed differs:\n%v\n%v", sorted(a), sorted(b))
	}
	t.Logf("%d captures", len(css))
}
