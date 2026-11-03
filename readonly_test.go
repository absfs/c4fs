package c4fs

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestReadOnly(t *testing.T) {
	v := newFixture(t).views(t)["record"]
	rofs := []struct {
		name string
		err  error
	}{
		{"Create existing", second(v.Create("/README.md"))},
		{"Create new", second(v.Create("/new.txt"))},
		{"OpenFile O_WRONLY", second(v.OpenFile("/README.md", os.O_WRONLY, 0))},
		{"OpenFile O_RDWR", second(v.OpenFile("/README.md", os.O_RDWR, 0))},
		{"OpenFile O_APPEND", second(v.OpenFile("/README.md", os.O_APPEND, 0))},
		{"OpenFile O_TRUNC", second(v.OpenFile("/README.md", os.O_TRUNC, 0))},
		{"Mkdir new", v.Mkdir("/newdir", 0o755)},
		{"MkdirAll new", v.MkdirAll("/a/b/c", 0o755)},
		{"Remove file", v.Remove("/README.md")},
		{"Remove dir", v.Remove("/emptydir")},
		{"Remove link", v.Remove("/dangling")},
		{"RemoveAll", v.RemoveAll("/src")},
		{"Rename", v.Rename("/README.md", "/x")},
		{"Chmod", v.Chmod("/README.md", 0o600)},
		{"Chtimes", v.Chtimes("/README.md", time.Now(), time.Now())},
		{"Chown", v.Chown("/README.md", 1, 1)},
		{"Lchown", v.Lchown("/link", 1, 1)},
		{"Truncate", v.Truncate("/README.md", 0)},
		{"Symlink", v.Symlink("/README.md", "/newlink")},
	}
	for _, c := range rofs {
		var pe *fs.PathError
		if !errors.Is(c.err, ErrReadOnly) || !errors.Is(c.err, syscall.EROFS) || !errors.As(c.err, &pe) {
			t.Errorf("%s = %v, want a PathError wrapping EROFS", c.name, c.err)
		}
	}

	specific := []struct {
		name string
		err  error
		want error
	}{
		{"Mkdir existing", v.Mkdir("/src", 0o755), fs.ErrExist},
		{"Mkdir under file", v.Mkdir("/README.md/x", 0o755), errNotDir},
		{"Mkdir under missing", v.Mkdir("/nope/x", 0o755), fs.ErrNotExist},
		{"Mkdir empty", v.Mkdir("", 0o755), fs.ErrNotExist},
		{"MkdirAll over file", v.MkdirAll("/README.md", 0o755), errNotDir},
		{"MkdirAll through file", v.MkdirAll("/README.md/x/y", 0o755), errNotDir},
		{"MkdirAll over dangling link", v.MkdirAll("/dangling", 0o755), fs.ErrExist},
		{"MkdirAll empty", v.MkdirAll("", 0o755), fs.ErrNotExist},
		{"Create excl", second(v.OpenFile("/README.md", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0)), fs.ErrExist},
		{"Create dir", second(v.OpenFile("/src", os.O_WRONLY, 0)), errIsDir},
		{"Write missing", second(v.OpenFile("/nope", os.O_WRONLY, 0)), fs.ErrNotExist},
		{"Create under missing", second(v.Create("/nope/x")), fs.ErrNotExist},
		{"Remove missing", v.Remove("/nope"), fs.ErrNotExist},
		{"Rename missing", v.Rename("/nope", "/x"), fs.ErrNotExist},
		{"Chmod missing", v.Chmod("/nope", 0), fs.ErrNotExist},
		{"Chmod dangling", v.Chmod("/dangling", 0), fs.ErrNotExist},
		{"Symlink existing", v.Symlink("x", "/link"), fs.ErrExist},
		{"RemoveAll through file", v.RemoveAll("/README.md/x"), errNotDir},
	}
	for _, c := range specific {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.err, c.want)
		}
	}
	if err := v.MkdirAll("/src/lib", 0o755); err != nil {
		t.Errorf("MkdirAll existing = %v", err)
	}
	if err := v.MkdirAll("/dirlink", 0o755); err != nil {
		t.Errorf("MkdirAll through link = %v", err)
	}
	if err := v.RemoveAll("/nope"); err != nil {
		t.Errorf("RemoveAll missing = %v", err)
	}

	f, err := v.Open("/README.md")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for name, err := range map[string]error{
		"Write":       second(f.Write([]byte("x"))),
		"WriteAt":     second(f.WriteAt([]byte("x"), 0)),
		"WriteString": second(f.WriteString("x")),
		"Truncate":    f.Truncate(0),
	} {
		if !errors.Is(err, ErrReadOnly) {
			t.Errorf("File.%s = %v", name, err)
		}
	}
	if got := mustRead(t, v, "/README.md"); got != "hello\n" {
		t.Errorf("content changed: %q", got)
	}
}

func second[T any](_ T, err error) error { return err }
