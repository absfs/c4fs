//go:build windows

package c4fs

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestOpenSharedAllowsRename checks that a file opened by openShared
// can be moved and removed while open, as on other systems, and that
// the handle still reads its bytes. TestOpenSharedDuringRename checks
// the opposite order: an open while a rename holds the file.
func TestOpenSharedAllowsRename(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "obj")
	if err := os.WriteFile(p, []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := openShared(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	moved := filepath.Join(dir, "moved")
	if err := os.Rename(p, moved); err != nil {
		t.Fatalf("move of an open object: %v", err)
	}
	if b, err := io.ReadAll(f); err != nil || string(b) != "first" {
		t.Errorf("open handle reads %q %v", b, err)
	}
	g, err := openShared(moved)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := os.Remove(moved); err != nil {
		t.Errorf("remove of an open object: %v", err)
	}
	if _, err := openShared(filepath.Join(dir, "missing")); !os.IsNotExist(err) {
		t.Errorf("missing file = %v", err)
	}
	if _, err := openShared("bad\x00name"); err == nil {
		t.Error("NUL in name accepted")
	}
}

// TestOpenSharedSharingViolation checks that an open refused by an
// exclusive holder is classified as a sharing violation.
func TestOpenSharedSharingViolation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "locked")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	name, _ := syscall.UTF16PtrFromString(p)
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(h)
	_, err = openShared(p)
	if !isSharingViolation(err) {
		t.Errorf("open of an exclusively held file = %v, want a sharing violation", err)
	}
}

// winDelete is the DELETE access right, which package syscall does not
// name.
const winDelete = 0x00010000

// TestOpenSharedDuringRename checks the cause of the Windows flake
// directly. A rename (MoveFileEx) holds a handle with DELETE access on
// the file it moves, sharing everything, until it returns. Inside that
// window os.Open, which does not share delete access, is refused with
// a sharing violation; openShared, which does, opens and reads.
func TestOpenSharedDuringRename(t *testing.T) {
	p := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(p, []byte("published"), 0o644); err != nil {
		t.Fatal(err)
	}
	name, _ := syscall.UTF16PtrFromString(p)
	h, err := syscall.CreateFile(name, winDelete|syscall.SYNCHRONIZE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(h)

	if f, err := os.Open(p); err == nil {
		f.Close()
		t.Error("os.Open succeeded while a handle held delete access; the emulated rename window is not real")
	} else if !isSharingViolation(err) {
		t.Errorf("os.Open = %v, want a sharing violation", err)
	}
	c, err := openLocal(p)
	if err != nil {
		t.Fatalf("openLocal while a handle held delete access: %v", err)
	}
	defer c.close()
	b := make([]byte, c.size)
	if n, err := c.ra.ReadAt(b, 0); n != len(b) || (err != nil && err != io.EOF) || string(b) != "published" {
		t.Errorf("read %q %d %v", b, n, err)
	}
}

func TestLongPath(t *testing.T) {
	if got := longPath(`C:\short`); got != `C:\short` {
		t.Errorf("short path = %q", got)
	}
	long := `C:\` + strings.Repeat(`d\`, 130) + "f"
	if got := longPath(long); !strings.HasPrefix(got, `\\?\C:\`) {
		t.Errorf("long path = %q", got)
	}
	if got := longPath(`\\server\share\short`); got != `\\server\share\short` {
		t.Errorf("short UNC path = %q", got)
	}
	unc := `\\server\share\` + strings.Repeat("x", 260)
	if got, want := longPath(unc), `\\?\UNC\server\share\`+strings.Repeat("x", 260); got != want {
		t.Errorf("UNC path = %q, want %q", got, want)
	}
	for _, p := range []string{`\\?\C:\` + strings.Repeat("x", 260), `\\.\pipe\` + strings.Repeat("x", 260)} {
		if got := longPath(p); got != p {
			t.Errorf("extended or device path %q became %q", p, got)
		}
	}
	// A relative path shorter than the limit whose absolute form is
	// longer is prefixed, measured as os.Open measures it.
	rel := strings.Repeat(`d\`, 120) + "f"
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := longPath(rel), `\\?\`+filepath.Join(wd, rel); got != want {
		t.Errorf("long relative path = %q, want %q", got, want)
	}
	if got := longPath(`short\rel`); got != `short\rel` {
		t.Errorf("short relative path = %q", got)
	}
	// A long path that really exists opens through the prefix.
	dir := t.TempDir()
	deep := dir
	for len(deep) < 300 {
		deep = filepath.Join(deep, strings.Repeat("d", 40))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Skipf("cannot make a long path here: %v", err)
	}
	p := filepath.Join(deep, "f")
	if err := os.WriteFile(p, []byte("deep"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := openLocal(p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if c.size != 4 {
		t.Errorf("size = %d", c.size)
	}
}
