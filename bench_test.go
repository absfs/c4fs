package c4fs

import (
	"fmt"
	"io/fs"
	"runtime"
	"testing"
	"time"

	"github.com/Avalanche-io/c4"
	"github.com/Avalanche-io/c4/c4m"
)

// synthetic returns a nested manifest of dirs*subdirs directories, each
// holding files files: 100 x 10 x 100 is 100,000 files and 1,100
// directories, the size of a large source tree.
func synthetic(dirs, subdirs, files int) *c4m.Manifest {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	m := &c4m.Manifest{Version: "1.0", Entries: make([]*c4m.Entry, 0, dirs*(1+subdirs*(1+files)))}
	var id c4.ID
	for d := 0; d < dirs; d++ {
		m.Entries = append(m.Entries, &c4m.Entry{Name: fmt.Sprintf("dir%03d/", d), Mode: fs.ModeDir | 0o755, Timestamp: ts, Size: -1})
		for s := 0; s < subdirs; s++ {
			m.Entries = append(m.Entries, &c4m.Entry{Name: fmt.Sprintf("sub%02d/", s), Depth: 1, Mode: fs.ModeDir | 0o755, Timestamp: ts, Size: -1})
			for f := 0; f < files; f++ {
				id[0], id[1], id[2] = byte(d), byte(s), byte(f)
				m.Entries = append(m.Entries, &c4m.Entry{Name: fmt.Sprintf("file%03d.go", f), Depth: 2, Mode: 0o644, Timestamp: ts, Size: 1000, C4ID: id})
			}
		}
	}
	return m
}

func BenchmarkBuild100k(b *testing.B) {
	m := synthetic(100, 10, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := New(m, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(m.Entries)), "entries")
}

func BenchmarkStat100k(b *testing.B) {
	v, _ := New(synthetic(100, 10, 100), nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := v.Stat("/dir057/sub03/file042.go"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStatMissing100k(b *testing.B) {
	v, _ := New(synthetic(100, 10, 100), nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := v.Stat("/dir057/sub03/nope.go"); err == nil {
			b.Fatal("found")
		}
	}
}

func BenchmarkReadDir100k(b *testing.B) {
	v, _ := New(synthetic(100, 10, 100), nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ents, err := v.ReadDir("/dir057/sub03")
		if err != nil || len(ents) != 100 {
			b.Fatal(len(ents), err)
		}
	}
}

// BenchmarkReadDirFlat lists one directory of 100,000 files: the cost is
// the listing itself, with no scan of anything else.
func BenchmarkReadDirFlat100k(b *testing.B) {
	v, _ := New(synthetic(1, 1, 100000), nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ents, err := v.ReadDir("/dir000/sub00")
		if err != nil || len(ents) != 100000 {
			b.Fatal(len(ents), err)
		}
	}
}

// TestIndexMemory measures what the index retains beyond the manifest
// it was built from, and checks it stays lean.
func TestIndexMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a 100k-entry manifest")
	}
	m := synthetic(100, 10, 100)
	before := heap()
	v, err := New(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	after := heap()
	per := float64(after-before) / float64(len(m.Entries))
	t.Logf("index of %d entries: %.1f MB, %.0f bytes per entry beyond the manifest", len(m.Entries), float64(after-before)/1e6, per)
	if per > 128 {
		t.Errorf("index costs %.0f bytes per entry", per)
	}
	runtime.KeepAlive(v)
	runtime.KeepAlive(m)
}

func heap() uint64 {
	runtime.GC()
	runtime.GC()
	var s runtime.MemStats
	runtime.ReadMemStats(&s)
	return s.HeapAlloc
}
