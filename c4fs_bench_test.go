package c4fs

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/Avalanche-io/c4/c4m"
	"github.com/Avalanche-io/c4/store"
)

// Benchmark Store Operations

func BenchmarkStoreAdapterPut(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	data := bytes.Repeat([]byte("test"), 256) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := adapter.Put(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreAdapterPut_1MB(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	data := bytes.Repeat([]byte("test"), 256*1024) // 1MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := adapter.Put(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreAdapterGet(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	data := bytes.Repeat([]byte("test"), 256) // 1KB
	id, _ := adapter.Put(bytes.NewReader(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := adapter.Get(id)
		if err != nil {
			b.Fatal(err)
		}
		io.ReadAll(rc)
		rc.Close()
	}
}

func BenchmarkStoreAdapterHas(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	data := bytes.Repeat([]byte("test"), 256)
	id, _ := adapter.Put(bytes.NewReader(data))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.Has(id)
	}
}

// Benchmark File Operations

func BenchmarkWriteFile(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	data := bytes.Repeat([]byte("test"), 256) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), data, 0644)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteFile_1MB(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	data := bytes.Repeat([]byte("test"), 256*1024) // 1MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), data, 0644)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadFile(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	data := bytes.Repeat([]byte("test"), 256) // 1KB
	c4fs.WriteFile("test.txt", data, 0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadFile("test.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.Stat("test.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExists(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	c4fs.WriteFile("test.txt", []byte("content"), 0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c4fs.Exists("test.txt")
	}
}

// Benchmark Directory Operations

func BenchmarkMkdir(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.Mkdir(fmt.Sprintf("dir%d", i), 0755)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMkdirAll(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.MkdirAll(fmt.Sprintf("a/b/c/d/%d", i), 0755)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDir(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with 100 files
	c4fs.Mkdir("testdir", 0755)
	for i := 0; i < 100; i++ {
		c4fs.WriteFile(fmt.Sprintf("testdir/file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadDir("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDir_1000Files(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create directory with 1000 files
	c4fs.Mkdir("testdir", 0755)
	for i := 0; i < 1000; i++ {
		c4fs.WriteFile(fmt.Sprintf("testdir/file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadDir("testdir")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Manifest Operations

func BenchmarkFlatten(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create filesystem with some files
	for i := 0; i < 100; i++ {
		c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c4fs.Flatten()
	}
}

func BenchmarkFlatten_1000Files(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create filesystem with 1000 files
	for i := 0; i < 1000; i++ {
		c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c4fs.Flatten()
	}
}

// Benchmark Symlink Operations

func BenchmarkSymlink(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	c4fs.WriteFile("target.txt", []byte("content"), 0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.Symlink("target.txt", fmt.Sprintf("link%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadLink(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	c4fs.WriteFile("target.txt", []byte("content"), 0644)
	c4fs.Symlink("target.txt", "link.txt")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadLink("link.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolveSymlink(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)
	c4fs.WriteFile("target.txt", []byte("content"), 0644)
	c4fs.Symlink("target.txt", "link1")
	c4fs.Symlink("link1", "link2")
	c4fs.Symlink("link2", "link3")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.Stat("link3")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Remove Operations

func BenchmarkRemove(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Pre-create files
	for i := 0; i < b.N; i++ {
		c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.Remove(fmt.Sprintf("file%d.txt", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoveAll(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Pre-create directory trees
	for i := 0; i < b.N; i++ {
		c4fs.MkdirAll(fmt.Sprintf("dir%d/a/b/c", i), 0755)
		for j := 0; j < 10; j++ {
			c4fs.WriteFile(fmt.Sprintf("dir%d/a/b/c/file%d.txt", i, j), []byte("content"), 0644)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.RemoveAll(fmt.Sprintf("dir%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Rename Operations

func BenchmarkRename(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Pre-create files
	for i := 0; i < b.N; i++ {
		c4fs.WriteFile(fmt.Sprintf("old%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.Rename(fmt.Sprintf("old%d.txt", i), fmt.Sprintf("new%d.txt", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenameDirectory(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Pre-create directories with files
	for i := 0; i < b.N; i++ {
		c4fs.MkdirAll(fmt.Sprintf("olddir%d/subdir", i), 0755)
		for j := 0; j < 50; j++ {
			c4fs.WriteFile(fmt.Sprintf("olddir%d/subdir/file%d.txt", i, j), []byte("content"), 0644)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c4fs.Rename(fmt.Sprintf("olddir%d", i), fmt.Sprintf("newdir%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Glob Operations

func BenchmarkGlob(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create files with various extensions
	for i := 0; i < 100; i++ {
		c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), []byte("content"), 0644)
		c4fs.WriteFile(fmt.Sprintf("data%d.json", i), []byte("{}"), 0644)
		c4fs.WriteFile(fmt.Sprintf("doc%d.md", i), []byte("# Doc"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.Glob("*.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Copy-on-Write (Layer vs Base)

func BenchmarkStatWithLayer(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base with 1000 files
	base := c4m.NewManifest()
	for i := 0; i < 1000; i++ {
		content := []byte("content")
		id, _ := adapter.Put(bytes.NewReader(content))
		base.AddEntry(&c4m.Entry{
			Mode: 0644,
			Size: int64(len(content)),
			Name: fmt.Sprintf("file%d.txt", i),
			C4ID: id,
		})
	}

	c4fs := New(base, adapter)

	// Add 100 files to layer
	for i := 1000; i < 1100; i++ {
		c4fs.WriteFile(fmt.Sprintf("file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stat file from layer
		_, err := c4fs.Stat("file1050.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStatWithBase(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base with 1000 files
	base := c4m.NewManifest()
	for i := 0; i < 1000; i++ {
		content := []byte("content")
		id, _ := adapter.Put(bytes.NewReader(content))
		base.AddEntry(&c4m.Entry{
			Mode: 0644,
			Size: int64(len(content)),
			Name: fmt.Sprintf("file%d.txt", i),
			C4ID: id,
		})
	}

	c4fs := New(base, adapter)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stat file from base
		_, err := c4fs.Stat("file500.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Deep Directory Hierarchy

func BenchmarkDeepDirectoryAccess(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())
	c4fs := New(nil, adapter)

	// Create deep directory hierarchy
	c4fs.MkdirAll("a/b/c/d/e/f/g/h/i/j", 0755)
	c4fs.WriteFile("a/b/c/d/e/f/g/h/i/j/file.txt", []byte("content"), 0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadFile("a/b/c/d/e/f/g/h/i/j/file.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark Large Manifest Operations

func BenchmarkReadDir_MixedBaseAndLayer(b *testing.B) {
	adapter := NewStoreAdapter(store.NewRAM())

	// Create base with 500 files
	base := c4m.NewManifest()
	for i := 0; i < 500; i++ {
		content := []byte("content")
		id, _ := adapter.Put(bytes.NewReader(content))
		base.AddEntry(&c4m.Entry{
			Mode: 0644,
			Size: int64(len(content)),
			Name: fmt.Sprintf("dir/file%d.txt", i),
			C4ID: id,
		})
	}

	c4fs := New(base, adapter)

	// Add 500 files to layer
	for i := 500; i < 1000; i++ {
		c4fs.WriteFile(fmt.Sprintf("dir/file%d.txt", i), []byte("content"), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c4fs.ReadDir("dir")
		if err != nil {
			b.Fatal(err)
		}
	}
}
