package main

import (
	"fmt"
	"log"
	"os"

	"github.com/absfs/c4fs"
	"github.com/Avalanche-io/c4/c4m"
)

func main() {
	// Create a memory-based C4 store
	store := c4fs.NewMemoryStore()

	// Create a new C4 filesystem
	fs := c4fs.New(nil, store)

	fmt.Println("=== C4FS Demo ===\n")

	// 1. Write files (dehydration)
	fmt.Println("1. Writing files to C4FS...")

	err := fs.WriteFile("readme.md", []byte("# Welcome to C4FS\nContent-addressable filesystem"), 0644)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Created readme.md")

	err = fs.WriteFile("config.json", []byte(`{"version": "1.0", "enabled": true}`), 0644)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Created config.json")

	// 2. Demonstrate deduplication
	fmt.Println("\n2. Testing deduplication...")

	duplicateContent := []byte("Same content")
	err = fs.WriteFile("file1.txt", duplicateContent, 0644)
	if err != nil {
		log.Fatal(err)
	}

	err = fs.WriteFile("file2.txt", duplicateContent, 0644)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("   ✓ Created file1.txt and file2.txt with same content\n")
	fmt.Printf("   ✓ Store size: %d (only 3 unique content blocks)\n", store.Size())

	// 3. Read files (hydration)
	fmt.Println("\n3. Reading files from C4FS...")

	data, err := fs.ReadFile("readme.md")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   ✓ readme.md: %q\n", string(data))

	// 4. Create directory
	fmt.Println("\n4. Creating directory...")

	err = fs.Mkdir("docs", 0755)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Created docs/")

	// 5. Use file handle for writing
	fmt.Println("\n5. Using file handle for writing...")

	f, err := fs.Create("notes.txt")
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.WriteString("Line 1\n")
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.WriteString("Line 2\n")
	if err != nil {
		log.Fatal(err)
	}

	err = f.Close() // Dehydration happens on close
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Created notes.txt via file handle")

	// 6. Create snapshot
	fmt.Println("\n6. Creating snapshot...")

	snapshot := fs.Flatten()
	fmt.Printf("   ✓ Snapshot has %d entries\n", len(snapshot.Entries))

	// Save snapshot to file
	file, err := os.Create("snapshot.c4m")
	if err != nil {
		log.Fatal(err)
	}

	_, err = snapshot.WriteTo(file)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
	fmt.Println("   ✓ Saved snapshot to snapshot.c4m")

	// 7. Load snapshot and create new filesystem
	fmt.Println("\n7. Loading snapshot and creating new filesystem...")

	snapshotData, err := os.Open("snapshot.c4m")
	if err != nil {
		log.Fatal(err)
	}

	loadedManifest, err := c4m.GenerateFromReader(snapshotData)
	if err != nil {
		log.Fatal(err)
	}
	snapshotData.Close()

	// Create new filesystem from snapshot
	fs2 := c4fs.New(loadedManifest, store)

	// Verify we can read files from restored snapshot
	data, err = fs2.ReadFile("readme.md")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   ✓ Restored filesystem can read readme.md: %q\n", string(data)[:30]+"...")

	// 8. Layer on top of snapshot
	fmt.Println("\n8. Making changes on top of snapshot...")

	err = fs2.WriteFile("new-file.txt", []byte("New content in layer"), 0644)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Added new-file.txt to layer")

	fmt.Printf("   ✓ Base has %d entries\n", len(fs2.Base().Entries))
	fmt.Printf("   ✓ Layer has %d entries\n", len(fs2.Layer().Entries))

	// Clean up
	os.Remove("snapshot.c4m")

	fmt.Println("\n=== Demo Complete ===")
}
