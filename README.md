# c4fs

[![Go Reference](https://pkg.go.dev/badge/github.com/absfs/c4fs.svg)](https://pkg.go.dev/github.com/absfs/c4fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/absfs/c4fs)](https://goreportcard.com/report/github.com/absfs/c4fs)
[![CI](https://github.com/absfs/c4fs/actions/workflows/ci.yml/badge.svg)](https://github.com/absfs/c4fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A c4m manifest as a filesystem. c4fs presents the tree a
[C4](https://github.com/Avalanche-io/c4) manifest describes as a read-only
[absfs](https://github.com/absfs/absfs) filesystem, reading each file's bytes
by C4 ID from a c4 store only when the file is read.

```go
v, err := c4fs.OpenConfigured(id) // the ID `c4 id -s` printed
data, err := v.ReadFile("/src/main.go")
```

## Why

A c4m manifest is a complete description of a tree (names, modes, times,
sizes, symlinks, and the C4 ID of every file's content), and the c4 CLI's
store holds the content by ID. Together they are the tree, but tools want a
filesystem, so the usual move is to write the whole tree out to disk first.

A `View` skips that step. It is an `absfs.SymlinkFileSystem`, so it can be
listed and read directly, served over NFS by
[absnfs](https://github.com/absfs/absnfs), routed by
[switchfs](https://github.com/absfs/switchfs), or used as the shared read-only
lower layer of a [unionfs](https://github.com/absfs/unionfs) workspace. The
motivating case is C4's NFS workspace prototype: one export whose
`/<workspace>/` directories are each a writable upper over the same commit,
with no materialised base on disk at all.

## Install

```sh
go get github.com/absfs/c4fs
```

It builds on `github.com/Avalanche-io/c4` v1.0.21 (the `c4m` and `store`
packages) and `github.com/absfs/absfs` v1.0.0.

## Quick start

Snapshot a tree with the c4 CLI, then open it by the ID it prints:

```sh
c4 id -s mytree/ > mytree.c4m     # stderr: "stored: c4..."
```

```go
id, _ := c4.Parse("c4...")        // the stored ID
v, err := c4fs.OpenConfigured(id) // C4_STORE or ~/.c4/config, as the CLI
if err != nil {
    log.Fatal(err)
}
ents, _ := v.ReadDir("/")         // sorted, like os.ReadDir
f, _ := v.Open("/src/main.go")
buf := make([]byte, 4096)
n, _ := f.ReadAt(buf, 1<<20)      // random access, as NFS reads
```

Other ways in:

```go
s, _ := store.NewTreeStore("/data/c4store") // any c4 store.Source
v, err := c4fs.Open(s, id)                  // by ID
v, err := c4fs.New(manifest, s)             // a *c4m.Manifest in memory
v, err := c4fs.New(manifest, nil)           // names and metadata only
```

As the lower layer of a workspace, where every write lands in the upper and
the View is never touched:

```go
upper, _ := memfs.NewFS()
ws := unionfs.New(
    unionfs.WithWritableLayer(upper),
    unionfs.WithReadOnlyLayer(v),
).SymlinkFileSystem()
```

`go run ./example <id>` lists a stored tree, `go run ./example <id> path`
prints a file, and `go run ./example -w <id>` writes through a workspace.

## What it reads

A View reads exactly what the c4 CLI (1.0.2x) writes:

- **The store.** Any `store.Source`. `OpenConfigured` finds the store the
  CLI's way (`C4_STORE`, comma-separated for several, or `store =` lines in
  `~/.c4/config`), so a local TreeStore, S3 and multi-store setups all work.
- **Any ID the CLI gives you.** `c4 id -s` stores the manifest's text under
  the ID it prints, a one-level record for every directory, and the root's
  record under the root directory's ID. `Open` accepts either ID.
- **Patch chains.** A stream with several sections resolves to its final
  state, split as the CLI splits it. A stream whose first line is a bare
  C4 ID is a patch over that base; the base loads from the store and the
  patch applies with c4m's rules (a new name adds, an identical entry
  removes, any other entry replaces). This is the output of `c4 diff`,
  including its closing ID.
- **Sequences.** A folded entry like `frame.[0001-0100].exr` lists as its
  member files, with member IDs from the manifest's inline range data or the
  ID-list object `c4 id -s -S` stores.

## Design

**A per-directory index.** The View indexes the tree once, per directory,
from c4m's nested (indented) entries, with each directory's children sorted
by name. `ReadDir` costs O(children) and a lookup O(depth · log fanout);
nothing ever scans the whole manifest. Nodes point at the manifest's own
entries, so the index adds about 58 bytes per entry.

**Lazy directories.** A directory with a C4 ID but no children inline (how
the CLI stores directory records) loads its record the first time it is
visited, so opening a large tree by its root record reads one object.
Patching loads only the directories a patch passes through. A load that fails
is retried on the next visit rather than remembered.

**Lazy content.** Nothing is fetched until a file is first read, not on
`Open`, `Stat` or `Close`. Then, in order:

1. a store that keeps objects as local files (the c4 `TreeStore`, `Folder`,
   `MultiStore`) is read in place: `ReadAt` and `Seek` are `pread` and an
   offset on the object file;
2. a store whose readers are `io.ReaderAt` and `io.Seeker` is read directly;
3. anything else (S3, say) is hydrated into a `Cache`, verified against its
   C4 ID, then read locally. A `Cache` is itself a c4 TreeStore, publishes
   each object by atomic rename, and shares one download among concurrent
   readers of an ID. Pass one with `WithCache` to share it between Views;
   otherwise a process-wide cache in `DefaultCacheDir()` is created the first
   time it is needed.

**Metadata from c4m.** Mode, time and size come from the entry, with c4m's
null values handled as the standard defines them:

| Field     | Null in c4m   | The View reports                                     |
| --------- | ------------- | ---------------------------------------------------- |
| mode      | `-`           | 0644 file, 0755 directory, 0777 symlink              |
| timestamp | `-` or `0`    | the Unix epoch, c4m's internal value for null        |
| size      | `-`           | measured from the content on first `Size()`, cached  |
| C4 ID     | `-`           | empty if the size is 0, otherwise reads `ErrNoID`    |

`FileInfo.Sys()` returns the `*c4m.Entry`, so callers can read C4 IDs.

**Read-only.** Every mutating method fails with an `*fs.PathError` wrapping
`ErrReadOnly` (`syscall.EROFS`, defined on every platform absfs builds for),
or the more specific error a read-only mount gives: `EEXIST`, `ENOENT`,
`ENOTDIR`, `EISDIR`. `MkdirAll` of an existing directory and `RemoveAll` of a
missing name succeed, as in package `os`.

**Symlinks.** `Lstat` and `Readlink` see links; `Stat`, `Open` and `ReadDir`
follow them, through up to `MaxSymlinks` (40) links. Relative targets resolve
against the link's directory, absolute targets against the View's root.

**Concurrency.** A View is safe for any number of goroutines, as an NFS
server needs. A file's `ReadAt` calls run in parallel; `Read` and `Seek`
share its offset.

## Errors

| Error                | Meaning                                                      |
| -------------------- | ------------------------------------------------------------ |
| `ErrReadOnly`        | a mutation (EROFS)                                           |
| `ErrContentMissing`  | the file's ID is not in the store; it still lists and stats  |
| `ErrNoID`            | a non-empty file whose entry has a null C4 ID                |
| `ErrIDMismatch`      | stored bytes do not hash to their ID                         |
| `ErrSizeMismatch`    | content length contradicts the entry's size                  |
| `ErrNoStore`         | a manifest, record or base is needed and there is no store   |
| `ErrNotManifest`     | the object named as a manifest is not c4m                    |
| `ErrInvalidManifest` | an entry skips its parent's depth, or a name is not a name   |

Missing content is deliberately not `fs.ErrNotExist`: the file exists, its
bytes do not. A store error that is not an absence (a timeout, say) passes
through unchanged.

## Performance

A synthetic 100,000-file manifest (100 x 10 directories of 100 files),
Apple M3 Max, `go test -bench .`:

| Operation                              | Time    | Allocations |
| -------------------------------------- | ------- | ----------- |
| build the index (`New`)                | 5.0 ms  | 112k        |
| `Stat` of a file three levels down     | 290 ns  | 6           |
| `ReadDir` of a 100-entry directory     | 1.3 µs  | 105         |
| `ReadDir` of a 100,000-entry directory | 1.0 ms  | 100k        |

The index retains about 58 bytes per entry beyond the manifest itself
(`TestIndexMemory`).

## Testing

`go test -race ./...` covers, among the rest:

- the standard library's `testing/fstest.TestFS` over `Sub`;
- absfs/fstesting's read-only wrapper suite, its path-traversal fuzz target,
  and its full suite run against a unionfs whose lower is a View;
- a union over a View (reads, copy-up, whiteouts, lower untouched) and the
  NFS prototype's switchfs composition, with many concurrent readers;
- the real `c4` CLI, when installed: trees, sequences and `c4 diff` patches
  it stores open and read back identically.

## Caveats

- Path cleaning is lexical: `..` after a symlink does not return through it.
- Absolute symlink targets resolve inside the View, not on the host.
- A View built with `New` must not have its manifest modified while in use.
- Content read directly from a store (cases 1 and 2 above) is trusted; only
  hydrated content is verified against its ID, and `ErrSizeMismatch` guards
  the rest cheaply.
- A sequence's members share the folded entry's mode and time; their sizes
  come from their content.
- c4fs is pre-1.0; its API may still change. The writable workspace layer
  that will sit on a View is the next step.

## License

MIT, see [LICENSE](LICENSE).
