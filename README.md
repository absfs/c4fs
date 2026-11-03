# c4fs

[![Go Reference](https://pkg.go.dev/badge/github.com/absfs/c4fs.svg)](https://pkg.go.dev/github.com/absfs/c4fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/absfs/c4fs)](https://goreportcard.com/report/github.com/absfs/c4fs)
[![CI](https://github.com/absfs/c4fs/actions/workflows/ci.yml/badge.svg)](https://github.com/absfs/c4fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A [C4](https://github.com/Avalanche-io/c4) manifest as a filesystem.

- A **View** presents the tree a c4m manifest describes as a read-only
  [absfs](https://github.com/absfs/absfs) filesystem, reading each file's
  bytes by C4 ID from a c4 store only when the file is read.
- A **Workspace** makes a View writable. It records what changes, keeps
  written content in a scratch folder, and **captures** the result as a c4m
  patch plus a new manifest, hashing only what was written.

```go
v, err := c4fs.OpenConfigured(id)                 // the ID `c4 id -s` printed
w, err := c4fs.NewWorkspace(v, scratch)           // scratch: a folder or memfs
// ... anything writes to w: Go code, or an NFS client through absnfs ...
cs, err := w.Capture(st)                          // hash and store what changed
cs.WriteTo(f)                                     // a patch that c4 log and c4 patch read
```

## Why

A c4m manifest is a complete description of a tree: names, modes, times,
sizes, symlinks, and the C4 ID of every file's content. The c4 CLI's store
holds the content by ID. Together they *are* the tree, but tools want a
filesystem, so the usual move is to write the whole tree to disk first.

c4fs skips that step, on both sides:

- **Reading.** A View is an `absfs.SymlinkFileSystem`, so it can be listed
  and read directly, served over NFS by [absnfs](https://github.com/absfs/absnfs),
  or routed by [switchfs](https://github.com/absfs/switchfs). Nothing is
  materialised.
- **Writing.** A Workspace over a View is what an agent or a build gets to
  work in. It is a writable filesystem whose base is the commit itself, and
  any number of workspaces can share that base. When the work is done,
  Capture turns it into a c4m changeset against the commit, at a cost
  proportional to what changed. Nothing has to scan the result.

The motivating case is C4's NFS workspace prototype: one NFSv3 export served
by absnfs, with switchfs routing `/<workspace>/...` to a writable layer over
one shared commit, mounted on macOS by a normal user. The prototype used a
unionfs over a commit materialised on disk, and captured by scanning the
union's upper folder. With c4fs, each workspace is a `c4fs.Workspace` over a
View of the commit: there is no materialised base, no whiteout files, and no
scan.

## Install

```sh
go get github.com/absfs/c4fs
```

It builds on `github.com/Avalanche-io/c4` v1.0.21 (the `c4m` and `store`
packages) and `github.com/absfs/absfs` v1.0.0, with Go 1.23 or later.

## Quick start

### Read a stored tree

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
v, err := c4fs.Open(s, id)                  // by ID: a manifest, a root record, or a patch
v, err := c4fs.New(manifest, s)             // a *c4m.Manifest in memory
v, err := c4fs.New(manifest, nil)           // names and metadata only
```

### Work in it, then capture

```go
host, _ := osfs.NewFS()
scratch, _ := basefs.NewFS(host, "/var/c4/scratch/job-17") // or memfs.NewFS()
w, err := c4fs.NewWorkspace(v, scratch)

f, _ := w.Create("/src/gen.go")         // new files go straight to scratch
f.Write(code)
f.Close()
w.Rename("/src/lib", "/lib")            // a whole subtree, in constant time
w.RemoveAll("/build")                   // one whiteout, whatever its size
w.Chmod("/run.sh", 0o755)               // metadata only: nothing is copied

cs, err := w.Capture(st)                // st: the c4 store.Store to put content in
fmt.Println(cs.Base, "->", cs.ID)       // the c4m IDs before and after
fmt.Println(cs.Files, "files hashed")   // only what was written since the last capture
cs.WriteTo(changeset)                   // base ID, patch entries, closing ID
m, _ := cs.Manifest()                   // the whole new tree, if you need it
nv := cs.View()                         // ... or a read-only View of it
```

Capture again later, and the new changeset applies to this one's state, so
a series of captures is a patch chain.

### Pick up where you left off

```go
w, err := c4fs.Resume(base, chainFile, st, scratch) // base View + changesets + store
head, _ := w.Head()                                 // the last captured ID
```

`go run ./example <id>` lists a stored tree, `go run ./example <id> path`
prints a file, and `go run ./example -w <id>` writes in a workspace and
prints the captured changeset. The `-w` mode puts the new file's content (a
few bytes) into the c4 CLI's configured store, as `c4 id -s` would; the
stored tree itself is untouched.

## Serving workspaces over NFS

Each workspace is its own `absfs.SymlinkFileSystem`, so the prototype's
composition holds, with c4fs in place of unionfs and the materialised base:

```go
base, _ := c4fs.OpenConfigured(commitID) // one View, shared by every workspace
root, _ := memfs.NewFS()                 // lists the workspaces at /
sw, _ := switchfs.New(switchfs.WithDefault(root))

// A new workspace: an empty scratch folder, a root entry and a route. Nothing is copied.
scratch, _ := basefs.NewFS(host, "/var/c4/scratch/alpha")
w, _ := c4fs.NewWorkspace(base, scratch)
root.Mkdir("/alpha", 0o755)
sw.Router().AddRoute(switchfs.Route{
    Pattern:  `^/alpha(/|$)`,
    Type:     switchfs.PatternRegex,
    Backend:  w,
    Priority: 100,
    Rewriter: switchfs.StripPrefix("/alpha"),
})

nfs, _ := absnfs.New(router, absnfs.ExportOptions{}) // router: see the note below
srv, _ := absnfs.NewServer(absnfs.ServerOptions{Port: 12049, MountPort: 12049, UseRecordMarking: true})
srv.SetHandler(nfs)
srv.Listen()
```

```sh
mount_nfs -o vers=3,tcp,port=12049,mountport=12049,locallocks,noresvport localhost:/ ~/c4/ws
```

Notes:

- switchfs v1.0.0 does not apply a route's `Rewriter`, and has no symlink
  methods. Wrap it so that every path method asks the router for the backend
  and the rewritten path, as the prototype does. The tests have a complete
  wrapper: `router` in `union_test.go` together with `fullRouter` in
  `ws_nfs_test.go`, which drives workspaces through absnfs's own READ,
  WRITE, CREATE, REMOVE, RENAME, SYMLINK, READDIR and SETATTR code.
- absnfs v1.1.0 needs the prototype's fixes before macOS lists directories
  (file IDs dropped in READDIRPLUS) and before files belong to the mounting
  user. Those are absnfs issues, not c4fs ones.
- An NFS WRITE opens the file, writes and closes it, then sets its time. A
  workspace makes that cheap: no hashing and no lock held during the write.
  The first write to a commit file copies it to scratch once.
- Use a folder (basefs over osfs) for scratch on a server. memfs v1.1.0 is not
  safe for a write and a stat of the same file at once. basefs v1.0.0 takes a
  Unix-style absolute path, so on Windows use another absfs for scratch.

## Workspace or unionfs?

A View also works as the read-only lower layer of a
[unionfs](https://github.com/absfs/unionfs), which is still supported and
tested. A Workspace is the better fit for C4:

|                         | unionfs over a View                   | c4fs.Workspace                                        |
| ----------------------- | ------------------------------------- | ----------------------------------------------------- |
| deletions               | `.wh.` files in the upper             | in-memory whiteouts; never visible as files           |
| rename of a directory   | copies the subtree up                 | moves it, changes and all, in constant time           |
| chmod / chtimes         | copies the file up                    | records the metadata; copies nothing                  |
| `rm -r` of a commit dir | walks it, a whiteout per name         | one whiteout                                          |
| capture                 | scan and hash the upper folder        | `Capture`: hash only files written since last capture |
| result                  | a manifest of the upper, to reconcile | a c4m patch against the commit, and the new manifest  |

## Capture and patches

`Capture(st)` does three things:

1. Every file written since the last capture is read once, hashed, and put
   into `st`. Files up to 1 MiB are read whole, skipped if `st` has them
   already, and written with `st.Create`. Larger files stream through
   `st.Put`. Files of the base, and files whose only change is metadata, keep
   the C4 ID already known, and nothing reads them.
2. The changed directories are identified bottom up, exactly as `c4 id`
   identifies them: the C4 ID of the canonical one-level listing, the size
   as c4m defines it, and the directory's modification time, which moves
   when names in it are created, removed or renamed.
3. The patch is the difference from the last captured state. It is found by
   walking only what changed, because subtrees that did not change are the
   very same nodes in both trees.

The patch uses c4m's semantics, as `c4m.PatchDiff` writes them: a new name
adds, an entry identical to the base's removes, and any other entry replaces.
A changed directory's entry is followed by its children. c4m has no rename,
so a renamed directory appears as a removal and an addition of its whole
subtree; its content is not stored again.

**What you get.** A `Changeset` holds `Base` (the ID the patch applies to),
`ID` (the captured tree's ID, `c4m.Manifest.ComputeC4ID` of it), `Patch`
(the entries), and `Files`/`Bytes` (what was hashed: the files written since
the last capture, each stored unless the store had that content already). `Manifest()` returns
the whole new tree and `View()` a read-only View of it. Both are computed on
demand, so a caller that keeps only the patch never pays for them.

**Keeping it.** `WriteTo` writes the base ID line, the entries and the
closing ID line, which is exactly what `c4 diff` prints. Where it goes is the
caller's decision:

- **Appended to the commit's c4m**, it extends that file's patch chain:
  `c4 log commit.c4m` lists each capture, `c4 patch commit.c4m` resolves to
  the latest, and `c4 patch -n 2 commit.c4m` to an earlier one.
- **Kept as its own changeset file**, it is self-describing: its first line
  names its base. `c4 log commit.c4m changes.c4m` lists both. To resolve it
  with the CLI, concatenate the files first:
  `cat commit.c4m changes.c4m > chain.c4m && c4 patch chain.c4m`. Do not
  run `c4 patch commit.c4m changes.c4m`: with two c4m arguments the CLI
  writes the first into the second. Stored with `st.Put`, a changeset is
  also a tree that `c4fs.Open(st, id)` opens, if its base is in the store.

An empty changeset writes nothing, as `c4 diff` writes nothing for identical
trees, so appending it is a no-op.

**After a capture** the workspace carries on unchanged: its files, open
handles and scratch content stay as they were. What moves is its head: the
next capture's patch applies to this capture's state (`cs.Base` is the
previous `cs.ID`), and it hashes only what was written since. `Head()`
reports the current head. Capture holds the workspace's lock while it runs,
so writers wait for it; capture at a quiet point. A failed capture loses
nothing, and the next one picks up the rest.

**Resume.** `Resume(base, changesets, st, scratch)` applies the changesets
to the base View, checking each base ID and the closing ID, and returns a
workspace over the result with an empty scratch and its head at the last
capture. `View.Apply` does the same and returns the View. The resumed
workspace reads captured content from `st` and the rest from the base's
store.

## The store

Anything that implements c4's `store.Source` can back a View: a local
`TreeStore`, S3, or a `MultiStore`. `OpenConfigured` finds the store the
CLI's way: `C4_STORE` (comma-separated for several) or `store =` lines in
`~/.c4/config`. Capture needs a `store.Store` to put content into. It may be
the base's store or a different one; Views of captured state read from both.

Reading content, in order of preference:

1. A store that keeps objects as local files (the c4 `TreeStore`, `Folder`,
   `MultiStore`) is read in place: `ReadAt` and `Seek` are `pread` and an
   offset on the object file.
2. A store whose readers are `io.ReaderAt` and `io.Seeker` is read directly.
3. Anything else (S3, say) is hydrated into a `Cache`, verified against its
   C4 ID, then read locally. A `Cache` is itself a c4 TreeStore. It
   publishes each object by atomic rename and shares one download among
   concurrent readers of an ID. Pass one with `WithCache` to share it between
   Views; otherwise a process-wide cache in `DefaultCacheDir()` is created
   the first time it is needed.

## What a View reads

A View reads exactly what the c4 CLI (1.0.2x) writes:

- **Any ID the CLI gives you.** `c4 id -s` stores the manifest's text under
  the ID it prints, a one-level record for every directory, and the root's
  record under the root directory's ID. `Open` accepts either ID.
- **Patch chains.** A stream with several sections resolves to its final
  state, split as the CLI splits it. A stream whose first line is a bare C4
  ID is a patch over that base; the base loads from the store and the patch
  applies with c4m's rules. This covers the output of `c4 diff`, closing ID
  included, and changesets from `Capture`.
- **Sequences.** A folded entry like `frame.[0001-0100].exr` lists as its
  member files, with member IDs from the manifest's inline range data or the
  ID-list object that `c4 id -s -S` stores. A capture keeps a sequence
  folded while all its members are unchanged and in place. If one changes,
  the sequence is written as its members, and the size of each one written
  out, which the folded entry does not record, is measured from its content.

## Design

**A per-directory index.** The View indexes the tree once, per directory,
from c4m's nested (indented) entries, with each directory's children sorted
by name. `ReadDir` costs O(children) and a lookup O(depth · log fanout);
nothing ever scans the whole manifest. Nodes point at the manifest's own
entries, so the index adds about 74 bytes per entry. Nodes never change, so
trees derived from a View (patched, or captured) share every subtree they
do not touch.

**Lazy directories and content.** A directory with a C4 ID but no children
inline, which is how the CLI stores directory records, loads its record the
first time it is visited. Opening a large tree by its root record therefore
reads one object. Content is fetched on first read, never on `Open`, `Stat`
or `Close`, with one exception: `Size()` of a file whose c4m size is null
reads the content to measure it (hydrating it from a streaming store), so a
GETATTR or `ls -l` of such a file fetches it. If that content is missing,
`Size()` reports 0 and a read reports `ErrContentMissing`.

**Metadata from c4m.** Mode, time and size come from the entry, with c4m's
null values handled as the standard defines them:

| Field     | Null in c4m | The View reports                                    |
| --------- | ----------- | --------------------------------------------------- |
| mode      | `-`         | 0644 file, 0755 directory, 0777 symlink             |
| timestamp | `-` or `0`  | the Unix epoch, c4m's internal value for null       |
| size      | `-`         | measured from the content on first `Size()`, cached |
|           |             | (0 if the content is missing)                       |
| C4 ID     | `-`         | empty if the size is 0, otherwise reads `ErrNoID`   |

`FileInfo.Sys()` returns the `*c4m.Entry`, so callers can read C4 IDs.
`View.ID()` is the tree's own C4 ID as c4 computes it, resolving null
directory sizes and times from their children as c4m does.

**The workspace's change set.** A workspace node exists only for a name the
workspace changed, or a directory on the way to one. Anything else falls
through to the View. A directory node holds the children it changed; a nil
child is a whiteout. A node made where the View had a directory that was
removed has no View directory behind it, so it is opaque. Each changed file
has one scratch file, named by a counter, so renames never touch scratch. A
removed file's scratch file is deleted once no handle holds it open.

**Incremental capture.** Every change marks its node and the node's
ancestors as changed since the last capture. Capture rebuilds only marked
nodes and reuses the rest from the last capture. A node that turns out to
be exactly its View node, like a file opened for writing but not changed or
a chmod undone, is reused as that node, so the patch does not show it. Each
directory's listing in c4m's natural order, and each entry's canonical line,
is computed once and remembered. c4fs formats C4 IDs and compares names
without allocating, and checks both against c4's own code.

**Read-only View.** Every mutating method of a View fails with an
`*fs.PathError` wrapping `ErrReadOnly` (`syscall.EROFS`), or with the more
specific error a read-only mount gives: `EEXIST`, `ENOENT`, `ENOTDIR`,
`EISDIR`. `MkdirAll` of an existing directory succeeds; otherwise it reports
what `os.MkdirAll` would on a read-only mount: `ENOTDIR` for a file or a path
through one, `EEXIST` for a name that exists but does not lead to a directory
(a dangling link), and `EROFS`, naming the first directory it would make, only
where it would make one. A workspace's `MkdirAll` follows `os.MkdirAll` the
same way.

**Symlinks.** `Lstat` and `Readlink` see links; `Stat`, `Open` and `ReadDir`
follow them, through up to `MaxSymlinks` (40) links. Relative targets
resolve against the link's directory, absolute targets against the root.

**Concurrency.** A View is safe for any number of goroutines, as an NFS
server needs, and a file's `ReadAt` calls run in parallel. A Workspace is
safe for concurrent use: a copy-up does not hold the lock, and concurrent
openers of the same file share one copy. A workspace handle describes what
it was opened on as it is now: `Stat` on it shows a later `Chmod` or write,
and a directory handle lists afresh after a rewind (`Seek(0, io.SeekStart)`),
while one pass of a listing stays a consistent snapshot.

## Performance

Apple M3 Max, Go 1.27.3, `go test -bench .`. The base is a synthetic tree of
100,000 files in 1,100 directories (100 directories of 10 subdirectories of
100 files). The View rows use it as a hand-written manifest might give it,
with each directory's size and C4 ID null; the Workspace rows use it with
every directory's size and C4 ID filled in, as `c4 id` writes them.

| View                                   | Time   | Allocations |
| -------------------------------------- | ------ | ----------- |
| build the index (`New`)                | 5.2 ms | 112k        |
| `Stat` of a file three levels down     | 293 ns | 6           |
| `ReadDir` of a 100-entry directory     | 1.3 µs | 105         |
| `ReadDir` of a 100,000-entry directory | 1.0 ms | 100k        |

| Workspace over the 100,000-file base                          | Time    |
| ------------------------------------------------------------- | ------- |
| first `Capture` of 1,000 rewritten 1 KiB files, RAM store     | 155 ms  |
| the same into a `TreeStore` (no fsync)                        | 163 ms  |
| next `Capture`, 10 more files rewritten                       | 0.40 ms |
| `Changeset.Manifest()`: the whole 101,100-entry manifest      | 5.2 ms  |
| `Resume` / `View.Apply` of the 1,000-file changeset           | 6.5 ms  |
| `Stat` of a commit file, 1,000 changes elsewhere              | 264 ns  |
| one NFS WRITE (open, 32 KiB `WriteAt`, close), folder scratch | 28 µs   |
| create, write 1 KiB and close a new file, folder scratch      | 164 µs  |

The capture benchmark is a near worst case. It rewrites one file in each of
1,000 different directories, so 1,100 directories of 100 entries each get
new IDs, and its patch has 2,100 entries. Most of its time is the canonical
c4m text of those 110,000 directory entries, which any c4 identification of
those directories needs. Every iteration starts from a fresh View, with
nothing remembered. A capture of the same files concentrated in a few
directories, or any later capture, costs far less, as the incremental row
shows.

## Errors

| Error                | Meaning                                                      |
| -------------------- | ------------------------------------------------------------ |
| `ErrReadOnly`        | a mutation of a View (EROFS)                                 |
| `ErrContentMissing`  | the file's ID is not in the store; it still lists and stats  |
| `ErrNoID`            | a non-empty file whose entry has a null C4 ID                |
| `ErrIDMismatch`      | stored bytes do not hash to their ID; a patch's closing ID   |
| `ErrSizeMismatch`    | content length contradicts the entry's size                  |
| `ErrNoStore`         | a store is needed and there is none                          |
| `ErrNotManifest`     | the object named as a manifest is not c4m, or not a patch    |
| `ErrInvalidManifest` | an entry skips its parent's depth, or a name is not a name   |
| `ErrBaseMismatch`    | a patch's base ID is not the ID of the tree it is applied to |

Missing content is deliberately not `fs.ErrNotExist`: the file exists, its
bytes do not. A store error that is not an absence (a timeout, say) passes
through unchanged. A workspace reports what `os` would: `EEXIST`, `ENOENT`,
`ENOTDIR`, `EISDIR`, `ENOTEMPTY`, `EINVAL` (a negative offset, say),
`ELOOP`, `EBADF`, and `EBUSY` for the root.

Filesystem methods wrap these in an `*fs.PathError` (`*os.LinkError` for
`Rename` and `Symlink`) naming the path, the workspace path rather than a
scratch file. `Open`, `OpenConfigured`, `New`, `View.Apply`, `Resume` and
`Capture` only add context, such as the C4 ID. Test with `errors.Is`.

A path through a file (`/README.md/x`) is `syscall.ENOTDIR` on every platform,
and `RemoveAll` of one fails rather than succeeding as for a missing name. On
Windows, though, Go defines `syscall.ENOTDIR` as `ERROR_PATH_NOT_FOUND`, so the
same error also satisfies `os.IsNotExist` and `errors.Is(err, fs.ErrNotExist)`.
A caller that ignores not-exist errors, as code around `RemoveAll` often does,
will treat a path through a file as missing there. Test for `syscall.ENOTDIR`
first where the difference matters.

## Testing

`go test -race ./...` covers, among the rest:

- absfs/fstesting's full suite against a workspace: over a View, over an
  empty View, inside the View's own directories, and with folder scratch.
  Also its wrapper suite and its path-traversal fuzz target. For the View,
  the read-only wrapper suite, and the full suite through a unionfs whose
  lower is a View.
- the standard library's `testing/fstest.TestFS` over a View and over a
  changed workspace;
- **capture against `c4 id`**: one script of changes (writes, appends,
  truncates, chmods, renames of files and directories, delete then recreate,
  opaque replacement of a directory, removals) is applied to a real folder
  and to a workspace over its snapshot. The captured ID and manifest must be
  byte-for-byte what `c4`'s scanner makes of the folder, and the patch must
  take the old manifest there under c4m's own `ApplyPatch`;
- **the real `c4` CLI**, when installed: `c4 log` and `c4 patch` (including
  `-n`) read captured changesets appended to the commit's c4m and kept
  apart, and `c4 patch` materialises the captured tree from the store with
  exactly the captured ID;
- workspaces driven through absnfs's NFS operations behind switchfs routing,
  two workspaces sharing one View;
- writers, readers, directory renames, a shared copy-up and dozens of
  captures running at once, after which the chain of changesets must apply in
  order and resume to exactly the workspace;
- a copy-up held open while the file is removed or renamed and a second
  opener waits on it, in a fixed order, so the unlocked copy path is pinned;
- sequences that stay folded, unfold when a member changes, and fold again
  when it is put back; null directory metadata; missing directory records;
  failing stores and scratch filesystems;
- every operation on a path through a file, on a View and a workspace, with
  `ENOTDIR` made to match `fs.ErrNotExist` as Windows' does, so every OS
  checks that none succeeds as though the name were missing; and `MkdirAll`
  held to `os.MkdirAll` on the host over files, links, dangling links, loops
  and links through files.

Statement coverage is 97.8% for the package and 90.8% for the example
command.

## Caveats

**View**

- Path cleaning is lexical: `..` after a symlink does not return through it.
- Absolute symlink targets resolve inside the View, not on the host.
- A View built with `New` must not have its manifest modified while in use.
- Content read directly from a store (cases 1 and 2 above) is trusted; only
  hydrated content is verified against its ID, and `ErrSizeMismatch` guards
  the rest cheaply.
- A sequence's members share the folded entry's mode and time.
- `Chdir` sets the View's own working directory, shared by every user of the
  View (workspaces, unionfs lowers, NFS goroutines). Code that shares a View
  should use absolute paths.
- A `Cache`, `DefaultCacheDir()`'s included, is never pruned: it grows with
  everything hydrated into it. Its owner decides its lifetime; delete the
  directory, when no View is reading from it, to reclaim the space.

**Workspace**

- A read-only handle on a commit file that has not been copied to scratch
  keeps reading the commit's content if the file is then written through
  another handle, as with overlayfs; its `Stat` does show the new size and
  metadata. Handles on changed files share their scratch file and see each
  other's writes.
- A directory handle opened before its directory was removed keeps listing
  what it listed, rather than nothing.
- Hard links are not supported (absfs has no `Link`). A commit file that
  carries a c4m hard-link mark loses it when its content changes.
- `Chown` and `Lchown` change nothing: c4m records no ownership. `Chtimes`
  sets the modification time; c4m records no access time. A zero mtime leaves
  the time unchanged, as `os.Chtimes` does.
- A file's mode 0 (from `Chmod` or a create with `perm` 0) is c4m's null
  mode. The workspace reports `----------`, but the captured manifest records
  `-`, which a View, or a workspace resumed from it, reads as 0644.
- A symbolic link made in a workspace is captured with a null C4 ID. `c4 id`
  would record its target's ID instead.
- The root directory's own mode and time are not captured: c4m has no entry
  for the root.
- `OpenFile` applies `perm` as given, with no umask, as memfs does. An NFS
  client applies its umask before it asks.
- A workspace's change set and scratch hold everything changed since it was
  made or resumed. Capture's cost follows what changed since the last
  capture, but the change set only grows. `Resume` from the chain starts
  afresh with an empty scratch.
- A file of the commit whose content is missing from the store cannot be
  opened for writing, because there is nothing to copy. It can still be
  truncated to zero or replaced.
- memfs v1.1.0 makes a fine scratch for tests and single-threaded use, but it
  is not safe for concurrent writes and stats of one file. It also panics on
  a negative `Truncate`, `ReadAt` or `WriteAt` offset, which the workspace
  refuses with `EINVAL` before it gets there.
- Folder scratch through basefs v1.0.0 works on Unix only: basefs takes a
  Unix-style absolute path, which Windows refuses. Use memfs or another absfs
  for scratch on Windows.
- `Base()` of a resumed workspace is the View `Resume` built (the base with
  the chain applied). Keep the original base to resume the whole chain again.
- c4fs is pre-1.0; its API may still change.

## Migrating from v0.9.x

c4fs was rewritten for C4 1.0. The v0.9 package (`FS`, layers, tombstones,
dehydrating files, `ReferencedIDs`, `StoreAdapter`, and the c4 v0.8.2
pre-release) is gone. It did not fit absfs: files had no `ReadAt` or
`Seek`, `Open` returned its own types, listing scanned the whole manifest,
names were flat paths, and its store layout differed from the 1.0 CLI's.

| v0.9.x                                    | now                                                                |
| ----------------------------------------- | ------------------------------------------------------------------ |
| `c4fs.New(base, c4fs.NewStoreAdapter(s))` | `c4fs.New(base, s)`, or `c4fs.Open(s, id)`: a `*View`              |
| `c4fs.NewWithLayer(base, layer, store)`   | `c4fs.NewWorkspace(view, scratch)`                                 |
| tombstones in the layer manifest          | whiteouts in the workspace's in-memory change set                  |
| files hashed into the store as they close | files hashed when `Workspace.Capture` runs                         |
| `FS.Layer()`                              | `Changeset.Patch`, or `WriteTo`: a c4m patch against the base      |
| `FS.Flatten()`                            | `Changeset.Manifest()`                                             |
| `FS.ReferencedIDs()`                      | the C4 IDs of `View.Manifest()`                                    |
| `StoreAdapter` over the c4 v0.8.2 store   | any c4 v1 `store.Source`, the CLI's own store included             |
| `FS.Store()`                              | the `store.Source` you passed in                                   |
| `FS.Base()`                               | `View.Manifest()`, or `Workspace.Base()` for the View              |
| `FS.WriteFile(name, data, perm)`          | `Workspace.Create` or `OpenFile`, then `Write` and `Close`         |
| `FS.ReadLink(name)`                       | `Readlink(name)`, on a View or a Workspace                         |
| `FS.Exists`, `IsDir`, `IsFile`, `Size`    | `Stat(name)`: its error, `IsDir()`, `Mode().IsRegular()`, `Size()` |
| `FS.Glob(pattern)`                        | `fs.Glob` over `View.Sub("/")` or `Workspace.Sub("/")`             |
| the `FileSystem` and `File` interfaces    | `absfs.SymlinkFileSystem` and `absfs.File`                         |

Snapshot existing trees with the 1.0 CLI (`c4 id -s`) and open them by the ID
it prints.

## License

MIT, see [LICENSE](LICENSE).
