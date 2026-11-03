# Changelog

All notable changes to c4fs are documented here. c4fs is pre-1.0, so minor
versions may break its API.

## [0.10.0] - Unreleased

**Breaking:** the whole API changes. Nothing of v0.9.x's `FS` remains; see
"Migrating from v0.9.x" in the README for what replaces each part. The next
tag is v0.10.0, a minor bump, as c4fs is pre-1.0.

A rewrite. c4fs is now a read-only view of a c4m manifest, and a
writable workspace over it that captures its changes as c4m, built on
github.com/Avalanche-io/c4 v1.0.21 and absfs v1.0.0. The previous
implementation is removed.

### Added

- `Workspace`, a writable `absfs.SymlinkFileSystem` over a `View`, made with
  `NewWorkspace(view, scratch)`. Written content lives in a caller-supplied
  scratch filesystem (a basefs folder or memfs), one file per written file.
  Everything else is an in-memory change set: creations, removals with
  whiteouts, renames of files and whole directories in constant time,
  `Mkdir`, `Symlink`, `Chmod` and `Chtimes`. A View file is copied to
  scratch on its first open for writing (`O_WRONLY` or `O_RDWR`) or
  truncate; `O_TRUNC` copies nothing, `Truncate` copies only what it keeps,
  and metadata changes copy nothing. Read-only opens copy nothing, even with
  `O_CREATE` or `O_APPEND`. Handles report current metadata from `Stat`, a
  directory handle lists afresh after a rewind, negative offsets are
  `EINVAL`, and a zero mtime in `Chtimes` leaves the time alone, all as
  package os behaves. A directory removed and made again is opaque. `Remove` of a
  directory needs the merged directory to be empty. Passes absfs/fstesting's
  suites, and is safe for concurrent use.
- `Workspace.Capture(store)`, which hashes only the files written since the
  last capture (no hashing on write or close), puts them into the store,
  identifies the changed directories as `c4 id` does, and returns a
  `Changeset`: the patch against the previous state (`Base`, `ID`, `Patch`,
  `WriteTo` in `c4 diff`'s format, which `c4 log` and `c4 patch` read), plus
  the new tree on demand (`Manifest`, `View`). Successive captures form a
  patch chain; `Workspace.Head` reports where the next one applies.
- `Resume(base, changesets, store, scratch)`, which rebuilds a workspace from
  its base and its chain of changesets, and `View.Apply`, which applies a
  chain to a View, checking base and closing IDs (`ErrBaseMismatch`,
  `ErrIDMismatch`).
- `View.ID`, the tree's C4 ID as `c4m.Manifest.ComputeC4ID` computes it, and
  `View.Manifest`, the whole tree as a manifest with sequences folded.
- Tests that hold a capture to `c4 id`'s scan of the same changes made on
  disk, to `c4m.ApplyPatch`, and to the real `c4 log` and `c4 patch`; tests
  through absnfs's NFS operations behind switchfs; a concurrency test with
  dozens of interleaved captures; and benchmarks for capture over a
  100,000-file base.
- `View`, an `absfs.SymlinkFileSystem` over the tree a c4m manifest
  describes, opened with `New` (a `*c4m.Manifest`), `Open` (a C4 ID in any
  c4 `store.Source`) or `OpenConfigured` (the store the c4 CLI is configured
  to use).
- A per-directory index from c4m's nested names: O(children) `ReadDir`,
  O(depth · log fanout) lookup, about 74 bytes per entry beyond the manifest.
- Lazy directories: a directory given by C4 ID alone loads its one-level
  record from the store on first visit, so a tree opened by its root record
  reads one object.
- Patch chains resolved to their final state, including external bases and
  the output of `c4 diff`, loading only the directories a patch touches.
- Folded sequences listed as their member files.
- Files with `Read`, `ReadAt`, `Seek` and `Stat`; directories with
  `ReadDir`, `Readdir` and `Readdirnames` using `os.File`'s pagination.
- Lazy content: read in place from stores with local files, directly from
  readers that are `io.ReaderAt` and `io.Seeker`, and otherwise hydrated into
  a `Cache` (a c4 TreeStore; atomic, verified, one download per ID).
- A cached object is not opened until its fill has returned, even once its
  rename has made it visible: on Windows that rename holds the file open,
  and an open inside the window failed with a sharing violation. On Windows
  local objects are also opened sharing delete access, so a rename or
  removal of the name by another process neither refuses the open nor is
  refused by it, and any remaining sharing violation is retried with a
  bounded backoff. A stress test emulates the Windows window on every OS.
- c4m null modes, times and sizes handled per the standard; `FileInfo.Sys`
  returns the `*c4m.Entry`.
- Symlinks: `Lstat`, `Readlink`, and following with a 40-link limit.
- Errors: `ErrReadOnly` (EROFS), `ErrContentMissing`, `ErrNoID`,
  `ErrIDMismatch`, `ErrSizeMismatch`, `ErrNoStore`, `ErrNotManifest`,
  `ErrInvalidManifest`.
- A path through a file is `ENOTDIR` on every platform, and `RemoveAll` of
  one fails, on Windows too, where that `ENOTDIR` also satisfies
  `os.IsNotExist`. A test runs every operation through a file on a View and a
  Workspace with `ENOTDIR` behaving as Windows' does, on every OS.
- `MkdirAll` reports what `os.MkdirAll` does, held to it on the host by a
  test: `ENOTDIR` for a file, a link to one or a path through one, and
  `EEXIST` for a link that cannot be followed (dangling, a loop, or a link
  to a path through a file); a View's gives `EROFS` naming the first
  directory it would make. Both name the path as given, unclean or not. A
  View's `Mkdir` and `MkdirAll` report a missing directory record as
  `ErrContentMissing`, not `EROFS`.
- A known zero mode (`c4m.ZeroMode` in c4 releases after v1.0.21, written
  `----------`) is reported as mode 0; its bit 18 never reaches `Mode()`.
- Tests against `testing/fstest`, absfs/fstesting, unionfs and switchfs, and
  the real c4 CLI when installed; benchmarks on a 100,000-entry manifest.
- An example command that lists and reads a stored tree, and with `-w`
  writes in a `Workspace` and prints the captured changeset.

### Removed

- The old `FS`, `Layer`, tombstones, dehydrating files, garbage collection
  and its own store types, with the dependency on the c4 v0.8.2 pre-release,
  whose store layout did not match the 1.0 CLI's.

[0.10.0]: https://github.com/absfs/c4fs/compare/v0.9.1...HEAD
