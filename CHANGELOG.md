# Changelog

All notable changes to c4fs are documented here. c4fs is pre-1.0, so minor
versions may break its API.

## [Unreleased]

A rewrite. c4fs is now a read-only view of a c4m manifest, built on
github.com/Avalanche-io/c4 v1.0.21 and absfs v1.0.0. The previous
implementation is removed.

### Added

- `View`, an `absfs.SymlinkFileSystem` over the tree a c4m manifest
  describes, opened with `New` (a `*c4m.Manifest`), `Open` (a C4 ID in any
  c4 `store.Source`) or `OpenConfigured` (the store the c4 CLI is configured
  to use).
- A per-directory index from c4m's nested names: O(children) `ReadDir`,
  O(depth) lookup, about 58 bytes per entry beyond the manifest.
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
- c4m null modes, times and sizes handled per the standard; `FileInfo.Sys`
  returns the `*c4m.Entry`.
- Symlinks: `Lstat`, `Readlink`, and following with a 40-link limit.
- Errors: `ErrReadOnly` (EROFS), `ErrContentMissing`, `ErrNoID`,
  `ErrIDMismatch`, `ErrSizeMismatch`, `ErrNoStore`, `ErrNotManifest`,
  `ErrInvalidManifest`.
- Tests against `testing/fstest`, absfs/fstesting, unionfs and switchfs, and
  the real c4 CLI when installed; benchmarks on a 100,000-entry manifest.

### Removed

- The old `FS`, `Layer`, tombstones, dehydrating files, garbage collection
  and its own store types, with the dependency on the c4 v0.8.2 pre-release,
  whose store layout did not match the 1.0 CLI's.
