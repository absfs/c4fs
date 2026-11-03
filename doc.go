// Package c4fs presents a c4m manifest as an absfs filesystem: a
// read-only View of the tree it describes, with file content read lazily
// by C4 ID from a c4 store, and a writable Workspace layered over a View
// that records what changes and captures it back as c4m.
//
// A c4m manifest (github.com/Avalanche-io/c4/c4m) describes a tree:
// names, modes, times, sizes, symbolic links and the C4 ID of every
// file's bytes. A c4 store (github.com/Avalanche-io/c4/store) holds those
// bytes by ID. A View joins the two into an absfs.SymlinkFileSystem, so a
// commit's tree can be listed, read, or served over NFS by absnfs, with
// nothing materialised on disk. A Workspace makes that tree writable for
// one user, an agent say, and turns what they did into a c4m patch.
//
// # Opening a View
//
//	// From the store the c4 CLI uses (C4_STORE or ~/.c4/config):
//	v, err := c4fs.OpenConfigured(id)
//
//	// From any store, by the ID c4 id -s printed:
//	s, _ := store.NewTreeStore(dir)
//	v, err := c4fs.Open(s, id)
//
//	// From a manifest in memory; the store may be nil for metadata only:
//	v, err := c4fs.New(m, s)
//
// Open accepts a full manifest, a one-level directory record (the root
// directory's ID), or a patch whose first line names its base. Patch
// chains resolve to their final state: inline chains as c4m decodes them,
// and external bases by loading the base and applying the patch with
// c4m's semantics (a new name adds, an identical entry removes, anything
// else replaces). View.Apply applies a patch to a View in memory.
//
// # The index
//
// The tree is indexed per directory, with names sorted, so ReadDir costs
// O(children) and a lookup O(depth · log fanout). It is built once, when
// the View is made, from c4m's nested (indented) entries. A directory that
// carries a C4 ID but no children inline, which is how c4 id -s stores
// directory records, loads its record from the store on first use, so a
// large tree opened by its root record reads only what is visited.
// Folded sequences (frame.[0001-0100].exr) are listed as their member
// files, with member IDs from the manifest's range data or the store's ID
// list. Nodes never change once made, so trees derived from a View (by
// Apply, or by a Workspace's Capture) share every subtree they do not
// change.
//
// # Content
//
// Nothing is fetched until a file is first read, with one exception:
// FileInfo.Size of a file whose c4m size is null reads the content to
// measure it (a stat of a local object, or a hydration otherwise), so a
// GETATTR or ls -l of such a file fetches it; if the content is missing
// its size is reported as 0, while a read reports ErrContentMissing.
// Open, Close and Stat of anything else never touch the store. A store
// that keeps
// objects as local files (the c4 TreeStore) is read in place, with
// ReadAt and Seek; one whose readers are io.ReaderAt and io.Seeker is read
// directly; anything else is hydrated into a Cache, a local TreeStore,
// verified against the ID before it is used. The Cache is never pruned:
// its owner decides when to delete it. Empty files are served without
// the store. A file whose content is absent reads as
// ErrContentMissing and one with a null ID as ErrNoID; both still list and
// stat.
//
// # Metadata
//
// Mode, time and size come from the c4m entry. c4m's null values are
// handled as the standard defines them: a null mode becomes 0644 for a
// file, 0755 for a directory and 0777 for a symbolic link; a null time is
// the Unix epoch, c4m's internal value for it; a null file size is
// measured from the content when first asked for. FileInfo.Sys returns
// the *c4m.Entry, so callers can read C4 IDs. View.ID returns the tree's
// own C4 ID as c4 computes it (c4m.Manifest.ComputeC4ID), and
// View.Manifest the whole tree as a manifest.
//
// # A read-only View
//
// Every mutating method of a View fails with an *fs.PathError wrapping
// ErrReadOnly (syscall.EROFS), or with the more specific error a
// read-only mount gives: EEXIST, ENOENT, ENOTDIR or EISDIR. MkdirAll of
// an existing directory and RemoveAll of a missing name succeed, as in
// package os.
//
// # Workspaces
//
// NewWorkspace layers a writable filesystem over a View. The View is
// never modified, so any number of workspaces share one. A workspace
// keeps the content of files written since it was made in a scratch
// absfs filesystem the caller supplies (a basefs folder over osfs, or a
// memfs), one scratch file per written file, and everything else in an
// in-memory change set: names created, removed and renamed, modes and
// times, symbolic links.
//
//   - A file of the View is copied to scratch the first time it is
//     opened for writing (O_WRONLY or O_RDWR), or truncated; O_TRUNC
//     copies nothing, and Truncate copies only what it keeps. Opening to
//     read copies nothing, even with O_CREATE or O_APPEND.
//   - Chmod and Chtimes record metadata only: no content is copied. A
//     zero mtime leaves the time as it is, as os.Chtimes does. A file's
//     mode 0 is c4m's null mode: the workspace reports 0, a captured
//     tree reads it as 0644.
//   - Rename moves a file, or a whole directory with every change inside
//     it, in constant time; the old name is hidden. Remove and RemoveAll
//     of a directory of the View is one whiteout, whatever its size.
//   - A directory removed and made again starts empty: the View's
//     children do not show through. Remove of a directory succeeds only
//     when the merged directory is empty.
//   - Chown and Lchown check the name and change nothing: c4m records no
//     ownership. Hard links are not supported: absfs has no Link, and a
//     file of the View carrying a c4m hard-link mark loses it once its
//     content changes.
//
// Nothing is hashed on write or close. NFSv3 has no close, and a build
// makes many short-lived files, so content is identified only by
// Capture: the files written since the last capture are hashed as they
// are put into a c4 store, the changed directories are identified bottom
// up as c4 identifies them, and the result is a Changeset holding the
// change as a c4m patch (Changeset.Patch, written by Changeset.WriteTo as
// c4 diff writes it: base ID, entries, closing ID) and the new tree
// (Changeset.View, Changeset.Manifest). After a capture the workspace
// carries on as it was, but its head moves: the next capture's patch
// applies to this one's state, so successive captures form a patch chain
// that c4 log lists and c4 patch resolves. Whether to append each patch
// to the commit's c4m or keep it as a separate changeset is the caller's
// choice; both need only what a Changeset holds. Resume rebuilds a
// workspace from its base View, the chain of changesets, and the store
// they were captured into.
//
// # Symbolic links
//
// Lstat and Readlink see links; Stat, Open and ReadDir follow them, at
// most MaxSymlinks deep. Relative targets resolve against the link's
// directory and absolute targets against the root. Paths are cleaned
// lexically, so ".." after a link does not return through it. A
// workspace's Symlink makes links, and OpenFile with O_CREATE through a
// dangling link creates its target, as os does.
//
// # Concurrency
//
// A View is safe for any number of concurrent readers, as an NFS server
// needs. A File's ReadAt calls may run in parallel; Read and Seek share
// the File's offset. The working directory Chdir sets belongs to the
// View, and so is shared by everyone using it: code that shares a View
// should use absolute paths. A Workspace is safe for concurrent use;
// Capture holds its lock throughout, so writers wait for it.
//
// A workspace handle describes what it was opened on as it is now:
// Stat reports a later Chmod or write, and a directory handle lists the
// directory afresh after a rewind (Seek to 0). One pass of a listing is
// a snapshot, so its pages never skip or repeat a name. A read-only
// handle on a file not yet copied to scratch keeps reading the View's
// content if the file is written through another handle later, as with
// overlayfs.
//
// # Errors
//
// Filesystem methods (Stat, Open, Read, Rename and the rest) return an
// *fs.PathError (*os.LinkError for Rename and Symlink) naming the path
// and the operation. Functions that open or build a tree (Open,
// OpenConfigured, New, View.Apply, Resume) and Capture add context to
// the package's errors but promise no *fs.PathError. Test for causes
// with errors.Is either way.
package c4fs
