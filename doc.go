// Package c4fs presents a c4m manifest as a read-only absfs filesystem,
// with file content read lazily by C4 ID from a c4 store.
//
// A c4m manifest (github.com/Avalanche-io/c4/c4m) describes a tree:
// names, modes, times, sizes, symbolic links and the C4 ID of every
// file's bytes. A c4 store (github.com/Avalanche-io/c4/store) holds those
// bytes by ID. A View joins the two into an absfs.SymlinkFileSystem, so a
// commit's tree can be listed, read, served over NFS by absnfs, or used
// as the shared read-only lower layer of a unionfs workspace, with
// nothing materialised on disk.
//
// # Opening
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
// else replaces).
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
// list.
//
// # Content
//
// Nothing is fetched until a file is first read. A store that keeps
// objects as local files (the c4 TreeStore) is read in place, with
// ReadAt and Seek; one whose readers are io.ReaderAt and io.Seeker is read
// directly; anything else is hydrated into a Cache, a local TreeStore,
// verified against the ID before it is used. Empty files are served
// without the store. A file whose content is absent reads as
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
// the *c4m.Entry, so callers can read C4 IDs.
//
// # Read-only
//
// Every mutating method fails with an *fs.PathError wrapping ErrReadOnly
// (syscall.EROFS), or with the more specific error a read-only mount
// gives: EEXIST, ENOENT, ENOTDIR or EISDIR. MkdirAll of an existing
// directory and RemoveAll of a missing name succeed, as in package os.
//
// # Symbolic links
//
// Lstat and Readlink see links; Stat, Open and ReadDir follow them, at
// most MaxSymlinks deep. Relative targets resolve against the link's
// directory and absolute targets against the View's root. Paths are
// cleaned lexically, so ".." after a link does not return through it.
//
// # Concurrency
//
// A View is safe for any number of concurrent readers, as an NFS server
// needs. A File's ReadAt calls may run in parallel; Read and Seek share
// the File's offset.
package c4fs
