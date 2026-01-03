# Basic architecture

Files are divided into fixed-size blocks (64MB), which are addressed solely by their SHA256 hash.

Blocks are synchronized and stored first, and once all blocks for a file are available, the file can then be reconstructed. Files can share blocks.

Blocks are stored remotely on an object store and/or network filesystem, under `blocks/<size>/<hash:2>/<hash:4>/hash` - e.g. `blocks/64M/ab/abf3/abf3523fa30b37174c2b7bf18dee58069544f772bfabdc964cf2fc6d6f26d4a1`.

File attributes:
 * `path` (string) Path incl. filename. / is path separator.
 * `blocks` (list[str]) SHA256 hashes of the blocks that make up the file, in order. Empty list for deleted or zero length files.
 * `mtime` (uint64) Modification time of the file
 * `size` (uint64) Filesize in bytes
 * `executable` (bool) If this file should be marked +x
 * `deleted` (bool) If this version of the file is a deletion

Files can be uniquely referenced by a combination of their `path` and `mtime` - `mtime` acts essentially as a version.

A checked-out copy of a File is only overwritten with the "latest" version
once its current version has been persisted in a quorum of backends.

Directories are implicit.

## Databases

There is a single, unified, global view of the filesystem - a union of all the possible sources (generally the File databases from each remote, plus the local one).

## Transfers

To upload a new file, first it is scanned and split into blocks, which are hashed. Then, for each block, we see if it is already on enough remotes, and if not, upload it to enough.

Then, once all the blocks have reached a quorate number of remotes, a file entry is created in the local database.
