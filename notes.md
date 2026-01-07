# Basic architecture

Files are divided into fixed-size blocks (1GB), which are addressed solely by their SHA256 hash.

Blocks are synchronized and stored first, and once all blocks for a file are available, the file can then be reconstructed. Files can share blocks.

Blocks are stored remotely on an object store and/or network filesystem, under `blocks/<size>/<hash:2>/<hash:4>/hash` - e.g. `blocks/64M/ab/abf3/abf3523fa30b37174c2b7bf18dee58069544f772bfabdc964cf2fc6d6f26d4a1`.

Directories are implicit.


File:
 * `path` (string, key) Path incl. filename. / is path separator.
 * `version` (string, key) SHA256 hash of type + blocks
 * `blocks` (list[str]) SHA256 hashes of the blocks that make up the file, in order. Empty list for deleted or zero length files.
 * `type` (int) Type of the file (normal/executable/deleted)
 * `mtime` (uint64) Modification time of the file (and how we pick the "latest" version)
 * `size` (uint64) Filesize in bytes
 * `backends` (json) Backend IDs it is already on
 * `state` (string) One of REMOTE, DESIRED, DOWNLOADING, LOCAL


LocalFile:

 * `path` (string, key) File path (relative to root, as it would be in File)
 * `version` (null or string) File version it was matched to
 * `mtime` (int) On-disk mtime that the matching was done with
 * `state` (string) One of NEW, HASHING, MATCHED


Backend:

 * `id` (int, key) Auto ID
 * `type` (string) Identifies backend class to use
 * `options` (json) Options for that backend
 * `download_priority` (int) If we prefer to download from here (higher is better)
 * `state` (string) One of ONLINE, OFFLINE, ERROR
 * `error` (string) Human-readable error details


Block:

 * `sha256sum` (string, key) Hash/ID
 * `backends` (json) Backend IDs it is already on
 * `files` (json) File [path, version] tuples it is used in


BlockTransfer:

 * `sha256sum` (string) Hash/ID
 * `backend` (int) Backend ID
 * `direction` (int) 1 for upload, 2 for download
 * `state` (string) One of PENDING, TRANSFERRING, COMPLETE

## Reconciliation

Synchronization is done via a reconcilation loop.

There is an overall "file filter" system that for any path can say what mode to use:
 * FULL - Download all new files, upload all changes and new files, and propagate deletions
 * SPARSE - Download files on request, upload all changes and new files, but take deletion as un-requesting the file
 * SPARSE_DOWN - Download files on request, do not upload any changes, new files or deletions.

When a new file is discovered locally, or an existing file updated locally:

 * A LocalFile is created/updated for the disk path in state NEW
   * If there was an existing LocalFile for this path, the File previously linked
     to it is moved into state REMOTE and then its details cleared from the LocalFile
 * A loop comes along and sets it into state HASHING
   * The file has its blocks scanned and hashed and the resulting File path/version calculated
   * If it does not match an existing File, a new File is created with no backends listed in state LOCAL
   * `version` is filled with the File it matches to and state set to MATCHED
 * A loop looks for Blocks that are missing from Backends and adds them
    * It does this by looping through the Blocks table and seeing what backends each one has listed, and thus what is missing
    * If a block is needed in a backend a BlockTransfer object is made
    * Another loop just satisfies BlockTransfer requests
 * A loop looks for Files that have backends missing in their `backends` field
   * It updates the backend's file database in batches with missing files and then adds the backend into the `backends` field


When a remote File needs to be downloaded and added locally:

 * The File object is downloaded from the Backend at some point and appears in the local database with state REMOTE
 * A loop examines it and determines if it should go to state DESIRED
 * Another loop goes through each DESIRED file and downloads them block by block into a temporary file (sourcing blocks from the 'best' backend)
   * Once the download is done, it moves the finished file into place and puts the File into state LOCAL
   * A LocalFile object is created to match
   * The one exception is if the File is of type DELETED, in which case any local version is unlinked and no LocalFile row is created.


When a local File is deleted but should not be deleted remotely, as per file filter:

 * The LocalFile row is removed
 * The File it was pointing to is moved into state REMOTE


When a local File is deleted and should be deleted remotely, as per file filter:

 * The LocalFile row is removed
 * A new File row is created with empty blocks, zero size, current mtime, type deleted, empty list of backends, and state LOCAL


When someone wants to truly purge a file from the entire system (phase 2):

 * The File database on each backend must be rewritten to remove all matching path entries
 * Orphaned blocks should be discovered and removed from each backend
