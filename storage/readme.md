<h1> Storage </h1>

- [Brief Introduction](#brief-introduction)
  - [1. Block, Allocation unit](#1-block-allocation-unit)
  - [2. File metadata](#2-file-metadata)
  - [3. Storage meta-info](#3-storage-meta-info)
  - [4. Allocation](#4-allocation)
- [Storage API](#storage-api)
  - [1. CreateSmallFile](#1-createsmallfile)
  - [2. GetSmallFile](#2-getsmallfile)
  - [3. DeleteFile](#3-deletefile)
  - [4. CreateLargeFileAlloc, CreateLargeFileFinish](#4-createlargefilealloc-createlargefilefinish)
  - [5. CreateBlockGroup, CreateLogFile, DeleteLogFile](#5-createblockgroup-createlogfile-deletelogfile)

# Brief Introduction

## 1. Block, Allocation unit

A block is a file that stores on disk. A block is composed of multiple allocation units. A block can contain multiple files.

Default block size: 64MB

Default allocation unit size: 4KB

Minimum allocation unit size: 1KB

A block is indexed by uint32.

An allocation unit in a block is indexed by uint16.

A file is promised to be sequentially allocated in a block.

A file might locate in multiple blocks, if and only if the file cannot fit in one block.

---
## 2. File metadata
A file metadata stores how a file locates in blocks.

The layout is:

(The amount in parentheses is how many bytes this field needs.)

  | Type | Field | Field | Field | Field |  Field |
  |------|-------|-------|-------|-------|--------|
  |  metadata | crc32 (4) | block count (4) | flag (2) | key size (2) | padding (4) |
  |  key 0 | .... | key content  | .... | |
  |  key 1 | .... | key content  | .... | |
  |  key ... | .... | key content  | ... 0000 |  padding to  | 16 bytes
  |  block 0 | location (4) | offset (2) | units (2) | size (4) | crc32 (4)
  |  block 1 | location (4) | offset (2) | units (2) | size (4) | crc32 (4)
  |  block ...

    A block's info needs 16 bytes to store.

    Location: the block's index. We use this to find the disk file

    Offset: the index of the first allocation unit

    Units: how many allocation units are allocated to this file

    Size: how many bytes the file actually use in this block

    CRC32: CRC32 checksum of the file content in this block

Since we have crc32 of both blocks and metadata, we are able to find out corrupted/invalid metadata and blocks.

Also, with the file's key stored, we do not need additional info to reconstruct file metadata. Just scanning the metadata folder is sufficient.

The padding field is "META".

File metadata are also stored in blocks, but with the limitation that a file metadata can only store in one block. This limits the maximum file size. For example, with default block size 64MB, a file metadata can store at most 64MB/16B - 96B(a reasonable header&key size)/16B = 4194298 blocks' info. Therefore, the default maximum file size is about 255.99 TB. However, a default value of LimitMaxFileBlockCount is set to 16384, which further limits a file's size to 1TB.

---
## 3. Storage meta-info

We also have some storage meta-info.

Storage meta-info does not need to be persistent. We can reconstruct them from file metadata.

However, having a storage meta-info can help us quickly restart the storage service.

When the storage service is gracefully stopped, we dump these in-memory data to disk:

    (1) blockFileStats (see storage.go), which contains the used/free file blocks' info.

    (2) blockMetaStats, which contains the used/free metadata blocks' info.

    (3) All FileMetadata structs (see common.go, if FileMetadata is not loaded, only its block info is dumped).

## 4. Allocation

The allocation is a simple best-fit allocation. That is to say, data is stored into a block (spare sequential allocation units in a block) that fulfills:

(1) It has enough space to store the data.

(2) Other blocks either have insufficient space or have more spare space than this block's.

If a file needs more than one blocks to store, we always allocate new blocks, and only the residual part of the file has the chance to be stored in an already allocated block.

<br>

# Storage API
This storage service is intended for some specific usages, so it does not support some normal operations (like appending to a file).

## 1. CreateSmallFile

Create a small file (typically < 1MB) with a key and a value (file content). CreateSmallFile does not actually check the size of the value, as long as it can fit in memory.

## 2. GetSmallFile

Get the content of a small file by the key. The content is copied into memory.

## 3. DeleteFile

Delete a file by key. The file is marked as deleted, but the file content is not cleared immediately, and will be overwritten by new files later.

## 4. CreateLargeFileAlloc, CreateLargeFileFinish

Use the two methods to create a large file. Users have to write file content and set size, crc32, and so on.

## 5. CreateBlockGroup, CreateLogFile, DeleteLogFile

If file metadata is unnecessary, users can use these three methods to manage pure data blocks. Storage of Paxos logs uses them to avoid metadata sync.

