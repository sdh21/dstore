package storage

import (
	"os"
	"sync"
)

const LimitMaxFileBlockCount = 16384
const LimitMaxKeyLength = 65536

// How a file is located in blocks
type FileMetadata struct {
	// where is the block that stores this metadata
	myBlock *FileBlockInfo
	loaded  bool
	// file info
	Key        string
	BlockCount uint32
	FileSize   int64
	Blocks     []*FileBlockInfo
	Flag       uint16
}

// How a file's block is located on disk
type FileBlockInfo struct {
	// this block located on /folder/stats.prefix-index
	Index uint32
	// data begins from which unit
	Offset uint32
	// how many units allocated to this file
	Units uint32
	// how many bytes this file actually uses
	Size             uint32
	Crc32Checksum    uint32
	relativeFilePath string
	deleted          bool
}

type OpenedFile struct {
	readOnly         bool
	lock             sync.RWMutex
	File             *os.File
	memoryMap        *FileMMap
	refCount         int
	relativeFilePath string
}

type Entry struct {
	metadataIndex uint32
	flag          uint32
}

// unitCount := uint32(math.Ceil(float64(size) / float64(stats.allocationUnitSize)))
func divCeil(x int64, y int64) int64 {
	d := x / y
	if x%y != 0 {
		d++
	}
	return d
}
