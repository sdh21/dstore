package storage

import (
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"github.com/emirpasic/gods/trees/redblacktree"
	"hash/crc32"
	"sync"
)

type Storage struct {
	folder    string
	blockSize uint32

	fileEntries *FileEntries

	// a disk file that is opened and has ref count > 0
	openedFiles map[string]*OpenedFile
	// Cache OpenedFile with ref count=0
	cachedOpenedFiles map[string]*OpenedFile
	openedFilesLock   sync.Mutex

	// see allocation.go
	blockFileStats *BlockStatistics
	blockMetaStats *BlockStatistics
}

const entryFileRelativePath = "/storage"
const dataBlockRelativePathPrefix = "/data-blocks/data-"
const metaBlockRelativePathPrefix = "/file-metadata-blocks/fmd-"
const initFileRelativePath = "/initialized"

const defaultMetadataAllocUnitSize = 1 * 1024 // 1KB
const defaultBlockSize = 64 * 1024 * 1024     // 64MB

func (sg *Storage) GetFileMetadata(key string) (*FileMetadata, error) {
	return sg.getFmdByKey(key)
}

// CreateSmallFile creates a file with the given key and value; empty files are not supported!
func (sg *Storage) CreateSmallFile(key string, size int64, value []byte) (*FileMetadata, error) {
	if size == 0 {
		return nil, errors.New("empty files not supported")
	}
	blocks, err := sg.createFile(size, value, true, sg.blockFileStats)
	if err != nil {
		return nil, err
	}
	fmd, err := sg.createMetadata(key, size, blocks, 0)
	return fmd, err
}

func (sg *Storage) GetSmallFile(key string) ([]byte, error) {
	fmd, err := sg.getFmdByKey(key)
	if err != nil {
		return nil, err
	}
	fmd.lock.Lock()
	defer fmd.lock.Unlock()
	fileContent := make([]byte, fmd.FileSize)
	offset := 0
	for _, block := range fmd.Blocks {
		file, err := sg.openBlock(block.relativeFilePath, true)
		if err != nil {
			return nil, err
		}
		begin := int64(block.Offset) * int64(sg.blockFileStats.allocationUnitSize)
		end := begin + int64(block.Size)
		copy(fileContent[offset:offset+int(block.Size)], file.memoryMap.Data[begin:end])
		offset += int(block.Size)
		sg.closeBlock(file)
	}
	return fileContent, nil
}

func (sg *Storage) DeleteFile(key string) error {
	fmd, err := sg.getFmdByKey(key)
	if err != nil {
		return err
	}
	sg.deleteFile(fmd)
	return nil
}

// CreateLargeFileAlloc pre-allocates the blocks needed, caller should write blocks by calling CreateLargeFileWrite,
// and finally call CreateLargeFileFinish
func (sg *Storage) CreateLargeFileAlloc(key string, size int64) (*FileMetadata, error) {
	blocks, err := sg.createFile(size, nil, false, sg.blockFileStats)
	if err != nil {
		return nil, err
	}
	fmd, err := sg.createMetadata(key, size, blocks, FlagFileInvalid)
	if err != nil {
		return nil, err
	}
	return fmd, err
}

type LargeFileIntegrityInfo struct {
	BlockChecksumCRC32 []uint32
	BlockSize          uint32
	FileSHA512         string
}

// CheckLargeFileIntegrity correctly sets blocks' size and crc32, and
// compute the file's sha512.
func (sg *Storage) CheckLargeFileIntegrity(fmd *FileMetadata) (*LargeFileIntegrityInfo, error) {
	fmd.lock.Lock()
	defer fmd.lock.Unlock()

	fileSHA512 := sha512.New()
	result := &LargeFileIntegrityInfo{}
	result.BlockChecksumCRC32 = make([]uint32, 0)

	for i, block := range fmd.Blocks {
		block.Size = sg.blockSize

		if i == len(fmd.Blocks)-1 {
			block.Size = uint32(fmd.FileSize % int64(sg.blockSize))
		}

		begin := int64(block.Offset) * int64(sg.blockFileStats.allocationUnitSize)
		end := begin + int64(block.Size)

		file, err := sg.openBlock(block.relativeFilePath, true)
		if err != nil {
			return nil, err
		}

		block.Crc32Checksum = crc32.ChecksumIEEE(file.memoryMap.Data[begin:end])
		fileSHA512.Write(file.memoryMap.Data[begin:end])

		result.BlockChecksumCRC32 = append(result.BlockChecksumCRC32, block.Crc32Checksum)

		sg.closeBlock(file)
	}

	result.FileSHA512 = hex.EncodeToString(fileSHA512.Sum(nil))
	result.BlockSize = sg.blockSize
	return result, nil
}

func (sg *Storage) CreateLargeFileFinish(newKey string, fmd *FileMetadata) error {
	err := sg.recreateMetadata(newKey, fmd, 0)
	if err != nil {
		return err
	}
	return nil
}

func (sg *Storage) CreateLargeFileWrite(block *FileBlockInfo, offset int64, content []byte) (int64, error) {
	file, err := sg.openBlock(block.relativeFilePath, true)
	if err != nil {
		return 0, err
	}
	defer sg.closeBlock(file)

	realOffset := int64(block.Offset)*int64(sg.blockFileStats.allocationUnitSize) + offset
	if realOffset >= file.memoryMap.Size {
		return 0, errors.New("invalid offset")
	}

	file.lock.Lock()
	defer file.lock.Unlock()
	length := copy(file.memoryMap.Data[realOffset:], content)
	return int64(length), nil
}

// GetBlockContent writes file[offset:] to buf and returns the the number of bytes copied
func (sg *Storage) GetBlockContent(block *FileBlockInfo, offset int64, buf []byte) (int64, error) {
	file, err := sg.openBlock(block.relativeFilePath, true)
	if err != nil {
		return 0, err
	}
	file.lock.Lock()
	defer file.lock.Unlock()
	beginPos := int64(block.Offset)*int64(sg.blockFileStats.allocationUnitSize) + offset
	contentSize := int64(block.Size) - offset
	bytesToRead := int64(len(buf))
	if bytesToRead > contentSize {
		bytesToRead = contentSize
	}
	if beginPos+bytesToRead > file.memoryMap.Size {
		return 0, errors.New("exceeds memory map size")
	}

	copy(buf[0:bytesToRead], file.memoryMap.Data[beginPos:beginPos+bytesToRead])

	sg.closeBlock(file)
	return bytesToRead, nil
}

// CreateLogFile : Metadata may be unnecessary for log files, use this method to
// create a log file in user-defined block group(indicated by stats)
// Users should have their own strategies to recover data from blocks
func (sg *Storage) CreateLogFile(size int64, value []byte, stats *BlockStatistics) ([]*FileBlockInfo, error) {
	blocks, err := sg.createFile(size, value, true, stats)

	if err != nil {
		return nil, err
	}
	return blocks, nil
}

func (sg *Storage) DeleteLogFile(blocks []*FileBlockInfo, stats *BlockStatistics) error {
	sg.releaseBlocks(blocks, stats)
	return nil
}

func (sg *Storage) CreateBlockGroup(allocUnitSize uint32, filePrefix string) *BlockStatistics {
	return &BlockStatistics{
		allocationUnitSize:    allocUnitSize,
		unitPerBlock:          sg.blockSize / allocUnitSize,
		blockAllocCount:       0,
		pathPrefix:            filePrefix,
		spaceInfo:             make([]*BlockSpaceInfo, 0),
		sequentialUnitsRBTree: redblacktree.NewWith(spareBlocksComparator),
	}
}
