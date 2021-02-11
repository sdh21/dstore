package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/emirpasic/gods/trees/redblacktree"
	"io"
	"log"
	"os"
	"sync"
)

type Storage struct {
	folder    string
	blockSize uint32

	entryCount uint32
	entries    map[string]*Entry

	fileEntries *FileEntries

	// a disk file that is opened
	openedFiles map[string]*OpenedFile
	// Cache OpenedFile with ref count=0
	cachedOpenedFiles map[string]*OpenedFile
	openedFilesLock   sync.Mutex

	// see allocation.go
	blockFileStats *BlockStatistics
	blockMetaStats *BlockStatistics
}

const entryFileRelativePath = "/storage-metadata"
const dataBlockRelativePathPrefix = "/data-blocks/data-"
const metaBlockRelativePathPrefix = "/file-metadata-blocks/fmd-"

const defaultMetadataAllocUnitSize = 1 * 1024 // 1KB
const defaultBlockSize = 64 * 1024 * 1024     // 64MB

// Empty files are not supported!
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
	fmd, found := sg.fileEntries.key2file[key]
	if !found {
		return nil, errors.New("key does not exist")
	}
	err := sg.loadFileMetadata(fmd)
	if err != nil {
		return nil, err
	}
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
	fmd, found := sg.fileEntries.key2file[key]
	if !found {
		return errors.New("key does not exist")
	}
	sg.deleteFile(fmd)
	return nil
}

//  caller should do CreateLargeFileAlloc -> write file, set crc32, size, ...
//  -> CreateLargeFileFinish
func (sg *Storage) CreateLargeFileAlloc(key string, size int64) (*FileMetadata, error) {
	blocks, err := sg.createFile(size, nil, false, sg.blockFileStats)
	if err != nil {
		return nil, err
	}
	fmd, err := sg.createMetadata(key, size, blocks, FlagFileInvalid)
	return fmd, err
}

func (sg *Storage) CreateLargeFileFinish(fmd *FileMetadata) error {
	err := sg.updateMetadata(fmd.Key, fmd)
	return err
}

func (sg *Storage) GetFileMetadata(key string) (*FileMetadata, error) {
	fmd, found := sg.fileEntries.key2file[key]
	if !found {
		return nil, errors.New("key does not exist")
	}
	err := sg.loadFileMetadata(fmd)
	if err != nil {
		return nil, err
	}
	return fmd, nil
}

func (sg *Storage) GetLargeFileOpenBlock(block *FileBlockInfo) (*OpenedFile, error) {
	file, err := sg.openBlock(block.relativeFilePath, false)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (sg *Storage) GetLargeFileCloseBlock(file *OpenedFile) error {
	sg.closeBlock(file)
	return nil
}

// Metadata may be unnecessary for log files, use this method to
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

// ---------------------------------------------------------
// Storage Initialization and Reconstruction

func (sg *Storage) loadBlockStatistics(file *os.File) error {
	log.Fatalf("not implemented")
	return nil
}

func (sg *Storage) loadEntries(file *os.File) bool {
	reader := bufio.NewReader(file)
	buf := make([]byte, 70)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return false
	}
	sg.blockSize = binary.BigEndian.Uint32(buf[0:4])
	sg.blockFileStats.allocationUnitSize = binary.BigEndian.Uint32(buf[4:8])
	sg.entryCount = binary.BigEndian.Uint32(buf[8:12])

	// metadata allocation unit size is always 1KB
	sg.blockMetaStats.allocationUnitSize = 1 * 1024

	for i := uint32(0); i < sg.entryCount; i++ {

		_, err := io.ReadFull(reader, buf)
		if err != nil {
			return false
		}
		//flag := uint32(buf[1]) | uint32(buf[0])<<8
		/*
			if (flag | entryFlagIsFileMetadata) != 0 {
				// This entry stores a file metadata
				fmd := &FileMetadata{}
				fmdIndex := binary.BigEndian.Uint32(buf[2:6])
				fmd.myBlock = sg.loadBlockInfoFromBuf(buf[6:20])
				fmd.myBlock.relativeFilePath = fmt.Sprintf(metaBlockRelativePathPrefix+ "%v", fmdIndex)
				fmd.Flag = flag
				//sg.fileEntries.key2file[fmdIndex] = fmd
			} else {
				// This entry stores a file
				key := string(buf[2:66])
				entry := &Entry{}
				entry.metadataIndex = binary.BigEndian.Uint32(buf[66:70])
				entry.flag = flag
				sg.entries[key] = entry
			}
		*/

	}
	return true
}

func NewEmptyStorage(folder string, blockSize uint32, allocUnitSize uint32) *Storage {
	sg := &Storage{}
	sg.folder = folder
	sg.blockSize = blockSize
	sg.entries = map[string]*Entry{}
	sg.fileEntries = &FileEntries{
		fileCount: 0,
		key2file:  map[string]*FileMetadata{},
	}
	sg.blockFileStats = &BlockStatistics{
		allocationUnitSize:    allocUnitSize,
		unitPerBlock:          sg.blockSize / allocUnitSize,
		blockAllocCount:       0,
		pathPrefix:            dataBlockRelativePathPrefix,
		spaceInfo:             make([]*BlockSpaceInfo, 0),
		sequentialUnitsRBTree: redblacktree.NewWith(spareBlocksComparator),
	}
	sg.blockMetaStats = &BlockStatistics{
		allocationUnitSize:    defaultMetadataAllocUnitSize,
		unitPerBlock:          sg.blockSize / allocUnitSize,
		blockAllocCount:       0,
		pathPrefix:            metaBlockRelativePathPrefix,
		spaceInfo:             make([]*BlockSpaceInfo, 0),
		sequentialUnitsRBTree: redblacktree.NewWith(spareBlocksComparator),
	}
	sg.openedFiles = make(map[string]*OpenedFile)
	sg.cachedOpenedFiles = make(map[string]*OpenedFile)
	return sg
}

func NewStorage(folder string, blockSize uint32, allocUnitSize uint32) (*Storage, error) {
	sg := &Storage{}
	sg.folder = folder
	file, err := os.OpenFile(folder+entryFileRelativePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == os.ErrExist {
		ok := sg.loadEntries(file)
		if !ok {
			return nil, errors.New("entry data corrupted")
		}
	} else {
		// create an empty storage

	}

	return sg, nil
}
