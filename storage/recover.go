package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

// ---------------------------------------------------------
// Storage Initialization and Reconstruction

func NewStorage(folder string, blockSize uint32, allocUnitSize uint32) (*Storage, error) {
	_, err := os.Stat(folder + initFileRelativePath)
	if os.IsNotExist(err) {
		// create an empty storage
		sg := newEmptyStorage(folder, blockSize, allocUnitSize)
		err = initEmptyStorage(sg)
		return sg, nil
	} else {
		// reload the storage
		sg, err := reloadStorage(folder)
		if err != nil {
			return nil, err
		}
		if sg.blockSize != blockSize {
			return nil, errors.New("block size not consistent")
		}
		if sg.blockFileStats.allocationUnitSize != allocUnitSize {
			return nil, errors.New("alloc unit size not consistent")
		}
		return sg, err
	}
}

func (sg *Storage) loadBlockStatistics(file *os.File) error {
	log.Fatalf("not implemented")
	return nil
}

func printEntryRecoveryResult(result *EntryRecoveryResult) {
	str := ""
	for _, f := range result.InvalidFiles {
		myBlock := f.RecoveredFmd.myBlock
		str += fmt.Sprintf("    At %v, offset %v units, size %v units, "+
			"Invalid Reason: %v\n", myBlock.relativeFilePath,
			myBlock.Offset, myBlock.Size, f.InvalidReason)
	}
	fmt.Printf("====== Storage Recorvery Result =====\n"+
		"  Valid File Count: %v\n"+
		"  Invalid Files: \n%v\n"+
		"  Empty16Bytes Count: %v\n", result.ValidFileCount, str,
		result.Empty16Bytes)
}

type EntryRecoveryResult struct {
	ValidFileCount int64
	InvalidFiles   []struct {
		RecoveredFmd  *FileMetadata
		InvalidReason string
	}
	Empty16Bytes int64
}

// recoverEntries scans through the metadata folder to load
// all valid files' metadata and stats info.
func recoverEntries(sg *Storage) (*EntryRecoveryResult, error) {
	folder := sg.folder
	dir, err := ioutil.ReadDir(folder + metaBlockRelativePathPrefix)
	if err != nil {
		return nil, err
	}
	result := &EntryRecoveryResult{}
	markInvalid := func(fmd *FileMetadata, reason string) {
		result.InvalidFiles = append(result.InvalidFiles,
			struct {
				RecoveredFmd  *FileMetadata
				InvalidReason string
			}{fmd, reason})
	}
	emptyBuf := make([]byte, 16)

	blockDir, err := ioutil.ReadDir(folder + dataBlockRelativePathPrefix)
	if err != nil {
		return nil, err
	}

	usedMetaBlocks := make([][]*blockUnits, len(dir))
	usedFileBlocks := make([][]*blockUnits, len(blockDir))

	for _, file_info := range dir {
		file_name := folder + "/" + file_info.Name()
		file, err := os.OpenFile(file_name, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(file_info.Name(), "fmd-") {
			return nil, errors.New(fmt.Sprintf("file %v: "+
				"name does not begin with fmd-", file_info.Name()))
		}
		for {
			fmd := &FileMetadata{
				myBlock: &FileBlockInfo{
					Index:  0,
					Offset: 0,
					Units:  0,
					Size:   0,
					// not used for a metadata block
					Crc32Checksum: 0,
				},
				loaded: false,
				Key:    "",
			}

			offset, err := file.Seek(0, io.SeekCurrent)
			if offset%int64(sg.blockMetaStats.allocationUnitSize) != 0 {
				return nil, errors.New(
					"a metadata offset cannot be divided by alloc unit size")
			}
			fmd.myBlock.Offset = uint32(offset /
				int64(sg.blockMetaStats.allocationUnitSize))

			buf := make([]byte, 16)
			_, err = io.ReadFull(file, buf)
			if err != nil {
				return nil, err
			}

			if bytes.Compare(buf, emptyBuf) == 0 {
				result.Empty16Bytes++
				continue
			}

			var blockIndex int64
			blockIndex, err = strconv.ParseInt(
				strings.TrimPrefix(file_info.Name(), "fmd-"),
				10, 64)
			fmd.myBlock.Index = uint32(blockIndex)

			keySize := binary.BigEndian.Uint16(buf[10:12])

			fmd.Key = string(buf[16 : 16+keySize])
			if keySize%16 != 0 {
				keySize += 16 - keySize%16
			}

			fmd.myBlock.Size = uint32(keySize) + fmd.BlockCount*16 + 16
			fmd.myBlock.Units = uint32(divCeil(int64(fmd.myBlock.Size),
				int64(sg.blockMetaStats.allocationUnitSize)))

			// load fmd to get block stats; then unload
			err = sg.loadFileMetadata(fmd)
			if err != nil {
				markInvalid(fmd, err.Error())
				continue
			}

			sg.fileEntries.key2file[fmd.Key] = fmd
			result.ValidFileCount++
			if usedMetaBlocks[blockIndex] == nil {
				usedMetaBlocks[blockIndex] = make([]*blockUnits, 0)
			}
			usedMetaBlocks[blockIndex] = append(usedMetaBlocks[blockIndex],
				&blockUnits{
					from: fmd.myBlock.Offset,
					to:   fmd.myBlock.Offset + fmd.myBlock.Units,
				})

			for _, dataBlock := range fmd.Blocks {
				dataBlockIndex := dataBlock.Index
				if usedFileBlocks[dataBlockIndex] == nil {
					usedFileBlocks[dataBlockIndex] = make([]*blockUnits, 0)
				}
				usedFileBlocks[dataBlockIndex] = append(usedFileBlocks[dataBlockIndex],
					&blockUnits{
						from: dataBlock.Offset,
						to:   dataBlock.Offset + dataBlock.Units,
					})
			}

			sg.unloadFileMetadata(fmd)
		}
	}
	rebuildBlockStats(sg.blockMetaStats, usedMetaBlocks)
	rebuildBlockStats(sg.blockFileStats, usedFileBlocks)
	return result, nil
}

func rebuildBlockStats(stats *BlockStatistics, blocks [][]*blockUnits) {
	stats.spaceInfo = make([]*BlockSpaceInfo, len(blocks))
	for i := range blocks {
		info := &BlockSpaceInfo{
			relativeFilePath:        fmt.Sprintf(stats.pathPrefix+"%v", i),
			usedUnitsMap:            map[uint32]*blockUnits{},
			spareUnitsRBTree:        redblacktree.NewWith(blockUnitsComparator),
			maxSpareSequentialUnits: 0,
			index:                   uint32(i),
		}
		units := blocks[i]
		sort.Sort(blockUnitsArray(units))
		var prevUnit *blockUnits = nil
		for _, unit := range units {
			if prevUnit == nil {
				if unit.from != 0 {
					prevUnit = &blockUnits{
						from: 0,
						to:   unit.from,
					}
					info.spareUnitsRBTree.Put(prevUnit, nil)
				}
			} else {
				if unit.from != prevUnit.to {
					prevUnit = &blockUnits{
						from: prevUnit.to,
						to:   unit.from,
					}
					info.spareUnitsRBTree.Put(prevUnit, nil)
				}
			}
			info.usedUnitsMap[unit.from] = unit
			unit.prev = prevUnit
			if prevUnit != nil {
				prevUnit.next = unit
			}
			prevUnit = unit
		}
		if prevUnit == nil {
			bu := &blockUnits{
				from: 0,
				to:   stats.unitPerBlock,
			}
			info.spareUnitsRBTree.Put(bu, nil)
		} else {
			if prevUnit.to != stats.unitPerBlock {
				bu := &blockUnits{
					from: prevUnit.to,
					to:   stats.unitPerBlock,
				}
				prevUnit.next = bu
				bu.prev = prevUnit
				info.spareUnitsRBTree.Put(bu, nil)
			}
		}
		maxItem := info.spareUnitsRBTree.Right()
		if maxItem == nil {
			info.maxSpareSequentialUnits = 0
		} else {
			info.maxSpareSequentialUnits = maxItem.Key.(*blockUnits).to -
				maxItem.Key.(*blockUnits).from
		}
		stats.sequentialUnitsRBTree.Put(&spareBlocks{
			index:                   info.index,
			maxSpareSequentialUnits: info.maxSpareSequentialUnits,
		}, nil)

		stats.spaceInfo[i] = info
	}

	stats.blockAllocCount = uint32(len(stats.spaceInfo))
}

func reloadStorage(folder string) (*Storage, error) {
	// read key info of this storage
	file, err := os.OpenFile(folder+entryFileRelativePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}
	var str string
	reader := bufio.NewReader(file)
	str, err = reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	blockSize, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, err
	}
	str, err = reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	allocUnitSize, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}

	sg := newEmptyStorage(folder, uint32(blockSize), uint32(allocUnitSize))

	recoveryResult, err := recoverEntries(sg)
	if err != nil {
		return nil, err
	}

	printEntryRecoveryResult(recoveryResult)
	return sg, nil
}

func newEmptyStorage(folder string, blockSize uint32, allocUnitSize uint32) *Storage {
	sg := &Storage{}
	sg.folder = folder
	sg.blockSize = blockSize
	sg.fileEntries = &FileEntries{
		key2file: map[string]*FileMetadata{},
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

func initEmptyStorage(sg *Storage) error {
	err := os.MkdirAll(sg.folder, os.FileMode(0777))
	if err != nil {
		return err
	}
	err = os.Mkdir(sg.folder+"/data-blocks", os.FileMode(0777))
	if err != nil {
		return err
	}
	err = os.Mkdir(sg.folder+"/file-metadata-blocks", os.FileMode(0777))
	if err != nil {
		return err
	}

	// write key info of this storage
	file, err := os.OpenFile(sg.folder+entryFileRelativePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	_, err = file.Write([]byte(fmt.Sprintf("FileBlockSize: %v\n"+
		"FileAllocUnitSize: %v\n", sg.blockSize, sg.blockFileStats.allocationUnitSize)))
	err = file.Sync()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	// mark the storage as initialized
	file, err = os.OpenFile(sg.folder+initFileRelativePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	_, err = file.Write([]byte("initialized"))
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func lockStorage(sg *Storage) error {
	file, err := os.OpenFile(sg.folder+"/lock", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	_, err = file.Write([]byte("locked"))
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func unlockStorage(sg *Storage) error {
	err := os.Remove(sg.folder + "/lock")
	return err
}

func (sg *Storage) CloseStorage() error {
	return unlockStorage(sg)
}
