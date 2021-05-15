package storage

import (
	"errors"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"hash/crc32"
	"log"
	"os"
	"sync"
	"syscall"
)

// A BlockStatistics is a block group, corresponding to a folder on disk
type BlockStatistics struct {
	// a block group can has its own allocation config
	allocationUnitSize uint32
	unitPerBlock       uint32
	// how many blocks already allocated
	blockAllocCount uint32
	// this group's blocks are stored at pathPrefix + index
	pathPrefix            string
	spaceInfo             []*BlockSpaceInfo
	sequentialUnitsRBTree *redblacktree.Tree
	statsLock             sync.Mutex
}

type BlockSpaceInfo struct {
	relativeFilePath        string
	usedUnitsMap            map[uint32]*blockUnits
	spareUnitsRBTree        *redblacktree.Tree
	maxSpareSequentialUnits uint32
	lock                    sync.Mutex
	index                   uint32
}

type blockUnits struct {
	from uint32
	to   uint32
	prev *blockUnits
	next *blockUnits
}
type blockUnitsArray []*blockUnits

func (ua blockUnitsArray) Less(i int, j int) bool {
	return blockUnitsComparator(ua[i], ua[j]) < 0
}
func (ua blockUnitsArray) Len() int {
	return len(ua)
}
func (ua blockUnitsArray) Swap(i int, j int) {
	t := ua[i]
	ua[i] = ua[j]
	ua[j] = t
}

func blockUnitsComparator(a, b interface{}) int {
	ua := a.(*blockUnits)
	ub := b.(*blockUnits)
	sizeA := ua.to - ua.from
	sizeB := ub.to - ub.from
	if sizeA < sizeB {
		return -1
	}
	if sizeA > sizeB {
		return 1
	}
	if ua.from < ub.from {
		return -1
	}
	if ua.from > ub.from {
		return 1
	}
	return 0
}

type spareBlocks struct {
	index                   uint32
	maxSpareSequentialUnits uint32
}

func spareBlocksComparator(a, b interface{}) int {
	ua := a.(*spareBlocks)
	ub := b.(*spareBlocks)
	if ua.maxSpareSequentialUnits < ub.maxSpareSequentialUnits {
		return -1
	}
	if ua.maxSpareSequentialUnits > ub.maxSpareSequentialUnits {
		return 1
	}
	if ua.index < ub.index {
		return -1
	}
	if ua.index > ub.index {
		return 1
	}
	return 0
}

// ----------------------------------------------------------
// Low-level functions
// Disk File Manipulation

//  create a new file with given size and fill it with value if writeValue=true
//
//  if writeValue=false, caller should write the file and set crc32, size,...
//   and then call the createMetadata
func (sg *Storage) createFile(size int64, value []byte, writeValue bool, stats *BlockStatistics) ([]*FileBlockInfo, error) {
	if size == 0 {
		return nil, errors.New("impl, should not call this method")
	}
	// how many alloc unit needed
	unitCount := uint32(divCeil(size, int64(stats.allocationUnitSize)))
	blocks, err := sg.allocateUnits(unitCount, stats)
	if err != nil {
		return nil, err
	}
	// Set blocks' size
	for _, block := range blocks {
		block.Size = sg.blockSize
	}
	blocks[len(blocks)-1].Size = uint32(size % int64(sg.blockSize))
	// write value into file
	if writeValue {
		err := sg.writeValueToBlocks(size, value, blocks, stats)
		if err != nil {
			return nil, err
		}
	}
	return blocks, nil
}

func (sg *Storage) writeValueToBlocks(size int64, value []byte, blocks []*FileBlockInfo, stats *BlockStatistics) error {
	beginSrc := int64(0)
	for _, block := range blocks {
		if beginSrc >= size {
			// the remaining blocks have nothing to write
			block.Size = 0
			block.Crc32Checksum = 0
			continue
		}
		err := func() error {
			file, err := sg.openBlock(block.relativeFilePath, true)
			defer sg.closeBlock(file)
			if err != nil {
				return err
			}
			block.Size = block.Units * stats.allocationUnitSize
			beginDst := int64(block.Offset * stats.allocationUnitSize)
			endDst := beginDst + int64(block.Size)
			endSrc := beginSrc + int64(block.Size)
			if endSrc >= size {
				endSrc = size
				endDst = endSrc - beginSrc + beginDst
				block.Size = uint32(endSrc - beginSrc)
			}
			block.Crc32Checksum = crc32.ChecksumIEEE(value[beginSrc:endSrc])
			// file.lock.Lock()
			//fmt.Printf("copy from %v:%v to block %v %v:%v\n", beginSrc, endSrc, block.index, beginDst, endDst)
			if beginDst < 0 || endDst > file.memoryMap.Size {
				// we make boundary checks by ourselves
				log.Fatalf("memoryMap.Data[%v:%v], out of range %v", beginDst, endDst, file.memoryMap.Size)
			}
			copy(file.memoryMap.Data[beginDst:endDst], value[beginSrc:endSrc])
			err = file.memoryMap.Sync(beginDst, endDst)
			if err != nil {
				return err
			}
			// file.lock.Unlock()
			beginSrc += int64(block.Size)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	if beginSrc != size {
		return errors.New("allocated units too small")
	}
	return nil
}

// ----------------------------------------------------------
// Allocation & Release

// mark blocks as released and can be allocated later; note: only the partial of the block is released
// as indicated by FileBlockInfo
func (sg *Storage) releaseBlocks(blocks []*FileBlockInfo, stats *BlockStatistics) {
	stats.statsLock.Lock()
	defer stats.statsLock.Unlock()

	for _, block := range blocks {
		if block.deleted {
			log.Fatalf("a block deleted twice")
		}
		block.deleted = true
		ssinfo := stats.spaceInfo[block.Index]
		ssinfo.lock.Lock()
		item, found := ssinfo.usedUnitsMap[block.Offset]
		if !found {
			log.Fatalf("cannot find the block, check impl of releaseBlocks")
		}
		delete(ssinfo.usedUnitsMap, block.Offset)

		if item.prev != nil {
			prevItem, prevFound := ssinfo.usedUnitsMap[item.prev.from]
			if prevFound {
				if prevItem != item.prev {
					log.Fatalf("prev not consistent")
				}
			} else {
				// item.prev not in use, merge them
				ssinfo.spareUnitsRBTree.Remove(item.prev)
				mergedItem := &blockUnits{
					from: item.prev.from,
					to:   item.to,
					prev: item.prev.prev,
					next: item.next,
				}
				if item.prev.to != item.from {
					log.Fatalf("prev from, to not consistent")
				}
				if mergedItem.prev != nil {
					mergedItem.prev.next = mergedItem
				}
				if mergedItem.next != nil {
					mergedItem.next.prev = mergedItem
				}
				item = mergedItem
			}
		}

		if item.next != nil {
			nextItem, nextFound := ssinfo.usedUnitsMap[item.next.from]
			if nextFound {
				if nextItem != item.next {
					log.Fatalf("next not consistent")
				}
			} else {
				// item.next not in use, merge them
				ssinfo.spareUnitsRBTree.Remove(item.next)
				mergedItem := &blockUnits{
					from: item.from,
					to:   item.next.to,
					prev: item.prev,
					next: item.next.next,
				}
				if item.to != item.next.from {
					log.Fatalf("next from, to not consistent")
				}
				if mergedItem.prev != nil {
					mergedItem.prev.next = mergedItem
				}
				if mergedItem.next != nil {
					mergedItem.next.prev = mergedItem
				}
				item = mergedItem
			}
		}

		ssinfo.spareUnitsRBTree.Put(item, nil)

		if item.to-item.from > ssinfo.maxSpareSequentialUnits {
			stats.sequentialUnitsRBTree.Remove(&spareBlocks{
				index:                   ssinfo.index,
				maxSpareSequentialUnits: ssinfo.maxSpareSequentialUnits,
			})
			ssinfo.maxSpareSequentialUnits = item.to - item.from
			stats.sequentialUnitsRBTree.Put(&spareBlocks{
				index:                   ssinfo.index,
				maxSpareSequentialUnits: ssinfo.maxSpareSequentialUnits,
			}, nil)
		}

		ssinfo.lock.Unlock()
	}
}

// blocks allocation

// allocate units and return FileBlockInfo, in which
//    block index and os path is specified
// If a file needs more than one blocks,
//    full 64MB blocks come first in the return value

func (sg *Storage) allocateUnits(unitCount uint32, stats *BlockStatistics) ([]*FileBlockInfo, error) {

	newBlocksNeeded := unitCount / stats.unitPerBlock
	unitsLeft := unitCount % stats.unitPerBlock

	stats.statsLock.Lock()
	defer stats.statsLock.Unlock()

	lastBlockIndex := -2
	if unitsLeft == 0 {
		// ok, fit
	} else {
		lastBlockIndex = -1
		node, found := stats.sequentialUnitsRBTree.Ceiling(&spareBlocks{
			index:                   0,
			maxSpareSequentialUnits: unitsLeft,
		})
		if found {
			lastBlockIndex = int(node.Key.(*spareBlocks).index)
		}
	}

	if lastBlockIndex == -1 {
		newBlocksNeeded++
	}

	newBlocks := make([]*FileBlockInfo, newBlocksNeeded)
	for i := 0; i < int(newBlocksNeeded); i++ {
		blockIndex := stats.blockAllocCount
		stats.blockAllocCount++

		path := fmt.Sprintf(stats.pathPrefix+"%v", blockIndex)
		err := sg.allocateBlock(path)
		if err != nil {
			return nil, err
		}
		// mark this block as used
		usedBlock := &BlockSpaceInfo{
			relativeFilePath:        path,
			maxSpareSequentialUnits: 0,
			index:                   blockIndex,
		}
		usedBlock.spareUnitsRBTree = redblacktree.NewWith(blockUnitsComparator)
		usedBlock.usedUnitsMap = map[uint32]*blockUnits{}

		stats.spaceInfo = append(stats.spaceInfo, usedBlock)
		// do not set the last non-full block
		if lastBlockIndex != -1 {
			stats.sequentialUnitsRBTree.Put(&spareBlocks{
				index:                   blockIndex,
				maxSpareSequentialUnits: 0,
			}, nil)

			usedBlock.usedUnitsMap[0] = &blockUnits{
				from: 0, to: stats.unitPerBlock}
		}

		newBlocks[i] = &FileBlockInfo{
			Index:            blockIndex,
			Offset:           0,
			Units:            stats.unitPerBlock,
			Size:             0,
			Crc32Checksum:    0,
			relativeFilePath: path,
		}
	}

	if lastBlockIndex == -2 {
		// nothing left
	} else if lastBlockIndex == -1 {
		// the last block is what we have just allocated
		lastOne := stats.spaceInfo[stats.blockAllocCount-1]

		leftBlock := &blockUnits{from: 0, to: unitsLeft}
		rightBlock := &blockUnits{from: unitsLeft, to: stats.unitPerBlock}
		rightBlock.prev = leftBlock
		leftBlock.next = rightBlock

		lastOne.spareUnitsRBTree.Put(rightBlock, nil)
		lastOne.usedUnitsMap[0] = leftBlock

		lastOne.maxSpareSequentialUnits = stats.unitPerBlock - unitsLeft

		stats.sequentialUnitsRBTree.Put(&spareBlocks{
			index:                   lastOne.index,
			maxSpareSequentialUnits: stats.unitPerBlock - unitsLeft,
		}, nil)

		newBlocks[newBlocksNeeded-1].Size = unitsLeft * stats.allocationUnitSize
		newBlocks[newBlocksNeeded-1].Units = unitsLeft
	} else {
		// find sufficiently large units
		lastOne := stats.spaceInfo[lastBlockIndex]
		lastOne.lock.Lock()
		tree := lastOne.spareUnitsRBTree
		node, found := tree.Ceiling(&blockUnits{from: 0, to: unitsLeft})
		if !found {
			log.Fatalf("allocation wrong impl, check ceiling, maxSpareSequentialUnits")
		}
		// we must retrieve item before removing it!
		item := node.Key.(*blockUnits)
		tree.Remove(node.Key)
		if item.to-item.from < unitsLeft {
			log.Fatalf("allocation wrong impl, not enough")
		}

		// logger.Debug("Alloc: found spare block %v, %v:%v\n", lastBlockIndex, item.from, item.to)
		if item.to-item.from > unitsLeft {
			// we need to return unused units to spareUnits
			// we use item.from -> item.from + unitsLeft
			usedItem := &blockUnits{from: item.from, to: item.from + unitsLeft}
			spareItem := &blockUnits{from: item.from + unitsLeft, to: item.to}
			usedItem.prev = item.prev
			usedItem.next = spareItem
			spareItem.prev = usedItem
			spareItem.next = item.next
			if item.prev != nil {
				item.prev.next = usedItem
			}
			if item.next != nil {
				item.next.prev = spareItem
			}
			tree.Put(spareItem, nil)
			lastOne.usedUnitsMap[usedItem.from] = usedItem
		} else {
			lastOne.usedUnitsMap[item.from] = item
		}
		if item.to-item.from == lastOne.maxSpareSequentialUnits {
			maxItem := tree.Right()
			stats.sequentialUnitsRBTree.Remove(&spareBlocks{
				index:                   lastOne.index,
				maxSpareSequentialUnits: lastOne.maxSpareSequentialUnits,
			})
			if maxItem == nil {
				lastOne.maxSpareSequentialUnits = 0
				stats.sequentialUnitsRBTree.Put(&spareBlocks{
					index:                   lastOne.index,
					maxSpareSequentialUnits: 0,
				}, nil)
			} else {
				maxItem := maxItem.Key.(*blockUnits)
				lastOne.maxSpareSequentialUnits = maxItem.to - maxItem.from
				// logger.Debug("Alloc: maxSpareSequentialUnits updated to %v\n", lastOne.maxSpareSequentialUnits)
				stats.sequentialUnitsRBTree.Put(&spareBlocks{
					index:                   lastOne.index,
					maxSpareSequentialUnits: lastOne.maxSpareSequentialUnits,
				}, nil)
			}
		}
		lastOne.lock.Unlock()

		// add to blocks
		newBlocks = append(newBlocks, &FileBlockInfo{
			Index:            uint32(lastBlockIndex),
			Offset:           uint32(item.from),
			Units:            unitsLeft,
			Size:             0,
			Crc32Checksum:    0,
			relativeFilePath: stats.spaceInfo[lastBlockIndex].relativeFilePath,
		})
	}
	return newBlocks, nil
}

func (sg *Storage) allocateBlock(path string) error {
	file, err := os.OpenFile(sg.folder+path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err == os.ErrExist {
		return errors.New("block already exists")
	}
	if err != nil {
		return err
	}
	err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(sg.blockSize))
	if err != nil {
		return err
	}
	err = file.Close()
	return err
}

func (sg *Storage) fetchBlock(path string) *OpenedFile {
	sg.openedFilesLock.Lock()
	defer sg.openedFilesLock.Unlock()
	file, found := sg.openedFiles[path]
	if found {
		return file
	} else {
		return nil
	}
}

// To write a block, useMemMap must be true.
func (sg *Storage) openBlock(path string, useMemMap bool) (*OpenedFile, error) {
	sg.openedFilesLock.Lock()
	defer sg.openedFilesLock.Unlock()
	file, found := sg.openedFiles[path]
	if found {
		if useMemMap && file.File != nil {
			return nil, errors.New("file is already opened in file mode")
		}
		if !useMemMap && file.memoryMap != nil {
			return nil, errors.New("file is already opened in memory map mode")
		}
		file.refCount++
		return file, nil
	}
	// Is in cache?
	cached, found := sg.cachedOpenedFiles[path]
	if found {
		if cached.refCount != 0 {
			return nil, errors.New("implementation error")
		}
		modeValid := true
		if useMemMap && cached.memoryMap == nil {
			modeValid = false
		}
		if !useMemMap && cached.File == nil {
			modeValid = false
		}
		if modeValid {
			cached.refCount = 1
			sg.openedFiles[path] = cached
			delete(sg.cachedOpenedFiles, path)
			return cached, nil
		} else {
			err := sg.cacheCloseBlockNoLock(cached)
			if err != nil {
				return nil, err
			}
			delete(sg.cachedOpenedFiles, path)
			// re-open the file using new mode
		}
	}
	if !useMemMap {
		f, err := os.OpenFile(sg.folder+path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		file = &OpenedFile{}
		file.refCount = 1
		file.File = f
		file.relativeFilePath = path
	} else {
		memmap, err := NewFileMMap(sg.folder + path)
		if err != nil {
			return nil, err
		}
		file = &OpenedFile{}
		file.memoryMap = memmap
		file.refCount = 1
		file.relativeFilePath = path
	}

	sg.openedFiles[path] = file
	return file, nil
}

// closeBlock never fails. But it will panic if openBlock and closeBlock are not in pairs.
func (sg *Storage) closeBlock(file *OpenedFile) {
	sg.openedFilesLock.Lock()
	defer sg.openedFilesLock.Unlock()
	if file == nil {
		log.Printf("WARNING: closeBlock(nil)\n")
		return
	}
	if file.refCount == 0 {
		panic("ref count is already 0")
	}
	file.refCount--
	if file.refCount == 0 {
		// put it into cachedOpenedFiles
		sg.cachedOpenedFiles[file.relativeFilePath] = file
		delete(sg.openedFiles, file.relativeFilePath)
	}
}

func (sg *Storage) cacheCloseBlockNoLock(file *OpenedFile) error {
	var err error
	if file.memoryMap != nil {
		err = file.memoryMap.Close()
		file.memoryMap = nil
	}
	if file.File != nil {
		err = file.File.Close()
		file.File = nil
	}
	return err
}

func (sg *Storage) cacheCloseBlock(file *OpenedFile) error {
	sg.openedFilesLock.Lock()
	defer sg.openedFilesLock.Unlock()
	return sg.cacheCloseBlockNoLock(file)
}
