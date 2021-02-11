package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

const FlagMetadata = 0b0001
const FlagFileInvalid = 0b0010
const FlagFileDeleted = 0b0100

type FileEntries struct {
	// how many files are in the storage
	fileCount int64
	lock      sync.Mutex
	key2file  map[string]*FileMetadata
}

// update a key->FileMetadata mapping
// THIS METHOD IS NOT TESTED. I AM NOT USING IT.
func (sg *Storage) updateMetadata(key string, newFmd *FileMetadata) error {
	sg.fileEntries.lock.Lock()
	fmd, found := sg.fileEntries.key2file[key]
	sg.fileEntries.lock.Unlock()
	if !found {
		return errors.New("key does not exist")
	}
	buf := sg.writeFileMetadataToBuf(newFmd)
	size := int64(len(buf))
	if size > int64(fmd.myBlock.Size) {
		// check if we need more alloc units
		spr := fmd.myBlock.Units*sg.blockMetaStats.allocationUnitSize - fmd.myBlock.Size
		if int64(spr) > size-int64(fmd.FileSize) {
			// we need to allocate more units
			unitCount := uint32(divCeil(size, int64(sg.blockMetaStats.allocationUnitSize)))
			blocks, err := sg.allocateUnits(unitCount, sg.blockMetaStats)
			if err != nil {
				return err
			}
			if len(blocks) != 1 {
				return errors.New("impl, metadata file can only have one block")
			}
			err = sg.writeValueToBlocks(size, buf, blocks, sg.blockMetaStats)
			if err != nil {
				return err
			}
			newFmd.myBlock = blocks[0]
			// OK.
			sg.fileEntries.lock.Lock()
			sg.fileEntries.key2file[key] = newFmd
			sg.fileEntries.lock.Unlock()
			// Release the old block
			sg.releaseBlocks([]*FileBlockInfo{fmd.myBlock}, sg.blockMetaStats)
			return nil
		}
	}
	// no need to allocate more units
	err := sg.writeValueToBlocks(size, buf, []*FileBlockInfo{fmd.myBlock}, sg.blockMetaStats)
	if err != nil {
		return err
	}
	newFmd.myBlock = fmd.myBlock
	sg.fileEntries.lock.Lock()
	sg.fileEntries.key2file[key] = newFmd
	sg.fileEntries.lock.Unlock()
	return nil
}

func (sg *Storage) deleteFile(fmd *FileMetadata) {
	if (fmd.Flag & FlagFileDeleted) != 0 {
		return
	}
	sg.fileEntries.lock.Lock()
	delete(sg.fileEntries.key2file, fmd.Key)
	sg.fileEntries.fileCount--
	sg.fileEntries.lock.Unlock()

	sg.releaseBlocks([]*FileBlockInfo{fmd.myBlock}, sg.blockMetaStats)
	sg.releaseBlocks(fmd.Blocks, sg.blockFileStats)
	fmd.Flag |= FlagFileDeleted
	// TODO: sync to disk

}

//  replace a (small) file's content with new value
func (sg *Storage) updateFile(fmd *FileMetadata, size int64, value []byte) error {
	stats := sg.blockFileStats
	if size > fmd.FileSize {
		// check if we need more alloc units
		// we do not know which blocks have spare spaces (though we can assume they only lie at the end)
		// just do an iteration given that it is a small file
		spr := int64(0)
		for _, block := range fmd.Blocks {
			spr += int64(block.Units*stats.allocationUnitSize - block.Size)
		}
		if spr > size-fmd.FileSize {
			// we need more alloc units
			// release the old one
			sg.releaseBlocks(fmd.Blocks, stats)
			// just create a new file
			blocks, err := sg.createFile(size, nil, false, stats)
			if err != nil {
				return err
			}
			fmd.Blocks = blocks
		}
	}

	err := sg.writeValueToBlocks(size, value, fmd.Blocks, stats)
	if err != nil {
		return err
	}

	newFmd := &FileMetadata{
		myBlock:    nil,
		Key:        fmd.Key,
		BlockCount: uint32(len(fmd.Blocks)),
		FileSize:   size,
		Blocks:     fmd.Blocks,
		Flag:       FlagMetadata,
	}
	err = sg.updateMetadata(fmd.Key, newFmd)
	if err != nil {
		return err
	}
	return nil
}

func (sg *Storage) createMetadata(key string, size int64, blocks []*FileBlockInfo, flag uint16) (*FileMetadata, error) {
	meta := &FileMetadata{}
	meta.Blocks = blocks
	meta.BlockCount = uint32(len(blocks))
	meta.FileSize = size
	meta.Key = key
	meta.Flag |= FlagMetadata
	meta.Flag |= flag

	buf := sg.writeFileMetadataToBuf(meta)

	file, err := sg.createFile(int64(len(buf)), buf, true, sg.blockMetaStats)
	if err != nil {
		return nil, err
	}
	if len(file) != 1 {
		return nil, errors.New("impl, file metadata must be in one block")
	}
	meta.myBlock = file[0]

	sg.fileEntries.lock.Lock()
	sg.fileEntries.fileCount++
	sg.fileEntries.key2file[key] = meta
	sg.fileEntries.lock.Unlock()
	return meta, nil
}

// ---------------------------------------------------------
// File Metadata Persistence

func (sg *Storage) writeFileMetadataToBuf(fmd *FileMetadata) []byte {
	buf := new(bytes.Buffer)
	header := make([]byte, 16)
	buf.WriteString(fmd.Key)
	if len(fmd.Key)%16 != 0 {
		buf.Write(make([]byte, 16-len(fmd.Key)%16))
	}

	binary.BigEndian.PutUint32(header[4:8], fmd.BlockCount)
	binary.BigEndian.PutUint16(header[8:10], fmd.Flag)
	binary.BigEndian.PutUint16(header[10:12], uint16(len(fmd.Key)))
	copy(header[12:16], "META")

	for _, block := range fmd.Blocks {
		b := sg.writeBlockInfoToBuf(block)
		buf.Write(b)
	}
	result := make([]byte, 0)
	result = append(result, header...)
	result = append(result, buf.Bytes()...)
	checksum := crc32.ChecksumIEEE(result[4:])
	binary.BigEndian.PutUint32(result[0:4], checksum)
	return result
}

// Load a file metadata from disk. To update file metadata, call updateMetadata.
func (sg *Storage) loadFileMetadata(fmd *FileMetadata) error {
	if fmd == nil {
		return errors.New("file metadata is nil")
	}
	if fmd.myBlock == nil {
		return errors.New("invalid fmd.myBlock")
	}
	if fmd.loaded {
		return nil
	}
	block, err := sg.openBlock(fmd.myBlock.relativeFilePath, true)
	if err != nil {
		return err
	}

	offset := int64(fmd.myBlock.Offset) * int64(sg.blockMetaStats.allocationUnitSize)

	mmBuf := block.memoryMap.Data[offset : offset+int64(fmd.myBlock.Size)]
	buf := make([]byte, len(mmBuf))
	copy(buf, mmBuf)

	checksum := binary.BigEndian.Uint32(buf[0:4])
	if crc32.ChecksumIEEE(buf[4:]) != checksum {
		return errors.New("metadata corrupted, checksum not correct")
	}

	fmd.BlockCount = binary.BigEndian.Uint32(buf[4:8])
	if fmd.BlockCount > LimitMaxFileBlockCount {
		return errors.New(fmt.Sprintf(
			"file metadata corrupted, a file contains %v blocks", fmd.BlockCount))
	}

	fmd.Flag = binary.BigEndian.Uint16(buf[8:10])
	keySize := binary.BigEndian.Uint16(buf[10:12])

	if !bytes.Equal(buf[12:16], []byte("META")) {
		return errors.New("metadata corrupted, META magic missing")
	}

	fmd.Key = string(buf[16 : 16+keySize])
	if keySize%16 != 0 {
		keySize += 16 - keySize%16
	}

	fmd.Blocks = make([]*FileBlockInfo, fmd.BlockCount)
	fmd.FileSize = 0

	for i := int64(0); i < int64(fmd.BlockCount); i++ {
		ptr := int64(keySize) + i*16 + 16
		blockInfo := sg.loadBlockInfoFromBuf(buf[ptr : ptr+16])
		fmd.FileSize += int64(blockInfo.Size)
		blockInfo.relativeFilePath = fmt.Sprintf(dataBlockRelativePathPrefix+"%v", blockInfo.Index)
		fmd.Blocks[i] = blockInfo
	}
	fmd.loaded = true
	return nil
}

func (sg *Storage) loadBlockInfoFromBuf(buf []byte) *FileBlockInfo {
	info := &FileBlockInfo{}
	info.Index = binary.BigEndian.Uint32(buf[0:4])
	info.Offset = uint32(buf[5]) | uint32(buf[4])<<8
	info.Units = uint32(buf[7]) | uint32(buf[6])<<8
	info.Size = binary.BigEndian.Uint32(buf[8:12])
	info.Crc32Checksum = binary.BigEndian.Uint32(buf[12:16])
	return info
}

func (sg *Storage) writeBlockInfoToBuf(info *FileBlockInfo) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], info.Index)
	buf[4] = byte(info.Offset >> 8)
	buf[5] = byte(info.Offset)
	buf[6] = byte(info.Units >> 8)
	buf[7] = byte(info.Units)
	binary.BigEndian.PutUint32(buf[8:12], info.Size)
	binary.BigEndian.PutUint32(buf[12:16], info.Crc32Checksum)
	return buf
}
