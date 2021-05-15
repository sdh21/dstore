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
const FlagFileDeleted = 0b0100 | FlagFileInvalid

type FileEntries struct {
	lock     sync.Mutex
	key2file map[string]*FileMetadata
}

func (sg *Storage) getFmdByKey(key string) (*FileMetadata, error) {
	sg.fileEntries.lock.Lock()
	fmd, found := sg.fileEntries.key2file[key]
	sg.fileEntries.lock.Unlock()
	if !found {
		return nil, errors.New("key does not exist")
	}
	err := sg.loadFileMetadata(fmd)
	if err != nil {
		return nil, err
	}
	return fmd, nil
}

// FileMetadata will not be locked; consider it invalid when calling this function.
func (sg *Storage) deleteFile(fmd *FileMetadata) {
	if (fmd.Flag & FlagFileDeleted) != 0 {
		return
	}
	sg.fileEntries.lock.Lock()
	delete(sg.fileEntries.key2file, fmd.Key)
	sg.fileEntries.lock.Unlock()

	sg.releaseBlocks([]*FileBlockInfo{fmd.myBlock}, sg.blockMetaStats)
	sg.releaseBlocks(fmd.Blocks, sg.blockFileStats)
	fmd.Flag |= FlagFileDeleted
	// TODO: sync to disk
}

// recreateMetadata deletes the old FileMetadata without releasing blocks, and
// creates a new mapping between key and a new FileMetadata with identical block information.
func (sg *Storage) recreateMetadata(key string, oldFmd *FileMetadata, flag uint16) error {
	oldFmd.lock.Lock()
	oldFmd.Flag |= FlagFileDeleted
	oldKey := oldFmd.Key

	sg.fileEntries.lock.Lock()
	delete(sg.fileEntries.key2file, oldKey)
	sg.fileEntries.lock.Unlock()

	sg.releaseBlocks([]*FileBlockInfo{oldFmd.myBlock}, sg.blockMetaStats)

	_, err := sg.createMetadata(key, oldFmd.FileSize, oldFmd.Blocks, flag)

	oldFmd.lock.Unlock()
	return err
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
	defer sg.fileEntries.lock.Unlock()
	_, found := sg.fileEntries.key2file[key]
	if found {
		return nil, errors.New("impl, duplicate key")
	}
	sg.fileEntries.key2file[key] = meta
	return meta, nil
}

// ---------------------------------------------------------
// File Metadata Persistence

func (sg *Storage) writeFileMetadataToBuf(fmd *FileMetadata) []byte {
	fmd.lock.Lock()
	defer fmd.lock.Unlock()
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

// Load a file metadata from disk.
func (sg *Storage) loadFileMetadata(fmd *FileMetadata) error {
	fmd.lock.Lock()
	defer fmd.lock.Unlock()
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

// currently a no-op
func (sg *Storage) unloadFileMetadata(fmd *FileMetadata) {
	fmd.lock.Lock()
	defer fmd.lock.Unlock()
	fmd.loaded = false
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
