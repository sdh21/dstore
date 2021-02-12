package forwarder

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/storage"
	"hash"
	"hash/crc32"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

// StorageServer wraps a storage.Storage and provides HTTP API,
// so users can directly talk to storage servers instead of
// asking forwarders to forward requests.
type StorageServer struct {
	// authentication, user-storage-token->files
	mu     sync.Mutex
	tokens map[string]*StorageData
	sg     *storage.Storage
}

type StorageData struct {
	// tokens -> keys of readonly files
	tokensRead map[string]string
	// tokens -> keys of pre-allocated files
	tokensWrite  map[string]string
	mu           sync.Mutex
	readHandlers map[string]*FileContentHandler
}

const BlockSize = 64 * 1024 * 1024
const AllocUnitSize = 4 * 1024

func NewStorageServer(folder string) (*StorageServer, error) {
	sg, err := storage.NewStorage(folder, BlockSize, AllocUnitSize)
	if err != nil {
		return nil, err
	}
	ss := &StorageServer{}
	ss.sg = sg
	ss.tokens = map[string]*StorageData{}
	return ss, nil
}

func (ss *StorageServer) GetFile() gin.HandlerFunc {
	return func(c *gin.Context) {
		userToken := c.PostForm("user-token")
		if userToken == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		fileToken := c.PostForm("file-token")
		if fileToken == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		fileName := c.PostForm("file-name")
		if fileName == "" {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		ss.mu.Lock()
		data, found := ss.tokens[userToken]
		ss.mu.Unlock()
		if !found {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		data.mu.Lock()
		fileKey, found := data.tokensRead[fileToken]
		data.mu.Unlock()
		if !found {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		// Auth passed
		fmd, err := ss.sg.GetFileMetadata(fileKey)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		data.mu.Lock()
		handler, found := data.readHandlers[fileKey]
		if !found {
			handler = NewFileContentHandler(fmd, ss.sg)
			data.readHandlers[fileKey] = handler
		}
		data.mu.Unlock()
		// do not let ServeContent sniff mime by reading the file
		ctype := mime.TypeByExtension(filepath.Ext(fileName))
		c.Writer.Header().Set("Content-Type", ctype)
		handler.mu.Lock()
		http.ServeContent(c.Writer, c.Request, "",
			time.Now(), handler)
		handler.mu.Unlock()
		return
	}
}

// FileContentHandler should not be used concurrently.
type FileContentHandler struct {
	fmd           *storage.FileMetadata
	sg            *storage.Storage
	blockChecked  []bool
	currentOffset int64
	mu            sync.Mutex
}

func (fc *FileContentHandler) Seek(offset int64, whence int) (int64, error) {
	newOffset := fc.currentOffset
	if whence == io.SeekStart {
		newOffset = offset
	} else if whence == io.SeekCurrent {
		newOffset += offset
	} else if whence == io.SeekEnd {
		newOffset = fc.fmd.FileSize - offset
	} else {
		return -1, errors.New("whence not valid")
	}
	if newOffset > fc.fmd.FileSize || newOffset < 0 {
		return -1, errors.New("offset not valid")
	}
	fc.currentOffset = newOffset
	return fc.currentOffset, nil
}

func NewFileContentHandler(fmd *storage.FileMetadata, sg *storage.Storage) *FileContentHandler {
	// check blocks
	size := int64(0)
	for i, block := range fmd.Blocks {
		if i != int(fmd.BlockCount-1) {
			// if this is not the last block
			if block.Size != BlockSize {
				panic("block size wrong")
			}
		}
		size += int64(block.Size)
	}
	if size != fmd.FileSize {
		panic("file size wrong")
	}
	return &FileContentHandler{
		fmd:          fmd,
		sg:           sg,
		blockChecked: make([]bool, fmd.BlockCount),
	}
}

// Reads a block and checks a block's crc32.
func (fc *FileContentHandler) read(idx int64, ret []byte) (int64, error) {
	checked := fc.blockChecked[idx]
	var checksum hash.Hash32
	if !checked {
		checksum = crc32.NewIEEE()
	}
	// OpenBlock is multi-thread safe
	block := fc.fmd.Blocks[idx]
	contentSize := int64(block.Size)
	file, err := fc.sg.GetLargeFileOpenBlock(block)
	defer func() {
		_ = fc.sg.GetLargeFileCloseBlock(file)
	}()
	if err != nil {
		return -1, err
	}
	_, err = file.File.Seek(int64(block.Offset)*int64(AllocUnitSize), io.SeekStart)
	if err != nil {
		return -1, err
	}

	offset := int64(0)
	retOffset := int64(0)
	sizeNeeded := int64(len(ret))
	for {
		if contentSize == offset {
			break
		}
		buf := make([]byte, 16384)
		if contentSize-offset < 16384 {
			buf = make([]byte, contentSize-offset)
		}
		n, err := file.File.Read(buf)
		if err != nil {
			return -1, err
		}
		if n != len(buf) {
			return -1, errors.New("unknown")
		}
		if !checked {
			_, err = checksum.Write(buf)
			if err != nil {
				return -1, err
			}
		}
		offset += int64(len(buf))
		if retOffset < sizeNeeded {
			retOffset += int64(copy(ret[retOffset:], buf))
		}
	}
	if !checked {
		if checksum.Sum32() != block.Crc32Checksum {
			return -1, errors.New("block corrupted")
		}
	}
	fc.blockChecked[idx] = true
	return retOffset, nil
}

func (fc *FileContentHandler) Read(p []byte) (int, error) {
	currentBlock := fc.currentOffset / BlockSize
	pSize := int64(len(p))
	pOffset := int64(0)
	var err error
	for {
		pOffset, err = fc.read(currentBlock, p[pOffset:])
		if err != nil {
			return 0, err
		}
		if pOffset >= pSize {
			break
		}
		currentBlock++
	}
	fc.currentOffset += pSize
	return int(pSize), nil
}
