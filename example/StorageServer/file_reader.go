package StorageServer

import (
	"context"
	"errors"
	"fmt"
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

func (ss *StorageServer) RegisterUserRead(ctx context.Context, args *RegUserReadArgs) (*RegUserReadReply, error) {
	user := ss.GetOrCreateUser(args.UserToken)

	user.mu.Lock()
	defer user.mu.Unlock()

	for i, token := range args.FileToken {
		user.tokensRead[token] = args.FileKey[i]
	}

	// TODO: add expiration and renewal

	reply := &RegUserReadReply{OK: true}
	return reply, nil
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
		fileKey, found := ss.GetReadFileKey(userToken, fileToken)
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

		ss.readHandlersLock.Lock()
		handler, found := ss.readHandlers[fileKey]
		if !found {
			handler = NewFileContentHandler(fmd, ss.sg)
			ss.readHandlers[fileKey] = handler
		}
		ss.readHandlersLock.Unlock()

		// do not let ServeContent sniff mime by reading the file
		ctype := mime.TypeByExtension(filepath.Ext(fileName))
		c.Writer.Header().Set("Content-Type", ctype)
		handler.mu.Lock()
		http.ServeContent(c.Writer, c.Request, "123456",
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

func (fc *FileContentHandler) readInBlock(blockIndex int64, offset int64, buf []byte) (int64, error) {
	block := fc.fmd.Blocks[blockIndex]
	if offset > int64(block.Size) {
		return 0, errors.New("offset exceeds valid size")
	}
	if offset >= int64(block.Size) {
		return 0, nil
	}

	bytesRead, err := fc.sg.GetBlockContent(block, offset, buf)
	if err != nil {
		return 0, err
	}
	return bytesRead, nil
}

// Reads a block and checks a block's crc32.
func (fc *FileContentHandler) checkBlock(idx int64) (int64, error) {
	checked := fc.blockChecked[idx]
	if checked {
		return 0, nil
	}
	var checksum hash.Hash32
	checksum = crc32.NewIEEE()

	// OpenBlock is multi-thread safe
	block := fc.fmd.Blocks[idx]
	contentSize := int64(block.Size)

	offset := int64(0)
	for {
		if contentSize == offset {
			break
		}
		buf := make([]byte, 16384)
		if contentSize-offset < 16384 {
			buf = make([]byte, contentSize-offset)
		}
		bytesRead, err := fc.sg.GetBlockContent(block, offset, buf)
		if err != nil {
			return -1, err
		}
		if bytesRead != int64(len(buf)) {
			return -1, errors.New("internal error: buf != bytesRead")
		}
		if !checked {
			_, err = checksum.Write(buf)
			if err != nil {
				return -1, err
			}
		}
		offset += int64(len(buf))
	}
	if !checked {
		if checksum.Sum32() != block.Crc32Checksum {
			return -1, errors.New("block corrupted")
		}
	}
	fc.blockChecked[idx] = true
	return 0, nil
}

func (fc *FileContentHandler) Read(p []byte) (int, error) {
	pSize := int64(len(p))
	pOffset := int64(0)
	for {
		currentBlock := fc.currentOffset / BlockSize
		offsetInner := fc.currentOffset % BlockSize
		_, err := fc.checkBlock(currentBlock)
		if err != nil {
			fmt.Printf("this error may be silent: %v\n", err)
			return 0, err
		}
		bytesRead, err := fc.readInBlock(currentBlock, offsetInner, p[pOffset:])
		if err != nil {
			fmt.Printf("this error may be silent %v\n", err)
			return 0, err
		}
		fc.currentOffset += bytesRead
		pOffset += bytesRead
		if pOffset >= pSize {
			break
		}
	}
	return int(pOffset), nil
}
