package StorageServer

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/storage"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"sync"
)

// UploadFileInit should be called by forwarder through GRPC
// we only trust init requests sent by forwarder
func (ss *StorageServer) UploadFileInit(ctx context.Context, args *UploadInitArgs) (*UploadInitReply, error) {
	user := ss.GetOrCreateUser(args.UserToken)

	for i, _ := range args.FileKey {
		user.mu.Lock()
		user.tokensWrite[args.FileToken[i]] = args.FileKey[i]
		user.mu.Unlock()

		fmd, err := ss.sg.CreateLargeFileAlloc(args.FileKey[i], args.FileSize[i])
		if err != nil {
			return &UploadInitReply{OK: false, Message: "Alloc failed."}, nil
		}

		ss.writeHandlersLock.Lock()
		_, found := ss.writeHandlers[args.FileKey[i]]
		if found {
			ss.writeHandlersLock.Unlock()
			return &UploadInitReply{OK: false, Message: "Duplicate key."}, nil
		}
		ss.writeHandlers[args.FileKey[i]] = NewFileUploadHandler(fmd, ss.sg)
		ss.writeHandlersLock.Unlock()
	}

	reply := &UploadInitReply{OK: true}
	return reply, nil
}

func (ss *StorageServer) UploadFileContent() gin.HandlerFunc {
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
		fileOffsetStr := c.PostForm("offset")
		if fileOffsetStr == "" {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		fileOffset, err := strconv.ParseInt(fileOffsetStr, 10, 64)
		if err != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		fileKey, found := ss.GetWriteFileKey(userToken, fileToken)
		if !found {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		fileContent, err := c.FormFile("content")
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		// Auth passed
		fmd, err := ss.sg.GetFileMetadata(fileKey)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}

		ss.writeHandlersLock.Lock()
		handler, found := ss.writeHandlers[fileKey]
		if !found {
			handler = NewFileUploadHandler(fmd, ss.sg)
			ss.writeHandlers[fileKey] = handler
		}
		ss.writeHandlersLock.Unlock()

		handler.mu.Lock()
		err = handler.Write(fileOffset, fileContent)
		handler.mu.Unlock()

		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"OK": true,
		})
	}
}

// UploadFileFinish is called by forwarder to mark a file as uploaded and available, and revoke the
// file token.
func (ss *StorageServer) UploadFileFinish(ctx context.Context, args *UploadFinishArgs) (*UploadFinishReply, error) {

	ss.writeHandlersLock.Lock()
	handler, found := ss.writeHandlers[args.OldFileKey]
	ss.writeHandlersLock.Unlock()

	if !found {
		return &UploadFinishReply{
			OK:      false,
			Message: "Invalid Old file key",
		}, nil
	}
	fmd := handler.fmd

	info, err := ss.sg.CheckLargeFileIntegrity(fmd)
	if err != nil {
		return &UploadFinishReply{
			OK:      false,
			Message: "Check file integrity failed.",
		}, nil
	}

	// if the file hash is not consistent, we must reject the Finish request,
	// because data in released blocks are not immediately cleared, and
	// hackers might get other users' data, though this attack is mitigated by
	// client-side file encryption
	if info.FileSHA512 != args.GetFileSHA512() {
		return &UploadFinishReply{
			OK: false,
			Message: fmt.Sprintf("Check file integrity failed. Expected %v, got %v.", info.FileSHA512,
				args.GetFileSHA512()),
		}, nil
	}

	err = ss.sg.CreateLargeFileFinish(args.NewFileKey, fmd)
	if err != nil {
		return &UploadFinishReply{
			OK:      false,
			Message: "Create Large File Finish failed.",
		}, nil
	}

	user, found := ss.GetUser(args.UserToken)

	if !found {
		return &UploadFinishReply{
			OK:      false,
			Message: "User missing?",
		}, nil
	}

	user.mu.Lock()
	delete(user.tokensWrite, args.FileToken)
	user.mu.Unlock()

	reply := &UploadFinishReply{OK: true}
	return reply, nil
}

// FileUploadHandler should not be used concurrently.
type FileUploadHandler struct {
	fmd *storage.FileMetadata
	sg  *storage.Storage
	mu  sync.Mutex
}

func NewFileUploadHandler(fmd *storage.FileMetadata, sg *storage.Storage) *FileUploadHandler {
	return &FileUploadHandler{
		fmd: fmd,
		sg:  sg,
	}
}

func (fu *FileUploadHandler) Write(offset int64, content *multipart.FileHeader) error {
	if offset >= fu.fmd.FileSize || offset < 0 || offset+content.Size > fu.fmd.FileSize {
		return errors.New("invalid offset")
	}

	file, err := content.Open()
	if err != nil {
		return err
	}
	defer file.Close()

	contentLength := content.Size

	for contentLength > 0 {
		bytesCopiedExpected := int64(0)
		if contentLength > BlockSize {
			bytesCopiedExpected = BlockSize
		} else {
			bytesCopiedExpected = contentLength
		}

		block := offset / BlockSize
		offsetInBlock := offset % BlockSize

		if block >= int64(len(fu.fmd.Blocks)) {
			return errors.New("invalid block number")
		}

		buf := make([]byte, bytesCopiedExpected)
		_, err := io.ReadFull(file, buf)
		if err != nil {
			return err
		}
		bytesCopied, err := fu.sg.CreateLargeFileWrite(fu.fmd.Blocks[block], offsetInBlock, buf)
		if err != nil {
			return err
		}
		if bytesCopied != bytesCopiedExpected {
			return errors.New("bytes copied incorrect")
		}

		contentLength -= bytesCopiedExpected
		offset += bytesCopied
	}

	return nil
}
