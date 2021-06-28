package FileServer

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/example/StorageServer"
	"github.com/sdh21/dstore/kvstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StorageServerInfo struct {
	client StorageServer.FileServiceClient
	conn   *grpc.ClientConn
}

type FileService struct {
	ts                   *TemporaryStorage
	storageProvidersLock sync.Mutex
	storageProviders     map[string]*StorageServerInfo
}

// TODO: we will not restrict storage providers to our own ones.
//     maybe we can support AWS S3, OSS and so on.

func (fs *FileService) GetStorageProvider(id string) *StorageServerInfo {
	fs.storageProvidersLock.Lock()
	defer fs.storageProvidersLock.Unlock()
	return fs.storageProviders[id]
}

func (fs *FileService) RegisterStorageProvider(id string, addr string, tlsConfig *cert.MutualTLSConfig) error {
	_, clientTLSConfig, err := cert.LoadMutualTLSConfig(tlsConfig)
	if err != nil {
		return err
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(
		credentials.NewTLS(clientTLSConfig)))
	if err != nil {
		return err
	} else {
		client := StorageServer.NewFileServiceClient(conn)
		fs.storageProvidersLock.Lock()
		fs.storageProviders[addr] = &StorageServerInfo{client: client,
			conn: conn}
		fs.storageProvidersLock.Unlock()
	}
	return nil
}

func NewFileService(ts *TemporaryStorage) *FileService {
	service := &FileService{
		ts:               ts,
		storageProviders: map[string]*StorageServerInfo{},
	}

	return service
}

func (fs *FileService) ListFile() gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := c.MustGet("userId").(string)
		result := fs.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  userId,
			Ops: []*kvstore.AnyOp{
				kvstore.OpGet([]string{"files"}, "files"),
			},
			TableVersionExpected: -1,
		})
		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if result.Status != kvstore.TransactionResult_OK {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		files, found := result.Values["files"]
		if !found {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if files.IsNull {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":    true,
			"files": files,
		})
	}
}

func (fs *FileService) InitFileReadLease() gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := c.MustGet("userId").(string)
		userToken := c.MustGet("userToken").(string)
		sessionToken := c.MustGet("sessionToken").(string)
		//sessionData := fs.ts.retrieveSession(sessionToken)

		fileFullName := c.PostForm("FileFullName")
		if fileFullName == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid file full name",
			})
		}

		storageProviderId := c.PostForm("StorageProviderId")

		if storageProviderId == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "empty storage provider id",
			})
			return
		}

		provider := fs.GetStorageProvider(storageProviderId)
		if provider == nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid storage provider",
			})
			return
		}

		// query the file key on the specified storage provider

		result := fs.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  userId,
			Ops: []*kvstore.AnyOp{
				// TODO: valid only
				kvstore.OpGet([]string{"files", fileFullName}, "file"),
			},
			TableVersionExpected: -1,
		})
		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if result.Status != kvstore.TransactionResult_OK {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		file, found := result.Values["file"]
		if !found {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if file.IsNull {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if file.Value.Type != "Map" {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		fileKey, found := file.Value.Map["key"]

		if !found {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		if fileKey.Str == "" {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		// register user read token to storage server

		identifier := "@@" + strconv.FormatInt(fs.ts.serverId, 10) + "-" + userId + "-" + sessionToken +
			"-" + storageProviderId
		fileToken := fs.ts.generateRandomUniqueToken(identifier)

		reply, err := provider.client.RegisterUserRead(context.Background(), &StorageServer.RegUserReadArgs{
			UserToken: userToken,
			FileToken: []string{fileToken},
			FileKey:   []string{fileKey.Str},
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"Message": err.Error(),
			})
			return
		}
		if !reply.OK {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"Message": reply.Message,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"UserToken": userToken,
			"FileToken": fileToken,
		})
	}
}

// UploadFileBegin pre-allocates disk space in the storage servers.
// We generate a storage token and subtract file size from user quota,
// and user will use this storage token to directly talk to
// storage servers.
//
// We also have a temporary file key, which is persistent in kvdb.
//
// If a previous upload fails,
// user quota can be given back at request.
func (fs *FileService) UploadFileBegin() gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := c.MustGet("userId").(string)
		userToken := c.MustGet("userToken").(string)
		sessionToken := c.MustGet("sessionToken").(string)
		sessionData := fs.ts.retrieveSession(sessionToken)

		fileHash := c.PostForm("FileHash")
		fileName := c.PostForm("FileName")
		fileSizeStr := c.PostForm("FileSizeStr")

		storageProviderId := c.PostForm("StorageProviderId")

		if fileName == "" || fileHash == "" || fileSizeStr == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "Empty field",
			})
			return
		}
		fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "wrong file size",
			})
			return
		}

		if storageProviderId == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "empty storage provider id",
			})
			return
		}

		provider := fs.GetStorageProvider(storageProviderId)
		if provider == nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid storage provider",
			})
			return
		}

		// include sessionToken in identifier, so same user in different sessions will not have
		// same temporary upload file key
		identifier := "@@" + strconv.FormatInt(fs.ts.serverId, 10) + "-" + userId + "-" + sessionToken +
			"-" + storageProviderId
		fileToken := fs.ts.generateRandomUniqueToken(identifier)
		tmpFileKey := identifier + "-" + fileToken

		sessionData.lock.Lock()

		_, found := sessionData.c[fileToken]
		if found {
			sessionData.lock.Unlock()
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"Message": "duplicate file token",
			})
			return
		}

		sessionData.c[fileToken] = "FileToken" + fileHash
		sessionData.c[fileToken+"##TmpKey"] = tmpFileKey
		sessionData.c[fileToken+"##FileName"] = fileName

		sessionData.lock.Unlock()

		result := fs.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  userId,
			Ops: []*kvstore.AnyOp{
				kvstore.OpMapStore([]string{"files"}, fileName, map[string]*kvstore.AnyValue{
					"key":               kvstore.MakeAnyValue(tmpFileKey),
					"size":              kvstore.MakeAnyValue(fileSize),
					"storage-providers": kvstore.MakeAnyValue([]string{"TODO"}),
					"valid":             kvstore.MakeAnyValue(0),
					// TODO:
				}),
				// TODO: add file size to reserved-capacity; if reserved-capacity > quota, abort&rollback
			},
			TableVersionExpected: -1,
		})
		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if result.Status != kvstore.TransactionResult_OK {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		// TODO: parse batch request here
		initReply, err := provider.client.UploadFileInit(context.Background(), &StorageServer.UploadInitArgs{
			FileKey:   []string{tmpFileKey},
			FileSize:  []int64{fileSize},
			UserToken: userToken,
			FileToken: []string{fileToken},
		})
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		if !initReply.OK {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"Message": initReply.Message,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"UserToken": userToken,
			"FileToken": fileToken,
		})
	}
}

func (fs *FileService) UploadFileEnd() gin.HandlerFunc {
	return func(c *gin.Context) {
		userId := c.MustGet("userId").(string)
		userToken := c.MustGet("userToken").(string)
		sessionToken := c.MustGet("sessionToken").(string)
		sessionData := fs.ts.retrieveSession(sessionToken)
		fileToken := c.PostForm("FileToken")

		storageProviderId := c.PostForm("StorageProviderId")

		if storageProviderId == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "empty storage provider id",
			})
			return
		}

		provider := fs.GetStorageProvider(storageProviderId)
		if provider == nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid storage provider",
			})
			return
		}

		sessionData.lock.Lock()

		fileHash, valid := sessionData.c[fileToken]
		if valid && !strings.HasPrefix(fileHash, "FileToken") {
			valid = false
		}
		var oldKey, fileName string

		if valid {
			fileHash = strings.TrimPrefix(fileHash, "FileToken")
			oldKey, valid = sessionData.c[fileToken+"##TmpKey"]
		}

		if valid {
			fileName, valid = sessionData.c[fileToken+"##FileName"]
		}

		sessionData.lock.Unlock()

		if !valid {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "file token is invalid or ##TmpKey/##FileName not found",
			})
			return
		}

		realFileKey := userId + "-" + fileHash

		finishReply, err := provider.client.UploadFileFinish(context.Background(), &StorageServer.UploadFinishArgs{
			UserToken:  userToken,
			FileToken:  fileToken,
			OldFileKey: oldKey,
			NewFileKey: realFileKey,
		})
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if !finishReply.OK {
			c.AbortWithStatusJSON(http.StatusPreconditionFailed, gin.H{
				"Message": finishReply.Message,
			})
		}

		result := fs.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  userId,
			Ops: []*kvstore.AnyOp{
				kvstore.OpMapStore([]string{"files", fileName}, "key", realFileKey).OpSetExpectedVersion(1),
				kvstore.OpMapStore([]string{"files", fileName}, "valid", 1),
			},
			TableVersionExpected: -1,
		})

		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		if result.Status != kvstore.TransactionResult_OK {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"Status":  result.Status,
				"Message": result.Message,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"OK": true,
		})
	}
}
