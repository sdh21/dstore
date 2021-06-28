package StorageServer

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/storage"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
	"time"
)

// StorageServer wraps a storage.Storage and provides HTTP API,
// so users can directly upload file content to storage servers.
type StorageServer struct {
	// authentication, user-storage-token->files
	users     map[string]*UserData
	usersLock sync.Mutex

	sg *storage.Storage

	// file key to read handler
	readHandlers     map[string]*FileContentHandler
	readHandlersLock sync.Mutex

	// file key to write handler
	writeHandlers     map[string]*FileUploadHandler
	writeHandlersLock sync.Mutex

	UnimplementedFileServiceServer
	server *grpc.Server
	router *gin.Engine
}

type UserData struct {
	// tokens -> keys of readonly files
	tokensRead map[string]string
	// tokens -> keys of pre-allocated files
	tokensWrite map[string]string
	mu          sync.Mutex
}

const BlockSize = 64 * 1024 * 1024
const AllocUnitSize = 4 * 1024

func NewStorageServer(folder string, tlsConfig *cert.MutualTLSConfig) (*StorageServer, error) {
	sg, err := storage.NewStorage(folder, BlockSize, AllocUnitSize)
	if err != nil {
		return nil, err
	}
	ss := &StorageServer{}
	ss.sg = sg
	ss.users = map[string]*UserData{}
	ss.readHandlers = map[string]*FileContentHandler{}
	ss.writeHandlers = map[string]*FileUploadHandler{}

	serverTLSConfig, _, err := cert.LoadMutualTLSConfig(tlsConfig)
	if err != nil {
		return nil, err
	}

	ss.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig)),
		grpc.MaxConcurrentStreams(20000))

	ss.router = gin.Default()
	ss.router.MaxMultipartMemory = 16 << 20 // 16MB
	ss.router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "PUT"},
		AllowHeaders:     []string{"Origin"},
		AllowCredentials: true,
		ExposeHeaders:    []string{"Content-Length"},
		MaxAge:           12 * time.Hour,
	}))
	ss.router.Use(gin.Logger())

	ss.router.POST("/write", ss.UploadFileContent())
	ss.router.GET("/read", ss.GetFile())

	return ss, nil
}

func (ss *StorageServer) StartServer(rpcListener net.Listener, httpAddr string) {
	RegisterFileServiceServer(ss.server, ss)
	go func() {
		err := ss.server.Serve(rpcListener)
		if err != nil {
			utils.Error("serve err: %v", err)
		}
	}()

	go func() {
		err := ss.router.Run(httpAddr)
		if err != nil {
			utils.Error("router err: %v", err)
		}
	}()
}

func (ss *StorageServer) GetUser(userToken string) (*UserData, bool) {
	ss.usersLock.Lock()
	sd, found := ss.users[userToken]
	ss.usersLock.Unlock()
	return sd, found
}

func (ss *StorageServer) GetOrCreateUser(userToken string) *UserData {
	ss.usersLock.Lock()
	defer ss.usersLock.Unlock()
	sd, found := ss.users[userToken]
	if found {
		return sd
	}
	sd = &UserData{
		tokensRead:  map[string]string{},
		tokensWrite: map[string]string{},
	}
	ss.users[userToken] = sd
	return sd
}

func (ss *StorageServer) GetReadFileKey(userToken string, fileToken string) (string, bool) {
	user, found := ss.GetUser(userToken)
	if !found {
		return "", false
	}
	user.mu.Lock()
	fileKey, found := user.tokensRead[fileToken]
	user.mu.Unlock()
	if !found {
		return "", false
	}
	return fileKey, true
}

func (ss *StorageServer) GetWriteFileKey(userToken string, fileToken string) (string, bool) {
	user, found := ss.GetUser(userToken)
	if !found {
		return "", false
	}
	user.mu.Lock()
	fileKey, found := user.tokensWrite[fileToken]
	user.mu.Unlock()
	if !found {
		return "", false
	}
	return fileKey, true
}
