package FileServer

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/utils"
	"time"
)

type FileServer struct {
	AuthService *AuthenticationService
	FileService *FileService
	ts          *TemporaryStorage
	router      *gin.Engine
}

func New(ts *TemporaryStorage) *FileServer {
	fs := &FileServer{
		AuthService: NewAuthService(ts),
		FileService: NewFileService(ts),
		ts:          ts,
	}

	router := gin.New()
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost"},
		AllowMethods:     []string{"GET", "PUT"},
		AllowHeaders:     []string{"Origin"},
		AllowCredentials: true,
		ExposeHeaders:    []string{"Content-Length"},
		MaxAge:           12 * time.Hour,
	}))
	router.Use(gin.Logger())

	router.Use(SessionMiddleware(fs.AuthService))

	router.POST("/login", fs.AuthService.Login())
	router.POST("/reg", fs.AuthService.Register())

	authorized := router.Group("/space")
	authorized.Use(AuthMiddleware(fs.AuthService))
	authorized.GET("/file-list", fs.FileService.ListFile())
	authorized.POST("/file_upload_init", fs.FileService.UploadFileBegin())
	authorized.POST("/file_upload_finish", fs.FileService.UploadFileEnd())

	authorized.POST("/init_file_read", fs.FileService.InitFileReadLease())

	router.MaxMultipartMemory = 16 << 20 // 16MB
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*", "192.168.50.17"},
		AllowMethods:     []string{"GET", "PUT"},
		AllowHeaders:     []string{"Origin"},
		AllowCredentials: true,
		ExposeHeaders:    []string{"Content-Length"},
		MaxAge:           12 * time.Hour,
	}))

	fs.router = router

	return fs
}

func (fs *FileServer) Start() {
	go func() {
		err := fs.router.Run(fs.ts.myHttpAddr)
		if err != nil {
			utils.Error("router err: %v", err)
		}
	}()
}
