package StorageServer

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/hex"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/utils"
	"io/ioutil"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
)

func createTestEnv(t *testing.T) *StorageServer {
	err := os.RemoveAll("/tmp/store-test/storage_server/1")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewStorageServer("/tmp/store-test/storage_server/1", utils.TestTlsConfig())
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func testReadFile(t *testing.T, server *StorageServer, userToken string, fileToken string, key string, from int64, to int64, content []byte) {
	read, err := server.RegisterUserRead(context.Background(), &RegUserReadArgs{
		UserToken: userToken,
		FileToken: fileToken,
		FileKey:   key,
	})
	if err != nil {
		t.Fatal(read)
	}

	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)

	var tmpWriter bytes.Buffer
	request := multipart.NewWriter(&tmpWriter)
	_ = request.WriteField("user-token", userToken)
	_ = request.WriteField("file-token", fileToken)
	_ = request.WriteField("offset", "0")
	_ = request.WriteField("file-name", "file name w1")
	err = request.Close()
	if err != nil {
		t.Fatal(err)
	}

	ctx.Request = httptest.NewRequest("POST", "/", &tmpWriter)
	ctx.Request.Header.Set("Content-Type", request.FormDataContentType())
	ctx.Request.Header.Set("Range", "bytes="+strconv.FormatInt(from, 10)+
		"-"+strconv.FormatInt(to-1, 10))

	getFile := server.GetFile()

	getFile(ctx)

	if w.Code != http.StatusPartialContent {
		t.Fatal(w)
	}

	partial := w.Result().Body
	bd, err := ioutil.ReadAll(partial)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(bd, content[from:to]) {
		log.Fatalf("not equal!\n")
	}
}

func testUploadFile(t *testing.T, key string, server *StorageServer, content []byte, userToken string, fileToken string) {
	reply, err := server.UploadFileInit(context.Background(), &UploadInitArgs{
		FileKey:   key + "-uploading",
		FileSize:  int64(len(content)),
		UserToken: userToken,
		FileToken: fileToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.OK {
		t.Fatal(reply)
	}

	upload := server.UploadFileContent()
	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)

	var tmpWriter bytes.Buffer
	request := multipart.NewWriter(&tmpWriter)
	fileWriter, _ := request.CreateFormFile("content", "content")
	_, err = fileWriter.Write(content)
	if err != nil {
		t.Fatal(err)
	}
	_ = request.WriteField("user-token", userToken)
	_ = request.WriteField("file-token", fileToken)
	_ = request.WriteField("offset", "0")
	err = request.Close()
	if err != nil {
		t.Fatal(err)
	}

	ctx.Request = httptest.NewRequest("POST", "/", &tmpWriter)
	ctx.Request.Header.Set("Content-Type", request.FormDataContentType())

	upload(ctx)

	if w.Code != http.StatusOK {
		t.Fatal(w)
	}
	b, _ := ioutil.ReadAll(w.Body)
	print(string(b) + "\n")

	sha := sha512.New()
	sha.Write(content)
	hexsha := hex.EncodeToString(sha.Sum(nil))

	replyFinish, err := server.UploadFileFinish(context.Background(),
		&UploadFinishArgs{
			OldFileKey: key + "-uploading",
			UserToken:  userToken,
			FileToken:  fileToken,
			NewFileKey: key,
			FileSHA512: hexsha,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !replyFinish.OK {
		t.Fatal(replyFinish)
	}
}

func TestFileUpload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := createTestEnv(t)

	content := make([]byte, 200000000)

	size := int64(len(content))

	rand.Read(content)

	userToken := "user1"
	fileToken := "file1"
	key := "file-name"

	testUploadFile(t, key, server, content, userToken, fileToken)
	testReadFile(t, server, userToken, fileToken, key, 0, 205, content)
	testReadFile(t, server, userToken, fileToken, key, 205, size, content)
}

func TestConcurrentFileUpload(t *testing.T) {
	var wg sync.WaitGroup
	gin.SetMode(gin.TestMode)
	server := createTestEnv(t)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		key := strconv.Itoa(i) + "server-file-test"
		userToken := strconv.Itoa(i) + "server-file-test-user"
		fileToken := strconv.Itoa(i) + "server-file-test-file"
		go func() {
			content := make([]byte, 1000027)
			size := int64(len(content))
			rand.Read(content)

			testUploadFile(t, key, server, content, userToken, fileToken)
			testReadFile(t, server, userToken, fileToken, key, 0, 205, content)
			testReadFile(t, server, userToken, fileToken, key, 205, size, content)
			wg.Done()
		}()
	}
	wg.Wait()
}
