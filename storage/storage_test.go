package storage

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadWriteBlockInfo(t *testing.T) {
	sg := &Storage{}
	info := &FileBlockInfo{
		Index:         0,
		Offset:        0,
		Size:          0,
		Crc32Checksum: 0,
	}
	buf := sg.writeBlockInfoToBuf(info)
	info2 := sg.loadBlockInfoFromBuf(buf)
	if !reflect.DeepEqual(info, info2) {
		t.Fail()
	}
	info = &FileBlockInfo{
		Index:         123456,
		Offset:        65535,
		Size:          87100248,
		Crc32Checksum: 1054883121,
	}
	buf = sg.writeBlockInfoToBuf(info)
	info2 = sg.loadBlockInfoFromBuf(buf)
	if !reflect.DeepEqual(info, info2) {
		t.Fail()
	}
	info = &FileBlockInfo{
		Index:         123456,
		Offset:        655,
		Size:          87100248,
		Crc32Checksum: 1054883121,
	}
	buf = sg.writeBlockInfoToBuf(info)
	info2 = sg.loadBlockInfoFromBuf(buf)
	if !reflect.DeepEqual(info, info2) {
		t.Fail()
	}
	info = &FileBlockInfo{
		Index:         math.MaxUint32,
		Offset:        65535,
		Size:          math.MaxUint32,
		Crc32Checksum: math.MaxUint32,
	}
	buf = sg.writeBlockInfoToBuf(info)
	info2 = sg.loadBlockInfoFromBuf(buf)
	if !reflect.DeepEqual(info, info2) {
		t.Fail()
	}
}

func createTempStorage() *Storage {
	folder := "/home/linux/Desktop/store-test"
	_ = os.RemoveAll(folder)
	_ = os.Mkdir(folder, os.FileMode(0777))
	//fmt.Printf("%v\n", err)
	_ = os.Mkdir(folder+"/data-blocks", os.FileMode(0777))
	_ = os.Mkdir(folder+"/file-metadata-blocks", os.FileMode(0777))
	sg, _ := NewStorage(folder, BLOCKSIZE, ALLOCSIZE)
	return sg
}

type testfile struct {
	content []byte
	blocks  []*FileBlockInfo
	key     string
	fmd     *FileMetadata
}

func allocTest(testfiles []*testfile, verbose bool, t *testing.T) {
	sg := createTempStorage()
	t0 := time.Now()

	for _, v := range testfiles {
		blocks, err := sg.createFile(int64(len(v.content)), v.content, true, sg.blockFileStats)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
		v.blocks = blocks
	}

	d1 := time.Since(t0)

	if len(sg.openedFiles) != 0 {
		fmt.Printf("file not closed\n")
		t.FailNow()
	}

	err := sg.clearCachedBlocks()
	if err != nil {
		fmt.Printf("error: %v\n", err)
		t.FailNow()
	}

	fmt.Printf("%v files %v seconds\n", len(testfiles), d1.Seconds())

	for i, v := range testfiles {
		if verbose {
			fmt.Printf("-------------File %v-------------\n", i)

		}
		checkEqual(v.content, t, v.blocks, verbose, sg)
	}
}

const BLOCKSIZE = 64 * 1024 * 1024
const ALLOCSIZE = 1 * 1024

func TestAllocSmall(t *testing.T) {
	lens := []int{1, 2, 1023, 1024, 1025, 4095, 4096, 4097, 10000, 8192, 100 * 1024,
		100*1024 + 1, 100*1024 - 1, 100*1024 + 31, 100*1024 - 31}
	content := make([]byte, 200*1024)
	rand.Read(content)
	for i := 0; i < len(lens); i++ {
		length := lens[i]
		fmt.Printf("-------------Length %v-------------\n", length)
		testfiles := make([]*testfile, 1000)
		for i, _ := range testfiles {
			testfiles[i] = &testfile{
				content: content[0:length],
				blocks:  nil,
			}
		}
		allocTest(testfiles, false, t)
	}
	testfiles := make([]*testfile, 10051)
	fmt.Printf("-------------Length mixed-------------\n")

	totalsize := 0

	for i, _ := range testfiles {
		testfiles[i] = &testfile{
			content: content[0:lens[rand.Intn(len(lens))]],
			blocks:  nil,
		}
		totalsize += len(testfiles[i].content)
	}
	allocTest(testfiles, false, t)

	fmt.Printf("totalsize: %v\n", totalsize)
}

func TestAllocLarge(t *testing.T) {
	lens := []int{1 * 1024 * 1024, 1*1024*1024 - 31, 1*1024*1024 + 31, 5 * 1024 * 1024, 21 * 1024 * 1024}
	content := make([]byte, 21*1024*1024)
	rand.Read(content)
	for i := 0; i < len(lens); i++ {
		length := lens[i]
		fmt.Printf("-------------Length %v-------------\n", length)
		testfiles := make([]*testfile, 50)
		for i, _ := range testfiles {
			testfiles[i] = &testfile{
				content: content[0:length],
				blocks:  nil,
			}
		}
		allocTest(testfiles, false, t)
	}
}

func TestAllocSuper(t *testing.T) {
	lens := []int{64 * 1024 * 1024, 500 * 1024 * 1024, 500*1024*1024 + 31, 517 * 1024 * 1024, 500*1024*1024 - 23}
	content := make([]byte, 520*1024*1024)
	rand.Read(content)
	for i := 0; i < len(lens); i++ {
		length := lens[i]
		fmt.Printf("-------------Length %v-------------\n", length)
		testfiles := make([]*testfile, 1)
		for i, _ := range testfiles {
			testfiles[i] = &testfile{
				content: content[0:length],
				blocks:  nil,
			}
		}
		allocTest(testfiles, true, t)
	}
}

func checkEqual(content []byte, t *testing.T, blocks []*FileBlockInfo, verbose bool, sg *Storage) {
	contentBegin := 0
	for i, block := range blocks {
		if verbose {
			fmt.Printf("Block %v: %v\n", i, block)
		}
		file, err := sg.openBlock(block.relativeFilePath, true)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
		sg.closeBlock(file)
		begin := block.Offset * sg.blockFileStats.allocationUnitSize
		end := int(begin) + int(block.Size)

		readBytes := file.memoryMap.Data[begin:end]
		contentBytes := content[contentBegin : contentBegin+int(block.Size)]
		if !bytes.Equal(contentBytes, readBytes) {
			fmt.Printf("byte not equal\n")
			t.FailNow()
		}
		contentBegin += int(block.Size)
	}
	if contentBegin != len(content) {
		fmt.Printf("file incomplete\n")
		t.FailNow()
	}
	err := sg.clearCachedBlocks()
	if err != nil {
		fmt.Printf("error: %v\n", err)
		t.FailNow()
	}

	// not sure, directly open file
	contentBegin = 0
	for i, block := range blocks {
		if verbose {
			fmt.Printf("Block %v: %v\n", i, block)
		}
		file, err := os.Open(sg.folder + block.relativeFilePath)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
		begin := int(block.Offset * sg.blockFileStats.allocationUnitSize)
		end := int(begin) + int(block.Size)
		_, err = file.Seek(int64(begin), 0)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}

		readBytes := make([]byte, end-begin)
		_, err = file.Read(readBytes)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}

		contentBytes := content[contentBegin : contentBegin+int(block.Size)]
		if !bytes.Equal(contentBytes, readBytes) {
			fmt.Printf("byte not equal\n")
			t.FailNow()
		}
		contentBegin += int(block.Size)
		_ = file.Close()
	}
	if contentBegin != len(content) {
		fmt.Printf("file incomplete\n")
		t.FailNow()
	}
}

func TestCreateFileSmall(t *testing.T) {
	sg := createTempStorage()

	lens := []int{1, 2, 1023, 1024, 1025, 4095, 4096, 4097, 10000, 8192, 100 * 1024,
		100*1024 + 1, 100*1024 - 1, 100*1024 + 31, 100*1024 - 31, 1 * 1024 * 1024, 1*1024*1024 - 31,
		1*1024*1024 + 31, 5 * 1024 * 1024, 21 * 1024 * 1024,
		64 * 1024 * 1024, 64*1024*1024 - 20154, 64*1024*1024 + 24812}
	getcnt := func(length int) int {
		if length < 200*1024 {
			return 50
		} else if length < 1*1024*1024 {
			return 5
		} else {
			return 2
		}
	}
	content := make([]byte, 128*1024*1024)
	rand.Read(content)

	testfiles := make([]*testfile, 0)

	keyI := 0

	for i := 0; i < len(lens); i++ {
		length := lens[i]
		for i := 0; i < getcnt(length); i++ {
			keyI++
			testfiles = append(testfiles, &testfile{
				content: content[0:length],
				blocks:  nil,
				key:     "key[]-" + strconv.Itoa(keyI),
			})
		}
	}

	for i := 0; i < 20; i++ {
		keyI++
		testfiles = append(testfiles, &testfile{
			content: content[0:lens[rand.Intn(len(lens))]],
			blocks:  nil,
			key:     "key[]-" + strconv.Itoa(keyI),
		})
	}

	for i := 0; i < len(testfiles); i++ {
		fmt.Println(i)
		testf := testfiles[i]
		fmd, err := sg.CreateSmallFile(testf.key, int64(len(testf.content)), testf.content)

		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}

		fmdFound := sg.fileEntries.key2file[testf.key]
		if fmdFound != fmd {
			fmt.Printf("fmd not equal\n")
			t.FailNow()
		}

		if int(fmd.FileSize) != len(testf.content) {
			fmt.Printf("size not equal\n")
			t.FailNow()
		}

		checkEqual(testf.content, t, fmd.Blocks, false, sg)
	}
}

func TestCreateSmallFileConcurrent(t *testing.T) {
	lens := []int{1, 2, 1023, 1024, 1025, 4095, 4096, 4097, 10000, 8192, 100 * 1024,
		100*1024 + 1, 100*1024 - 1, 100*1024 + 31, 100*1024 - 31}
	content := make([]byte, 200*1024)
	rand.Read(content)

	sg := createTempStorage()

	testfiles := make([]*testfile, 0)

	keyI := 0

	for i := 0; i < len(lens); i++ {
		length := lens[i]
		for i := 0; i < 200; i++ {
			keyI++
			testfiles = append(testfiles, &testfile{
				content: content[0:length],
				blocks:  nil,
				key:     "key[]-" + strconv.Itoa(keyI),
			})
		}
	}

	for i := 0; i < 2000; i++ {
		keyI++
		testfiles = append(testfiles, &testfile{
			content: content[0:lens[rand.Intn(len(lens))]],
			blocks:  nil,
			key:     "key[]-" + strconv.Itoa(keyI),
		})
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < len(testfiles); i++ {
		testf := testfiles[i]
		wg.Add(1)
		go func() {
			var err error
			testf.fmd, err = sg.CreateSmallFile(testf.key, int64(len(testf.content)), testf.content)

			if err != nil {
				fmt.Printf("error: %v\n", err)
				t.FailNow()
			}
			wg.Done()
		}()

	}
	wg.Wait()
	sg.clearCachedBlocks()
	for i := 0; i < len(testfiles); i++ {
		testf := testfiles[i]

		fmdFound := sg.fileEntries.key2file[testf.key]
		if fmdFound != testf.fmd {
			fmt.Printf("fmd not equal\n")
			t.FailNow()
		}

		if int(testf.fmd.FileSize) != len(testf.content) {
			fmt.Printf("size not equal\n")
			t.FailNow()
		}

		checkEqual(testf.content, t, testf.fmd.Blocks, false, sg)
	}

	if int(len(sg.fileEntries.key2file)) != len(testfiles) {
		fmt.Printf("file count wrong")
		t.FailNow()
	}

}

func TestCreateLargerFileConcurrent(t *testing.T) {
	lens := []int{1, 2, 1023, 1024, 1025, 4095, 4096, 4097, 10000, 8192, 100 * 1024,
		100*1024 + 1, 100*1024 - 1, 100*1024 + 31, 100*1024 - 31, 1 * 1024 * 1024, 1*1024*1024 - 31,
		1*1024*1024 + 31, 5 * 1024 * 1024, 21 * 1024 * 1024,
		64 * 1024 * 1024, 64*1024*1024 - 20154, 64*1024*1024 + 24812}
	content := make([]byte, 66*1024*1024+24812)
	rand.Read(content)

	sg := createTempStorage()
	getcnt := func(length int) int {
		if length < 200*1024 {
			return 50
		} else if length < 1*1024*1024 {
			return 5
		} else {
			return 2
		}
	}
	testfiles := make([]*testfile, 0)

	keyI := 0

	for i := 0; i < len(lens); i++ {
		length := lens[i]
		for i := 0; i < getcnt(length); i++ {
			keyI++
			testfiles = append(testfiles, &testfile{
				content: content[0:length],
				blocks:  nil,
				key:     "key[]-" + strconv.Itoa(keyI),
			})
		}
	}

	for i := 0; i < 10; i++ {
		keyI++
		testfiles = append(testfiles, &testfile{
			content: content[0:lens[rand.Intn(len(lens))]],
			blocks:  nil,
			key:     "key[]-" + strconv.Itoa(keyI),
		})
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < len(testfiles); i++ {
		testf := testfiles[i]
		wg.Add(1)
		go func() {
			var err error
			testf.fmd, err = sg.CreateSmallFile(testf.key, int64(len(testf.content)), testf.content)

			if err != nil {
				fmt.Printf("error: %v\n", err)
				t.FailNow()
			}
			wg.Done()
		}()

	}
	wg.Wait()
	sg.clearCachedBlocks()
	for i := 0; i < len(testfiles); i++ {
		testf := testfiles[i]

		fmdFound := sg.fileEntries.key2file[testf.key]
		if fmdFound != testf.fmd {
			fmt.Printf("fmd not equal\n")
			t.FailNow()
		}

		if int(testf.fmd.FileSize) != len(testf.content) {
			fmt.Printf("size not equal\n")
			t.FailNow()
		}

		checkEqual(testf.content, t, testf.fmd.Blocks, false, sg)
	}

	if int(len(sg.fileEntries.key2file)) != len(testfiles) {
		fmt.Printf("file count wrong")
		t.FailNow()
	}

}

func TestDeleteBlocks(t *testing.T) {
	lens := []int{1, 2, 1023, 1024, 1025, 4095, 4096, 4097, 10000, 8192, 100 * 1024,
		100*1024 + 1, 100*1024 - 1, 100*1024 + 31, 100*1024 - 31, 1 * 1024 * 1024, 1*1024*1024 - 31,
		1*1024*1024 + 31, 5 * 1024 * 1024, 21 * 1024 * 1024,
		64 * 1024 * 1024, 64*1024*1024 - 20154, 64*1024*1024 + 24812}
	content := make([]byte, 66*1024*1024+24812)
	rand.Read(content)

	sg := createTempStorage()
	mu := &sync.Mutex{}
	key2filesize := make(map[string]int)
	k := int64(0)
	wg := &sync.WaitGroup{}
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			l := lens[rand.Intn(len(lens))]
			key := strconv.Itoa(int(atomic.AddInt64(&k, 1)))
			_, err := sg.CreateSmallFile(key,
				int64(l), content[len(content)-l:])

			if err != nil {
				fmt.Printf("error: %v\n", err)
				t.FailNow()
			}
			mu.Lock()
			key2filesize[key] = l
			mu.Unlock()
			wg.Done()
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for {
				key := atomic.LoadInt64(&k)
				key2 := rand.Intn(int(key + 1))
				mu.Lock()
				_, found := key2filesize[strconv.Itoa(key2)]
				if found {
					delete(key2filesize, strconv.Itoa(key2))
				}
				mu.Unlock()
				if !found {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				err := sg.DeleteFile(strconv.Itoa(key2))

				if err != nil {
					t.Fatalf("error: %v\n", err)
				}
				wg.Done()
				break
			}

		}()
	}

	wg.Wait()
	sg.clearCachedBlocks()

	// Now we should have exactly 100 files
	if len(key2filesize) != 100 {
		t.Fatalf("test wrong!")
	}

	for key, filesize := range key2filesize {
		re, err := sg.GetSmallFile(key)
		if err != nil {
			t.Fatalf("cannot get small file")
		}
		if !bytes.Equal(re, content[len(content)-filesize:]) {
			t.Fatalf("content wrong!")
		}
	}

}
