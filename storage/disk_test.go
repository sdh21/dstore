package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Sequential Write

func TestSequentialWrite(t *testing.T) {
	file, _ := os.Create("/tmp/sequential-write-test.tttt")
	length := 1024
	fmt.Printf("-------------Length %v-------------\n", length)

	content := make([]byte, length)
	rand.Read(content)

	t0 := time.Now()

	cnt := 10000

	for i := 0; i < cnt; i++ {
		_, err := file.Write(content)

		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
		err = file.Sync()
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
	}

	d1 := time.Since(t0)

	fmt.Printf("%v files %v seconds\n", cnt, d1.Seconds())

}

func TestMmapWrite(t *testing.T) {
	length := 1024
	fmt.Printf("-------------Length %v-------------\n", length)

	content := make([]byte, length)
	rand.Read(content)

	sg := createTempStorage()

	t0 := time.Now()

	cnt := 10000
	for i := 0; i < cnt; i++ {
		_, err := sg.createFile(int64(length), content, true, sg.blockFileStats)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			t.FailNow()
		}
	}

	d1 := time.Since(t0)

	fmt.Printf("%v files %v seconds\n", cnt, d1.Seconds())

}
