// +build linux darwin

package storage

// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// LICENSE: https://github.com/golang/exp/blob/master/LICENSE

import (
	"fmt"
	"golang.org/x/sys/unix"
	"log"
	"os"
	"runtime"
	"syscall"
)

type FileMMap struct {
	Data []byte
	Size int64
}

// Open memory-maps the named file for reading.
func NewFileMMap(filename string) (*FileMMap, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		return &FileMMap{}, nil
	}
	if size < 0 {
		return nil, fmt.Errorf("mmap: file %q has negative size", filename)
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("mmap: file %q is too large", filename)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	r := &FileMMap{Data: data, Size: size}
	if debug {
		var p *byte
		if len(data) != 0 {
			p = &data[0]
		}
		println("mmap", r, p)
	}
	runtime.SetFinalizer(r, (*FileMMap).Close)
	return r, nil
}

// debug is whether to print debugging messages for manual testing.
//
// The runtime.SetFinalizer documentation says that, "The finalizer for x is
// scheduled to run at some arbitrary time after x becomes unreachable. There
// is no guarantee that finalizers will run before a program exits", so we
// cannot automatically test that the finalizer runs. Instead, set this to true
// when running the manual test.
const debug = false

// Close closes the reader.
func (r *FileMMap) Close() error {
	if r.Data == nil {
		return nil
	}
	data := r.Data
	r.Data = nil
	if debug {
		var p *byte
		if len(data) != 0 {
			p = &data[0]
		}
		println("munmap", r, p)
	}
	runtime.SetFinalizer(r, nil)
	return unix.Munmap(data)
}

// Close closes the reader.
func (r *FileMMap) Sync(from int64, to int64) error {
	//a1, a2 := r.alignToPageSize(from, to)
	//err := unix.Msync(r.Data[a1:a2], unix.MS_SYNC)
	// just sync all.
	err := unix.Msync(r.Data[0:r.Size], unix.MS_SYNC)
	if err != nil {
		fmt.Print("sync error!\n")
	}
	return err
}

var osPageSize = int64(os.Getpagesize())

func (r *FileMMap) alignToPageSize(from int64, to int64) (int64, int64) {
	a1 := from / osPageSize * osPageSize
	a2 := to / osPageSize * osPageSize
	if to%osPageSize != 0 {
		a2 += osPageSize
	}
	if a1 > from || a2 < to || a1 > a2 {
		log.Fatal("wrong align")
	}
	return a1, a2
}
