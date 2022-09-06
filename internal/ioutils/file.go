package ioutils

import (
	"fmt"
	"golang.org/x/sync/singleflight"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func SyncDir(dir string) (err error) {
	df, openErr := os.Open(dir)
	if openErr != nil {
		err = fmt.Errorf("sync %s failed, %v", dir, openErr)
		return
	}
	if syncErr := df.Sync(); syncErr != nil {
		err = fmt.Errorf("sync %s failed, %v", dir, syncErr)
		return
	}
	if closeErr := df.Close(); closeErr != nil {
		err = fmt.Errorf("sync %s failed, %v", dir, closeErr)
		return
	}
	return
}

func ExistFile(filePath string) (ok bool) {
	_, err := os.Stat(filePath)
	if err == nil {
		ok = true
		return
	}
	if os.IsNotExist(err) {
		return
	}
	ok = true
	return
}

type FileRegion interface {
	io.ReaderAt
	io.ReadCloser
	Bytes() (p []byte)
}

type fileRegion struct {
	data   []byte
	size   int64
	mapped bool
}

func (r *fileRegion) ReadAt(p []byte, off int64) (n int, err error) {
	if off > r.size {

		return
	}
	n = int(r.size - off)
	if n > cap(p) {
		n = cap(p)
	}
	copy(p, r.data[off:off+int64(n)])
	return
}

func (r *fileRegion) Read(p []byte) (n int, err error) {
	n = int(r.size)
	if n > cap(p) {
		n = cap(p)
	}
	copy(p, r.data[0:n])
	return
}

func (r *fileRegion) Bytes() (p []byte) {
	p = r.data[:]
	return
}

func OpenFile(filePath string) (file *File, err error) {
	if filePath == "" {
		err = fmt.Errorf("can not open empty path file")
		return
	}
	filePath, err = filepath.Abs(filePath)
	if err != nil {
		return
	}
	if !ExistFile(filePath) {
		dir, _ := filepath.Split(filePath)
		if !ExistFile(dir) {
			err = os.MkdirAll(dir, 0600)
			if err != nil {
				return
			}
		}
	}
	f, openErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0600)
	if openErr != nil {
		err = openErr
		return
	}
	file = &File{
		file:    f,
		mutex:   sync.RWMutex{},
		barrier: singleflight.Group{},
	}
	return
}

type File struct {
	file    *os.File
	mutex   sync.RWMutex
	barrier singleflight.Group
}

func (f *File) File() (file *os.File) {
	file = f.file
	return
}

func (f *File) WriteAt(offset int64, p []byte) (err error) {
	f.mutex.Lock()
	err = WriteRegion(f.file, offset, p)
	f.mutex.Unlock()
	return
}

func (f *File) Sync() (err error) {
	f.mutex.Lock()
	err = f.file.Sync()
	f.mutex.Unlock()
	return
}

func (f *File) Close() (err error) {
	f.mutex.Lock()
	err = f.file.Close()
	f.mutex.Unlock()
	return
}

func (f *File) ReadAt(offset int64, capacity int64) (p []byte, err error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	p = make([]byte, 0, capacity)
	nn := 0
	for {
		b := make([]byte, capacity)
		n, readErr := f.file.ReadAt(b, offset)
		if n == 0 {
			break
		}
		p = append(p, b[0:n]...)
		nn = nn + n
		if int64(nn) == capacity {
			break
		}
		if int64(nn) > capacity {
			p = p[0:capacity]
			break
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			err = fmt.Errorf("read region [%d,%d] failed, %v", offset, offset+capacity, readErr)
			break
		}
		offset = offset + int64(n)
	}
	return
}
