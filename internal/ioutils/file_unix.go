//go:build darwin || dragonfly || freebsd || linux || openbsd || solaris || netbsd

package ioutils

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
)

func (f *File) ReadRegion(offset int64, capacity int64) (region FileRegion, err error) {
	key := fmt.Sprintf("[%d:%d]", offset, capacity)
	v, doErr, _ := f.barrier.Do(key, func() (v interface{}, doErr error) {
		osPageSize := int64(os.Getpagesize())
		if offset%osPageSize == 0 {
			f.mutex.RLock()
			defer f.mutex.RUnlock()
			pageNo := int64(math.Floor(float64(offset) / float64(osPageSize)))
			pageOffset := pageNo * osPageSize
			pageCap := offset + capacity - pageOffset
			stats, statsErr := f.file.Stat()
			if statsErr != nil {
				doErr = fmt.Errorf("read region failed, can not get file status info, %v", statsErr)
				return
			}
			size := stats.Size()
			if pageOffset+pageCap > size {
				doErr = fmt.Errorf("file has no this region")
				return
			}
			if pageCap != int64(int(pageCap)) {
				doErr = fmt.Errorf("read region failed, capacity is too large")
				return
			}
			b, mapErr := syscall.Mmap(int(f.file.Fd()), pageOffset, int(pageCap), syscall.PROT_READ, syscall.MAP_PRIVATE)
			if mapErr != nil {
				doErr = mapErr
				return
			}
			v = &fileRegion{
				data:   b,
				size:   int64(len(b)),
				mapped: true,
			}
			runtime.SetFinalizer(v, (*fileRegion).Close)
		} else {
			p, readErr := f.ReadAt(offset, capacity)
			if readErr != nil {
				doErr = readErr
				return
			}
			v = &fileRegion{
				data:   p,
				size:   int64(len(p)),
				mapped: false,
			}
		}
		return
	})
	f.barrier.Forget(key)
	if doErr != nil {
		err = doErr
		return
	}
	region = v.(FileRegion)
	return
}

func (r *fileRegion) Close() error {
	if !r.mapped {
		return nil
	}
	runtime.SetFinalizer(r, nil)
	r.size = 0
	return syscall.Munmap(r.data)
}
