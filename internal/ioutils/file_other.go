//go:build !darwin && !dragonfly && !freebsd && !linux && !openbsd && !solaris && !netbsd

package ioutils

import (
	"fmt"
	"io"
)

func (f *File) ReadRegion(offset int64, capacity int64) (region FileRegion, err error) {
	key := fmt.Sprintf("[%d:%d]", offset, capacity)
	v, doErr, _ := f.barrier.Do(key, func() (v interface{}, doErr error) {
		f.mutex.RLock()
		defer f.mutex.RUnlock()
		stats, statsErr := f.file.Stat()
		if statsErr != nil {
			doErr = fmt.Errorf("read region failed, can not get file status info, %v", statsErr)
			return
		}
		size := stats.Size()
		if offset+capacity > size {
			doErr = fmt.Errorf("file has no this region")
			return
		}
		if capacity != int64(int(capacity)) {
			doErr = fmt.Errorf("read region failed, capacity is too large")
			return
		}
		p := make([]byte, 0, capacity)
		nn := int64(0)
		for {
			b := make([]byte, capacity)
			n, readErr := f.file.ReadAt(b, offset)
			if n == 0 {
				break
			}
			p = append(p, b[0:n]...)
			nn = nn + int64(n)
			if nn == capacity {
				break
			}
			if nn > capacity {
				p = p[0:capacity]
				break
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				err = fmt.Errorf("read at %d failed, %v", offset, readErr)
				break
			}
			offset = offset + int64(n)
		}
		v = &fileRegion{
			data: p,
			size: int64(len(p)),
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
	r.size = 0
	r.data = nil
	return nil
}
