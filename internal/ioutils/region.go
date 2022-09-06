package ioutils

import (
	"fmt"
	"io"
)

func ReadRegion(reader io.ReaderAt, offset int64, capacity int64) (p []byte, err error) {
	p = make([]byte, 0, capacity)
	nn := 0
	for {
		b := make([]byte, capacity)
		n, readErr := reader.ReadAt(b, offset)
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

func WriteRegion(writer io.WriterAt, offset int64, p []byte) (err error) {
	pLen := len(p)
	for {
		n, writeErr := writer.WriteAt(p, offset)
		if writeErr != nil {
			err = fmt.Errorf("write region [%d,%d] failed, %v", offset, offset+int64(cap(p)), writeErr)
			break
		}
		if n == pLen {
			break
		}
		p = p[n:]
		offset = offset + int64(n)
	}
	return
}
