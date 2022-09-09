package blocks

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/sync/singleflight"
	"io"
	"math"
	"os"
)

type PageBuffer interface {
	io.ReadCloser
	io.ReaderAt
	Bytes() (p []byte)
}

type bytesPageBuffer struct {
	size int64
	data []byte
}

func (b *bytesPageBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	if off > b.size {

		return
	}
	n = int(b.size - off)
	if n > cap(p) {
		n = cap(p)
	}
	copy(p, b.data[off:off+int64(n)])
	return
}

func (b *bytesPageBuffer) Read(p []byte) (n int, err error) {
	n = int(b.size)
	if n > cap(p) {
		n = cap(p)
	}
	copy(p, b.data[0:n])
	return
}

func (b *bytesPageBuffer) Bytes() (p []byte) {
	p = b.data[:]
	return
}

func (b *bytesPageBuffer) Close() error {
	b.size = 0
	b.data = nil
	return nil
}

type Page struct {
	volumeNo      int64
	beg           int64
	end           int64
	blockCapacity int64
	buffer        PageBuffer
}

func (p *Page) Key() string {
	return fmt.Sprintf("[%d][%d:%d]", p.volumeNo, p.beg, p.end)
}

func (p *Page) Segment(seq int64) (seg Segment, remainSeq int64) {
	b := p.buffer.Bytes()
	beg := seq - p.beg
	segmentIdx := binary.LittleEndian.Uint16(b[beg*p.blockCapacity+4 : beg*p.blockCapacity+6])
	segmentSize := binary.LittleEndian.Uint16(b[beg*p.blockCapacity+6 : beg*p.blockCapacity+8])
	end := seq + int64(segmentSize-segmentIdx)
	if end > p.end {
		remainSeq = p.end + 1
		end = p.end
	}
	end = end - p.beg
	seg = b[beg*p.blockCapacity : (end+1)*p.blockCapacity]
	return
}

type PageReader struct {
	volumeNo      int64
	volumeSEQ     *Sequence
	blockCapacity int64
	file          *os.File
	size          int64
	maxBlocks     int64
	barrier       *singleflight.Group
	cache         interface{}
}

func (pr *PageReader) GetPageRange(seq int64) (beg int64, end int64, has bool) {
	beg = (int64(math.Ceil(float64(seq)/float64(pr.maxBlocks))) - 1) * pr.maxBlocks
	end = beg + pr.maxBlocks
	offset := beg * pr.blockCapacity
	ln := int64(math.Ceil(float64(pr.volumeSEQ.Value()) / float64(pr.maxBlocks)))
	latestPageBegOffset := (ln*pr.maxBlocks - pr.maxBlocks) * pr.blockCapacity
	if offset < latestPageBegOffset {
		beg = 0
		end = 0
		return
	}
	has = true
	return
}

func (pr *PageReader) Read(beg int64, end int64) (p *Page, err error) {

	return
}
