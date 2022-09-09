package blocks

import (
	"encoding/binary"
	"math"
)

func calcBlockSize(p []byte, blockCapacity int64) (size int64) {
	size = int64(math.Ceil(float64(len(p)) / float64(blockCapacity-8)))
	return
}

// [no][segNo][segSize][content_size][...content]
type Block []byte

func (b Block) write(p []byte, segmentIdx int64, segmentSize int64) (n int) {
	bLen := len(b) - 8
	pLen := len(p)
	if pLen-bLen < 0 {
		n = pLen
	} else {
		n = bLen
	}
	if segmentIdx == 0 {
		segmentIdx = 1
	}
	if segmentSize == 0 {
		segmentSize = 1
	}
	binary.LittleEndian.PutUint32(b[0:4], uint32(n))
	binary.LittleEndian.PutUint16(b[4:6], uint16(segmentIdx))
	binary.LittleEndian.PutUint16(b[6:8], uint16(segmentSize))
	copy(b[bLen+8-n:], p)
	return
}

func (b Block) read() (p []byte, segmentIdx uint16, segmentSize uint16, has bool) {
	length := binary.LittleEndian.Uint16(b[0:4])
	has = length > 0
	if !has {
		return
	}
	segmentIdx = binary.LittleEndian.Uint16(b[4:6])
	segmentSize = binary.LittleEndian.Uint16(b[6:8])
	p = b[uint16(len(b))-length:]
	return
}
