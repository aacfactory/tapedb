package blocks

import (
	"encoding/binary"
	"fmt"
)

func NewSegment(p []byte, blockCapacity int64) (s Segment) {
	blockSize := calcBlockSize(p, blockCapacity)
	s = make([]byte, blockCapacity*blockSize)
	n := 0
	for i := int64(1); i <= blockSize; i++ {
		b := Block(s[blockCapacity*(i-1) : blockCapacity*i])
		n = b.write(p, i, blockSize)
		p = p[n:]
	}
	return
}

type Segment []byte

func (s Segment) Content() (p []byte, err error) {
	sLen := len(s)
	p = make([]byte, 0, len(s))
	blockSize := s.blocks()
	blockCapacity := int64(sLen) / blockSize
	blockIdx := int64(binary.LittleEndian.Uint16(s[4:6]))
	for i := blockIdx; i <= blockSize; i++ {
		length := binary.LittleEndian.Uint32(s[blockCapacity*(i-1) : blockCapacity*(i-1)+4])
		idx := int64(binary.LittleEndian.Uint16(s[blockCapacity*(i-1)+4 : blockCapacity*(i-1)+6]))
		if idx != i {
			err = fmt.Errorf("incomplete")
			return
		}
		p = append(p, s[uint32(blockCapacity*(i-1)):uint32(blockCapacity*i)][uint32(blockCapacity)-length:]...)
	}
	return
}

func (s Segment) blocks() (n int64) {
	n = int64(binary.LittleEndian.Uint16(s[6:8]))
	return
}
