package blocks

import "encoding/binary"

func EncodeToEntry(p []byte, blockCapacity int64) (s Entry) {
	blockSize := calcBlockSize(p, blockCapacity)
	s = make([]byte, blockCapacity*blockSize)
	n := 0
	for i := int64(1); i <= blockSize; i++ {
		b := Block(s[blockCapacity*(i-1) : blockCapacity*i])
		n = b.Write(p, i, blockSize)
		p = p[n:]
	}
	return
}

type Entry []byte

func (entry Entry) Decode() (p []byte, err error) {
	sLen := len(entry)
	p = make([]byte, 0, len(entry))
	blockSize := entry.blocks()
	blockCapacity := int64(sLen) / blockSize
	blockIdx := int64(binary.LittleEndian.Uint16(entry[4:6]))
	for i := blockIdx; i <= blockSize; i++ {
		length := binary.LittleEndian.Uint32(entry[blockCapacity*(i-1) : blockCapacity*(i-1)+4])
		idx := int64(binary.LittleEndian.Uint16(entry[blockCapacity*(i-1)+4 : blockCapacity*(i-1)+6]))
		if idx != i {
			err = IncompleteSegmentErr
			return
		}
		p = append(p, entry[uint32(blockCapacity*(i-1)):uint32(blockCapacity*i)][uint32(blockCapacity)-length:]...)
	}
	return
}

func (entry Entry) blocks() (n int64) {
	n = int64(binary.LittleEndian.Uint16(entry[6:8]))
	return
}
