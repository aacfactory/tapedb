package index

import "encoding/binary"

func encodeListNo(no int64) (p []byte) {
	p = make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(no))
	return
}

func decodeListNo(p []byte) (no int64) {
	no = int64(binary.BigEndian.Uint64(p[0:8]))
	return
}
