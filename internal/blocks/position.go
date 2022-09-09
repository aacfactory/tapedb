package blocks

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func ComparePosition(a Position, b Position) bool {
	return a.No() < b.No()
}

func NewPosition(bn int64, idx uint32, size uint32) (v Position) {
	v = make([]byte, 16)
	binary.BigEndian.PutUint64(v[0:8], uint64(bn))
	binary.LittleEndian.PutUint32(v[8:12], idx)
	binary.LittleEndian.PutUint32(v[12:16], size)
	return
}

type Position []byte

func (p Position) String() (v string) {
	v = fmt.Sprintf("%d:%d:%d", p.No(), p.Idx(), p.Size())
	return
}

func (p Position) No() (v int64) {
	v = int64(binary.BigEndian.Uint64(p[0:8]))
	return
}

func (p Position) Idx() (v uint32) {
	v = binary.LittleEndian.Uint32(p[8:12])
	return
}

func (p Position) Size() (v uint32) {
	v = binary.LittleEndian.Uint32(p[12:16])
	return
}

func ParsePosition(s string) (i Position, err error) {
	items := strings.Split(s, ":")
	if len(items) != 3 {
		err = fmt.Errorf("invalid index")
		return
	}
	bn, idx, size := int64(0), uint64(0), uint64(0)
	bn, err = strconv.ParseInt(items[0], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid index")
		return
	}
	idx, err = strconv.ParseUint(items[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid index")
		return
	}
	size, err = strconv.ParseUint(items[2], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid index")
		return
	}
	i = NewPosition(bn, uint32(idx), uint32(size))
	return
}
