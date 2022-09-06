package blocks

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func NewPosition(blockNo int64, num int64) (v Position) {
	v = make([]byte, 16)
	binary.BigEndian.PutUint64(v[0:8], uint64(blockNo))
	binary.LittleEndian.PutUint64(v[8:16], uint64(num))
	return
}

type Position []byte

func (p Position) String() (v string) {
	v = fmt.Sprintf("%d:%d", p.BlockNo(), p.BlockNum())
	return
}

func (p Position) BlockNo() (v int64) {
	v = int64(binary.BigEndian.Uint64(p[0:8]))
	return
}

func (p Position) BlockNum() (v int64) {
	v = int64(binary.LittleEndian.Uint64(p[8:16]))
	return
}

func (p Position) LT(a Position) (ok bool) {
	ok = p.BlockNo() < a.BlockNo()
	return
}

func ParsePosition(s string) (i Position, err error) {
	items := strings.Split(s, ":")
	if len(items) != 2 {
		err = fmt.Errorf("invalid index")
		return
	}
	blockNo, num := int64(0), int64(0)
	blockNo, err = strconv.ParseInt(items[0], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid index")
		return
	}
	num, err = strconv.ParseInt(items[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid index")
		return
	}
	i = NewPosition(blockNo, num)
	return
}
