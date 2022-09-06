package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func intBytes(i int) (v []byte) {
	v = make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(i))
	return
}

func bytesInt(p []byte) (i int) {
	return int(binary.BigEndian.Uint64(p))
}

func TestNewEntries(t *testing.T) {
	entries := NewEntries(10)
	fmt.Println(entries.size(), len(entries), cap(entries))
	entries.setEntry(0, NewEntry(intBytes(0), intBytes(0)))
	entries.setEntry(1, NewEntry(intBytes(1), intBytes(1)))
	entries.setEntry(2, NewEntry(intBytes(2), intBytes(2)))
	entries.setEntry(3, NewEntry(intBytes(3), intBytes(3)))
	entries.setEntry(4, NewEntry(intBytes(4), intBytes(4)))
	entries.setEntry(5, NewEntry(intBytes(5), intBytes(5)))
	fmt.Println(entries.size())

	l, m, r := entries.Split()
	ls := ""
	for i := 0; i < l.size(); i++ {
		ls = ls + ", " + fmt.Sprintf("%d", bytesInt(l.mustGetEntry(i).Value()))
	}
	fmt.Println("l:", ls[2:])
	fmt.Println("m:", bytesInt(m.Value()))
	rs := ""
	for i := 0; i < r.size(); i++ {
		rs = rs + ", " + fmt.Sprintf("%d", bytesInt(r.mustGetEntry(i).Value()))
	}
	fmt.Println("r:", rs[2:])
	fmt.Println(l.size(), r.size())
}
