package blist

import "encoding/binary"

const (
	listHead = 32
	itemSize = 16
	maxItems = 14
)

func NewList(no int64) List {
	v := List(make([]byte, listSize))
	v.setNo(no)
	return v
}

type List []byte

func (l List) No() (n int64) {
	n = int64(binary.BigEndian.Uint64(l[0:8]))
	return
}

func (l List) setNo(n int64) {
	binary.BigEndian.PutUint64(l[0:8], uint64(n))
	return
}

func (l List) Size() (n int64) {
	n = int64(binary.BigEndian.Uint64(l[8:16]))
	return
}

func (l List) setSize(n int64) {
	binary.BigEndian.PutUint64(l[8:16], uint64(n))
	return
}

func (l List) prev() (n int64) {
	n = int64(binary.BigEndian.Uint64(l[16:24]))
	return
}

func (l List) setPrev(prev int64) {
	binary.BigEndian.PutUint64(l[16:24], uint64(prev))
	return
}

func (l List) next() (n int64) {
	n = int64(binary.BigEndian.Uint64(l[24:32]))
	return
}

func (l List) setNext(next int64) {
	binary.BigEndian.PutUint64(l[24:32], uint64(next))
	return
}

func (l List) Items(offset int64) (items [][]byte) {
	n := l.Size()
	if n == 0 {
		return
	}
	items = make([][]byte, 0, n)
	for i := offset; i < n; i++ {
		items = append(items, l[i*itemSize+listHead:(i+int64(1))*itemSize+listHead])
	}
	return
}

func (l List) Add(p []byte) (ok bool) {
	n := l.Size()
	if n >= maxItems {
		return
	}
	copy(l[n*itemSize+listHead:(n+1)*itemSize+listHead], p)
	n++
	l.setSize(n)
	ok = true
	return
}

func (l List) Copy() (n List) {
	n = NewList(0)
	copy(n, l)
	return
}
