package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	entrySize      = 64
	entriesHeadLen = 64
)

func NewEntry(k []byte, v []byte) (e Entry) {
	e = make([]byte, 64)
	kLen := len(k)
	kl := make([]byte, 8)
	binary.BigEndian.PutUint64(kl, uint64(kLen))
	copy(e[0:8], kl)
	copy(e[8:kLen+8], k)
	copy(e[56:64], v)
	return
}

type Entry []byte

func (e Entry) Key() []byte {
	return e[8 : 8+binary.BigEndian.Uint64(e[0:8])]
}

func (e Entry) Value() []byte {
	return e[56:64]
}

func NewEntries(size int) (v Entries) {
	v = make([]byte, entriesHeadLen+size*entrySize)
	binary.BigEndian.PutUint64(v[0:8], 0)
	return
}

type Entries []byte

func (entries Entries) size() int {
	return int(binary.BigEndian.Uint64(entries[0:8]))
}

func (entries Entries) setSize(n int) {
	binary.BigEndian.PutUint64(entries[0:8], uint64(n))
	return
}

func (entries Entries) mustGetEntry(idx int) (e Entry) {
	has := false
	e, has = entries.getEntry(idx)
	if !has {
		panic(fmt.Errorf("btree get %d entry failed, not found", idx))
	}
	return
}

func (entries Entries) getEntry(idx int) (e Entry, has bool) {
	if entries.size() < idx {
		return
	}
	e = Entry(entries[idx*entrySize+entriesHeadLen : (idx+1)*entrySize+entriesHeadLen])
	has = true
	return
}

func (entries Entries) setEntry(idx int, e Entry) {
	copy(entries[(idx+1)*entrySize+entriesHeadLen:], entries[(idx)*entrySize+entriesHeadLen:])
	copy(entries[(idx)*entrySize+entriesHeadLen:(idx+1)*entrySize+entriesHeadLen], e)
	entries.setSize(entries.size() + 1)
	return
}

func (entries Entries) replaceEntry(idx int, e Entry) {
	copy(entries[idx*entrySize+entriesHeadLen:(idx+1)*entrySize+entriesHeadLen], e)
	return
}

func (entries Entries) Split() (left Entries, median Entry, right Entries) {
	mid := entries.size() / 2
	leftSize := entries.size() - mid - 1
	rightSize := entries.size() - leftSize - 1
	median, _ = entries.getEntry(leftSize)
	left = NewEntries(maxItems)
	left.setSize(leftSize)
	copy(left[entriesHeadLen:], entries[entriesHeadLen:(leftSize)*entrySize+entriesHeadLen])
	right = NewEntries(maxItems)
	right.setSize(rightSize)
	copy(right[entriesHeadLen:], entries[(leftSize+1)*entrySize+entriesHeadLen:])
	return
}

func (entries Entries) find(key []byte) (idx int, found bool) {
	low := 0
	high := entries.size()
	for low < high {
		mid := (low + high) / 2
		if bytes.Compare(key, Entry(entries[mid*entrySize+entriesHeadLen:(mid+1)*entrySize+entriesHeadLen]).Key()) >= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low > 0 && bytes.Compare(Entry(entries[(low-1)*entrySize+entriesHeadLen:low*entrySize+entriesHeadLen]).Key(), key) >= 0 {
		idx = low - 1
		found = true
		return
	}
	idx = low
	return
}
