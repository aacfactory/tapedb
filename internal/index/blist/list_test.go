package blist_test

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/index/blist"
	"testing"
)

func pos(i int64) (p []byte) {
	p = make([]byte, 16)
	binary.BigEndian.PutUint64(p, uint64(i))
	return
}

func TestNewList(t *testing.T) {
	list := blist.NewList(1)
	list.Add(pos(1))
	list.Add(pos(2))
	fmt.Println(list.No(), list.Size())
}

func TestList_Items(t *testing.T) {
	list := blist.NewList(1)
	list.Add(pos(1))
	list.Add(pos(2))
	fmt.Println(len(list.Items(0)))
	fmt.Println(len(list.Items(1)))
	fmt.Println(len(list.Items(2)))
}

func TestList_Add(t *testing.T) {
	list := blist.NewList(1)
	for i := 0; i < 20; i++ {
		fmt.Println(i, list.Add(pos(int64(i))))
	}
}
