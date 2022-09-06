package ioutils_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"math"
	"testing"
)

func TestToBytes(t *testing.T) {
	bloom := 8 + 8                                           // hash + bn
	fmt.Println(ioutils.ByteSize(uint64(bloom * 100000000))) // 1E = 1.5G, 0.1E = 152.6M
	list := (8 + 8 + 64 + 64) * 4                            // (vn + bn + key_cap + id_cap) + 4 levels
	fmt.Println(ioutils.ByteSize(uint64(list * 100000000)))  // 1E = 53.6G, 0.1E = 5.4G
	fmt.Println(ioutils.ByteSize(uint64(math.MaxInt64)))
	fmt.Println(ioutils.ByteSize(uint64(64 * 128 * 2)))
	fmt.Println(ioutils.ByteSize(uint64(1024*10000000)), (1024-48-8-8)/16)
	x := make([]byte, 8)
	copy(x, "0123")
	fmt.Println(string(x), len(x[:]), len(string(x)))
	n := bytes.IndexByte(x, 0)
	fmt.Println(len(x[0:n]))
	fmt.Println(bytes.Compare([]byte("b"), []byte("a")))
	fmt.Println(bytes.Compare([]byte("a"), []byte("a")))
	fmt.Println(bytes.Compare([]byte("a"), []byte("b")))
	// tree
	// 1024 nodes = 15.9M
	// 2048 nodes = 31.9M
	// 4096 nodes(1044480 items) = 63.8M
	treeItem := 64
	treeNode := 255
	treeTotalItems := treeNode * 4096
	fmt.Println(treeTotalItems, ioutils.ByteSize(uint64(treeItem*treeTotalItems))) // 1E = 53.6G, 0.1E = 5.4G\
	fmt.Println(ioutils.ByteSize(uint64(256*4096*16)), float64(256*4096*128)/float64(ioutils.MEGABYTE))
}

func TestHalf(t *testing.T) {
	n := 256
	mid := n / 2
	left := n - mid - 1
	right := n - left - 1
	fmt.Println(mid, left, right, left+right, left+right+1)
}

func TestClear(t *testing.T) {
	p := []byte("0.123456")
	pp := map[int][]byte{
		1: p,
	}
	ioutils.Clear(p)
	fmt.Println(bytes.Runes(p))
	//fmt.Println(p[:1])
	fmt.Println(pp[1])
}

func TestNo(t *testing.T) {
	fmt.Println(0/14, 0%14)
	fmt.Println(1/14, 1%14)
	fmt.Println(14/14, 14%14)
	fmt.Println(15/14, 15%14)
	fmt.Println(28/14, 28%14)
	fmt.Println(30/14, 30%14)

}
