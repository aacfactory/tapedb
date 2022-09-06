package btree_test

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/index/btree"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"github.com/cespare/xxhash/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHash(t *testing.T) {
	p := []byte(time.Now().Format(time.RFC3339))
	x := xxhash.Sum64(p)
	for i := 0; i < 1000000000000; i++ {
		y := xxhash.Sum64(p)
		if x != y {
			t.Error(i, x, y)
			break
		}
	}
}

func intBytes(i int64) (v []byte) {
	v = make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(i))
	return
}

func bytesInt(p []byte) (i int) {
	if len(p) == 0 {
		return -1
	}
	return int(binary.BigEndian.Uint64(p))
}

func TestBTree_Get(t *testing.T) {
	tr, err := btree.New(btree.Options{
		Path:          `G:\tmp\bt.txt`,
		MaxCacheNodes: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	fmt.Println(tr.Set(intBytes(0), intBytes(0)))
	fmt.Println(tr.Set(intBytes(0), intBytes(0)))
	fmt.Println(tr.Set(intBytes(1), intBytes(1)))
	fmt.Println(tr.Set(intBytes(2), intBytes(2)))
	fmt.Println(tr.Set(intBytes(4), intBytes(4)))
	//tr.Set(intBytes(5), intBytes(5))
	for i := 0; i < 50; i++ {
		tr.Set(intBytes(int64(i+6)), intBytes(int64(i+6)))
	}
	v := make([]byte, 0, 1)
	has := false
	for i := 0; i < 20; i++ {
		v, has, err = tr.Get(intBytes(int64(i)))
		fmt.Println(i, has, bytesInt(v), err)
	}
}

func TestNodeSize(t *testing.T) {
	const (
		degree         = 256
		maxItems       = degree*2 - 1 // max items per node. max children is +1
		entrySize      = 64
		entriesHeadLen = 64
		nodeSize       = 8*(maxItems+1) + entrySize*(maxItems) + entriesHeadLen
	)
	// 4k
	fmt.Println(8*(maxItems+1), ioutils.ByteSize(uint64(8*(maxItems+1))))
	fmt.Println(float64(nodeSize)/float64(1024), ioutils.ByteSize(uint64(nodeSize)))
	n := 4096
	fmt.Println(ioutils.ByteSize(uint64((entrySize*(maxItems)+entriesHeadLen)*n)), n*maxItems)

}

func BenchmarkBTree_Set(b *testing.B) {
	tr, err := btree.New(btree.Options{
		Path:          `G:\tmp\bt1.txt`,
		MaxCacheNodes: 12,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tr.Close()
	n := int64(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&n, 1)
			tr.Set(intBytes(i), intBytes(i))
		}
	})
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkBTree_Get(b *testing.B) {
	tr, err := btree.New(btree.Options{
		Path:          `G:\tmp\bt1.txt`,
		MaxCacheNodes: 12,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tr.Close()

	n := int64(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&n, 1)
			tr.Get(intBytes(i))
		}
	})
	b.StopTimer()
	b.ReportAllocs()
}

func TestBTree_Set(t *testing.T) {
	tr, err := btree.New(btree.Options{
		Path:          `P:\tmp\bt1.txt`,
		MaxCacheNodes: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	now := time.Now()
	wg := new(sync.WaitGroup)
	loops := 1000000
	sot := int64(0)
	got := int64(0)
	gotErrs := int64(0)
	for i := 0; i < loops; i++ {
		wg.Add(1)
		go func(bt *btree.BTree, n int64, wg *sync.WaitGroup) {
			if tr.Set(intBytes(n), intBytes(n)) == nil {
				atomic.AddInt64(&sot, 1)
			}
			_, _, getErr := tr.Get(intBytes(int64(n)))
			if getErr != nil {
				atomic.AddInt64(&gotErrs, 1)
			}
			wg.Done()
		}(tr, int64(i), wg)
	}
	wg.Wait()
	st := time.Now().Sub(now)
	fmt.Println("set", st, st/time.Duration(loops), atomic.LoadInt64(&sot))
	now = time.Now()

	for i := 0; i < loops; i++ {
		wg.Add(1)
		go func(bt *btree.BTree, n int64, wg *sync.WaitGroup) {
			_, has, getErr := tr.Get(intBytes(int64(n)))
			if has {
				atomic.AddInt64(&got, 1)
			}
			if getErr != nil {
				atomic.AddInt64(&gotErrs, 1)
			}
			wg.Done()
		}(tr, int64(i), wg)
	}
	wg.Wait()
	gt := time.Now().Sub(now)
	fmt.Println("get", gt, gt/time.Duration(loops), atomic.LoadInt64(&got), atomic.LoadInt64(&gotErrs), int64(loops)-atomic.LoadInt64(&got))

	oks := 0
	got = int64(0)
	gotErrs = int64(0)
	for i := 0; i < loops; i++ {
		x, has, getErr := tr.Get(intBytes(int64(i)))
		if has {
			atomic.AddInt64(&got, 1)
		}
		if getErr != nil {
			atomic.AddInt64(&gotErrs, 1)
		}
		if x != nil {
			if bytesInt(x) == i {
				oks++
			}
		}
		//if _, has, _ := tr.Get(intBytes(int64(i))); has {
		//	atomic.AddInt64(&got, 1)
		//}
	}
	gt = time.Now().Sub(now)
	fmt.Println("get", gt, gt/time.Duration(loops), atomic.LoadInt64(&got), atomic.LoadInt64(&gotErrs), int64(loops)-atomic.LoadInt64(&got), oks)
}
