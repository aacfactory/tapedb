package blist_test

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/index/blist"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	b, bErr := blist.New(blist.Options{
		Path:          `P:\tmp\bl1.txt`,
		MaxCacheLists: 0,
	})
	if bErr != nil {
		t.Fatal(bErr)
	}
	defer b.Close()
	l1, l1Err := b.AllocList()
	if l1Err != nil {
		t.Error(l1Err)
		return
	}
	l1Items := make([][]byte, 0, 1)
	for i := 0; i < 20; i++ {
		l1Items = append(l1Items, pos(int64(i+1)))
	}
	l1AddErr := b.Add(l1.No(), l1Items)
	if l1AddErr != nil {
		t.Error(l1AddErr)
		return
	}
	l2, l2Err := b.AllocList()
	if l2Err != nil {
		t.Error(l2Err)
		return
	}
	l2Items := make([][]byte, 0, 1)
	for i := 0; i < 10; i++ {
		l2Items = append(l2Items, pos(int64(i+1)))
	}
	l2AddErr := b.Add(l2.No(), l2Items)
	if l2AddErr != nil {
		t.Error(l2AddErr)
		return
	}
	l1Full, get1Err := b.Get(l1.No(), 0)
	if get1Err != nil {
		t.Error(get1Err)
		return
	}
	fmt.Println(l1.No(), "f", len(l1Full))
	l1Off3, get1Off3Err := b.Get(l1.No(), 3)
	if get1Off3Err != nil {
		t.Error(get1Off3Err)
		return
	}
	fmt.Println(l1.No(), "3", len(l1Off3), binary.BigEndian.Uint64(l1Off3[0][0:8]))
	l2Off9, get2Off9Err := b.Get(l2.No(), 9)
	if get2Off9Err != nil {
		t.Error(get2Off9Err)
		return
	}
	fmt.Println(l2.No(), "9", len(l2Off9), binary.BigEndian.Uint64(l2Off9[0][0:8]))
	l2Off100, get2Off100Err := b.Get(l2.No(), 100)
	fmt.Println(l2.No(), "100", len(l2Off100), get2Off100Err)
}

func TestBList_Add(t *testing.T) {
	b, bErr := blist.New(blist.Options{
		Path:          `P:\tmp\bl1.txt`,
		MaxCacheLists: 0,
	})
	if bErr != nil {
		t.Fatal(bErr)
	}
	defer b.Close()
	nos := []int64{1, 2, 3, 4, 5}
	for i := 0; i < len(nos); i++ {
		_, _ = b.AllocList()
	}
	wg := new(sync.WaitGroup)
	loops := 1000000
	sotErrs := int64(0)
	now := time.Now()
	for i := 0; i < loops; i++ {
		wg.Add(1)
		no := nos[i%len(nos)]
		go func(b *blist.BList, n int64, wg *sync.WaitGroup, no int64) {
			addErr := b.Add(no, [][]byte{pos(n)})
			if addErr != nil {
				atomic.AddInt64(&sotErrs, 1)
			}
			wg.Done()
		}(b, int64(i+1), wg, no)
	}
	wg.Wait()
	sd := time.Now().Sub(now)
	fmt.Println("set", loops, sotErrs, sd, sd/time.Duration(loops))

	gotErrs := int64(0)
	now = time.Now()
	for i := 0; i < loops; i++ {
		wg.Add(1)
		no := nos[i%len(nos)]
		go func(b *blist.BList, n int64, wg *sync.WaitGroup, no int64) {
			_, getErr := b.Get(no, 0)
			if getErr != nil {
				atomic.AddInt64(&gotErrs, 1)
			}
			wg.Done()
		}(b, int64(i+1), wg, no)
	}
	wg.Wait()
	gd := time.Now().Sub(now)
	fmt.Println("got", loops, gotErrs, gd, gd/time.Duration(loops))
}
