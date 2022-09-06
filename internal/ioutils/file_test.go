package ioutils_test

import (
	"fmt"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenFile(t *testing.T) {
	file, openErr := ioutils.OpenFile(`D:\os\archlinux-2021.12.01-x86_64.iso`)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer file.Close()
	stat, statErr := file.File().Stat()
	if statErr != nil {
		t.Fatal(statErr)
	}
	size := stat.Size()/int64(os.Getpagesize()) - 1
	loops := 1000000
	wg := &sync.WaitGroup{}
	errs := int64(0)
	now := time.Now()
	for i := 0; i < loops; i++ {
		wg.Add(1)
		go func(r *ioutils.File, wg *sync.WaitGroup) {
			off := rand.Int63n(size)
			m, mErr := r.ReadRegion(off, 4096)
			wg.Done()
			if mErr != nil {
				atomic.AddInt64(&errs, 1)
				return
			}
			m.Close()
		}(file, wg)
	}
	wg.Wait()
	sub := time.Now().Sub(now)
	fmt.Println(errs, sub, sub/time.Duration(loops))
}
