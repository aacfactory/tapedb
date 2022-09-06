package blist

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"github.com/aacfactory/tapedb/internal/lru"
	"sync"
	"time"
)

const (
	headSize      = 4096
	listSize      = listHead + itemSize*(maxItems)
	maxCacheLists = 4096 * 64 // 64M
)

var (
	NotSafelyClosedErr = fmt.Errorf("blist was not safely closed")
)

type Options struct {
	Path          string
	MaxCacheLists int64
}

func New(opts Options) (b *BList, err error) {
	file, openErr := ioutils.OpenFile(opts.Path)
	if openErr != nil {
		err = fmt.Errorf("new blist failed, %v", openErr)
		return
	}
	maxCacheListNum := opts.MaxCacheLists
	if maxCacheListNum <= 0 {
		maxCacheListNum = maxCacheLists
	}
	cache, cacheErr := lru.NewLRU(maxCacheListNum, nil)
	if cacheErr != nil {
		err = fmt.Errorf("new blist failed, %v", cacheErr)
		return
	}
	b = &BList{
		mutex:        new(sync.RWMutex),
		num:          0,
		file:         file,
		cache:        cache,
		counter:      new(sync.WaitGroup),
		syncInterval: 1 * time.Second,
		closeCh:      make(chan struct{}, 1),
	}
	err = b.load()
	if err != nil {
		_ = b.file.Close()
		err = fmt.Errorf("load blist from file failed, %v", err)
		return
	}
	b.sync()
	return
}

type BList struct {
	mutex        *sync.RWMutex
	file         *ioutils.File
	cache        *lru.LRU
	counter      *sync.WaitGroup
	syncInterval time.Duration
	closeCh      chan struct{}
	num          int64
}

func (b *BList) AllocList() (list List, err error) {
	b.counter.Add(1)
	b.mutex.Lock()
	list, err = b.allocList()
	b.mutex.Unlock()
	b.counter.Done()
	return
}

func (b *BList) Add(no int64, items [][]byte) (err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.counter.Add(1)
	defer b.counter.Done()
	tail, getTailErr := b.getTail(no)
	if getTailErr != nil {
		err = getTailErr
		return
	}
	dirty, addErr := b.add(tail.Copy(), items)
	if addErr != nil {
		err = addErr
		return
	}
	if len(dirty) == 0 {
		return
	}
	for _, l := range dirty {
		writeErr := b.write(l)
		if writeErr != nil {
			err = writeErr
			return
		}
	}
	return
}

func (b *BList) Get(no int64, offset int64) (items [][]byte, err error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	list, getErr := b.read(no)
	if getErr != nil {
		err = getErr
		return
	}
	n := offset / maxItems
	beg := offset % maxItems
	for i := int64(0); i < n; i++ {
		next := list.next()
		if next == 0 {
			err = fmt.Errorf("out of range")
			return
		}
		list, getErr = b.read(next)
		if getErr != nil {
			err = getErr
			return
		}
	}
	items = append(items, list.Items(beg)...)
	for {
		next := list.next()
		if next == 0 {
			break
		}
		list, getErr = b.read(next)
		if getErr != nil {
			err = getErr
			return
		}
		items = append(items, list.Items(0)...)
	}
	return
}

func (b *BList) Close() (err error) {
	b.counter.Wait()
	b.counter.Add(1)
	close(b.closeCh)
	b.counter.Wait()
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(1))
	wErr := b.file.WriteAt(0, p)
	if wErr != nil {
		err = wErr
		return
	}
	_ = b.file.Sync()
	_ = b.file.Close()
	return
}

func (b *BList) load() (err error) {
	stat, statErr := b.file.File().Stat()
	if statErr != nil {
		err = fmt.Errorf("blist load failed, %v", statErr)
		return
	}
	if stat.IsDir() {
		err = fmt.Errorf("data file of blist must be file")
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		return
	}
	// stats
	p, rErr := b.file.ReadAt(0, 8)
	if rErr != nil {
		err = rErr
		return
	}
	if binary.BigEndian.Uint64(p) != 1 {
		err = NotSafelyClosedErr
		return
	}
	// num
	b.num = (fileSize - headSize) / listSize
	// mark open
	binary.BigEndian.PutUint64(p, 0)
	wErr := b.file.WriteAt(0, p)
	if wErr != nil {
		err = wErr
		return
	}
	syncErr := b.file.Sync()
	if syncErr != nil {
		err = syncErr
		return
	}
	return
}

func (b *BList) sync() {
	go func(file *ioutils.File, syncInterval time.Duration, closeCh chan struct{}, cd *sync.WaitGroup) {
		for {
			stop := false
			select {
			case <-closeCh:
				stop = true
				break
			case <-time.After(syncInterval):
				_ = file.Sync()
			}
			if stop {
				break
			}
		}
		cd.Done()
	}(b.file, b.syncInterval, b.closeCh, b.counter)
}

func (b *BList) allocList() (list List, err error) {
	b.num++
	no := b.num
	list = NewList(no)
	err = b.write(list)
	return
}

func (b *BList) getTail(idx int64) (list List, err error) {
	list, err = b.read(idx)
	if err != nil {
		return
	}
	next := list.next()
	if next == 0 {
		return
	}
	list, err = b.getTail(next)
	return
}

func (b *BList) add(l List, items [][]byte) (dirty []List, err error) {
	adds := 0
	for i, item := range items {
		add := l.Add(item)
		if add {
			adds++
			continue
		}
		nextList, nextErr := b.allocList()
		if nextErr != nil {
			err = nextErr
			return
		}
		l.setNext(nextList.No())
		nextList.setPrev(l.No())
		nextAddDirty, nextAddErr := b.add(nextList, items[i:])
		if nextAddErr != nil {
			err = nextAddErr
			return
		}
		if len(nextAddDirty) > 0 {
			dirty = append(dirty, nextAddDirty...)
		}
		break
	}
	if adds > 0 {
		dirty = append(dirty, l)
	}
	return
}

func (b *BList) read(no int64) (list List, err error) {
	v, cached := b.cache.Get(no)
	if cached {
		list = v.(List)
		return
	}
	offset := headSize + (no-1)*listSize
	region, readErr := b.file.ReadRegion(offset, listSize)
	if readErr != nil {
		err = readErr
		return
	}
	list = NewList(0)
	copy(list, region.Bytes())
	_ = region.Close()
	b.cache.Add(no, list)
	return
}

func (b *BList) write(list List) (err error) {
	idx := list.No()
	offset := headSize + (idx-1)*listSize
	writeErr := b.file.WriteAt(offset, list)
	if writeErr != nil {
		err = writeErr
		return
	}
	b.cache.Add(idx, list)
	return
}
