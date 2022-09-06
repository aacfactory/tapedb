package index

import (
	"fmt"
	"github.com/aacfactory/tapedb/internal/index/blist"
	"github.com/aacfactory/tapedb/internal/index/btree"
	"sync"
)

type Options struct {
	BTree btree.Options
	BList blist.Options
}

func New(options Options) (idx *Indexer, err error) {
	bt, btErr := btree.New(options.BTree)
	if btErr != nil {
		err = btErr
		return
	}
	bl, blErr := blist.New(options.BList)
	if blErr != nil {
		err = blErr
		return
	}
	idx = &Indexer{
		bt:      bt,
		bl:      bl,
		mutex:   &sync.RWMutex{},
		counter: &sync.WaitGroup{},
	}
	return
}

type Indexer struct {
	bt      *btree.BTree
	bl      *blist.BList
	mutex   *sync.RWMutex
	counter *sync.WaitGroup
}

func (idx *Indexer) Set(key []byte, poss [][]byte) (err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	idx.counter.Add(1)
	defer idx.counter.Done()
	// bt
	encodedListNo, hasList, getListNoErr := idx.bt.Get(key)
	if getListNoErr != nil {
		err = getListNoErr
		return
	}
	listNo := int64(0)
	if !hasList {
		// bl
		list, newListErr := idx.bl.AllocList()
		if newListErr != nil {
			err = newListErr
			return
		}
		listNo = list.No()
	} else {
		listNo = decodeListNo(encodedListNo)
	}
	// bt
	addErr := idx.bl.Add(listNo, poss)
	if addErr != nil {
		err = addErr
		return
	}
	// bt
	if !hasList {
		setErr := idx.bt.Set(key, encodeListNo(listNo))
		if setErr != nil {
			err = setErr
			return
		}
	}
	return
}

func (idx *Indexer) Get(key []byte, offset int64) (poss [][]byte, err error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	// bt
	encodedListNo, hasList, getListNoErr := idx.bt.Get(key)
	if getListNoErr != nil {
		err = getListNoErr
		return
	}
	if hasList {
		err = fmt.Errorf("not exist")
		return
	}
	listNo := decodeListNo(encodedListNo)
	// bl
	poss, err = idx.bl.Get(listNo, offset)
	return
}

func (idx *Indexer) Close() (err error) {
	idx.counter.Wait()
	_ = idx.bt.Close()
	_ = idx.bl.Close()
	return
}
