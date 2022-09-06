package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"github.com/aacfactory/tapedb/internal/lru"
	"sync"
	"sync/atomic"
	"time"
)

const (
	degree        = 256          // 256 36k/node
	maxItems      = degree*2 - 1 // max items per node. max children is +1
	maxKeyLen     = 48
	maxCacheNodes = 32 * 64 // 64M
	headSize      = 4096
)

var (
	NotSafelyClosedErr = fmt.Errorf("btree was not safely closed")
)

type Options struct {
	Path          string
	MaxCacheNodes int64
	Less          func(a []byte, b []byte) (ok bool)
}

func New(opts Options) (tr *BTree, err error) {
	file, openErr := ioutils.OpenFile(opts.Path)
	if openErr != nil {
		err = fmt.Errorf("new btree failed, %v", openErr)
		return
	}
	maxCacheNodeNum := opts.MaxCacheNodes
	if maxCacheNodeNum <= 0 {
		maxCacheNodeNum = maxCacheNodes
	}
	cache, cacheErr := lru.NewLRU(maxCacheNodeNum, nil)
	if cacheErr != nil {
		err = fmt.Errorf("new btree failed, %v", cacheErr)
		return
	}
	tr = &BTree{
		mutex:        new(sync.RWMutex),
		cow:          new(cow),
		root:         nil,
		size:         0,
		file:         file,
		lessFn:       opts.Less,
		cache:        cache,
		counter:      new(sync.WaitGroup),
		syncInterval: 1 * time.Second,
		closeCh:      make(chan struct{}, 1),
	}
	err = tr.load()
	if err != nil {
		_ = tr.file.Close()
		err = fmt.Errorf("load btree from file failed, %v", err)
		return
	}
	tr.sync()
	return
}

type BTree struct {
	mutex        *sync.RWMutex
	cow          *cow
	root         *node
	size         int64
	file         *ioutils.File
	cache        *lru.LRU
	lessFn       func(a []byte, b []byte) (ok bool)
	counter      *sync.WaitGroup
	syncInterval time.Duration
	closeCh      chan struct{}
}

func (tr *BTree) sync() {
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
	}(tr.file, tr.syncInterval, tr.closeCh, tr.counter)
}

func (tr *BTree) less(a, b []byte) bool {
	if tr.lessFn == nil {
		return bytes.Compare(a, b) < 0
	}
	return tr.lessFn(a, b)
}

func (tr *BTree) newNode(leaf bool, idx int64) *node {
	n := &node{cow: tr.cow}
	n.idx = idx
	n.key = make([]byte, 8)
	binary.BigEndian.PutUint64(n.key, uint64(idx))
	if !leaf {
		n.children = new([]*node)
	}
	return n
}

func (tr *BTree) find(n *node, key []byte, hint *pathHint, depth int, write bool) (idx int, e Entry, found bool, err error) {
	items, getErr := n.entries(tr.file, tr.cache, write)
	if getErr != nil {
		err = getErr
		return
	}
	low := 0
	high := items.size() - 1
	if depth < 8 && hint.used[depth] {
		idx = int(hint.path[depth])
		if idx >= items.size() {
			// tail item
			if tr.less(items.mustGetEntry(items.size()-1).Key(), key) {
				idx = items.size()
				goto path_match
			}
			idx = items.size() - 1
		}
		if tr.less(key, items.mustGetEntry(idx).Key()) {
			if idx == 0 || tr.less(items.mustGetEntry(idx-1).Key(), key) {
				goto path_match
			}
			high = idx - 1
		} else if tr.less(items.mustGetEntry(idx).Key(), key) {
			low = idx + 1
		} else {
			found = true
			goto path_match
		}
	}

	for low <= high {
		mid := low + ((high+1)-low)/2
		if !tr.less(key, items.mustGetEntry(mid).Key()) {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if low > 0 && !tr.less(items.mustGetEntry(low-1).Key(), key) {
		idx = low - 1
		found = true
	} else {
		idx = low
		found = false
	}

path_match:
	if depth < 8 {
		hint.used[depth] = true
		var pathIndex uint8
		if n.leaf() && found {
			pathIndex = uint8(idx + 1)
		} else {
			pathIndex = uint8(idx)
		}
		if pathIndex != hint.path[depth] {
			hint.path[depth] = pathIndex
			for i := depth + 1; i < 8; i++ {
				hint.used[i] = false
			}
		}
	}
	if found {
		e, found = items.getEntry(idx)
	}

	return idx, e, found, nil
}

func (tr *BTree) Set(key []byte, value []byte) (err error) {
	if len(key) > maxKeyLen {
		return fmt.Errorf("key is too large")
	}
	tr.counter.Add(1)
	tr.mutex.Lock()
	defer tr.mutex.Unlock()

	dirty := &dirtyNodes{
		nodes: make(map[int64]*node),
	}
	err = tr.setHint(NewEntry(key, value), &pathHint{}, dirty)
	if err != nil {
		tr.counter.Done()
		return
	}
	err = tr.commit(dirty)
	if err != nil {
		tr.counter.Done()
		return
	}
	tr.release(tr.root)
	tr.counter.Done()
	return
}

func (tr *BTree) setHint(item Entry, hint *pathHint, dirty *dirtyNodes) (err error) {
	if tr.root == nil {
		tr.root = tr.newNode(true, atomic.AddInt64(&tr.size, 1))
		tr.root.items = NewEntries(maxItems)
		tr.root.items.setEntry(0, item)
		dirty.nodes[tr.root.idx] = tr.root
		return
	}
	split, setErr := tr.nodeSet(&tr.root, item, hint, 0, dirty)
	if setErr != nil {
		err = setErr
		return
	}
	if split {
		left := tr.cowLoad(&tr.root)
		right, median, splitErr := tr.nodeSplit(left, dirty)
		if splitErr != nil {
			err = splitErr
			return
		}
		tr.root = tr.newNode(false, atomic.AddInt64(&tr.size, 1))
		*tr.root.children = make([]*node, 0, maxItems+1)
		*tr.root.children = append([]*node{}, left, right)
		tr.root.items = NewEntries(maxItems)
		tr.root.items.setEntry(0, median)
		dirty.nodes[tr.root.idx] = tr.root
		return tr.setHint(item, hint, dirty)
	}
	return
}

func (tr *BTree) nodeSplit(n *node, dirty *dirtyNodes) (right *node, median Entry, err error) {
	items, getErr := n.entries(tr.file, tr.cache, true)
	if getErr != nil {
		err = getErr
		return
	}
	i := maxItems / 2
	l, m, r := items.Split()
	median = m
	// left node
	left := tr.newNode(n.leaf(), n.idx)
	left.items = l
	if !n.leaf() {
		*left.children = make([]*node, len((*n.children)[:i+1]), maxItems+1)
		copy(*left.children, (*n.children)[:i+1])
	}
	dirty.nodes[left.idx] = left
	// right node
	right = tr.newNode(n.leaf(), atomic.AddInt64(&tr.size, 1))
	right.items = r
	if !n.leaf() {
		*right.children = make([]*node, len((*n.children)[i+1:]), maxItems+1)
		copy(*right.children, (*n.children)[i+1:])
	}
	dirty.nodes[right.idx] = right

	*n = *left
	return right, median, nil
}

func (tr *BTree) updateRoot() (err error) {
	if tr.root == nil {
		return
	}
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(tr.root.idx))
	err = tr.file.WriteAt(0, p)
	return
}

// go:noinline
func (tr *BTree) release(n *node) {
	if n == nil {
		return
	}
	n.release()
	if !n.leaf() {
		for _, child := range *n.children {
			tr.release(child)
		}
	}
}

// go:noinline
func (tr *BTree) copy(n *node) *node {
	if n == nil {
		return nil
	}
	n2 := new(node)
	n2.cow = tr.cow
	n2.idx = n.idx
	n2.key = n.key
	if n.items != nil {
		n2.items = NewEntries(maxItems)
		copy(n2.items, n.items)
	}
	if !n.leaf() {
		n2.children = new([]*node)
		*n2.children = make([]*node, len(*n.children), maxItems+1)
		copy(*n2.children, *n.children)
	}
	return n2
}

func (tr *BTree) cowLoad(cn **node) *node {
	if (*cn).cow != tr.cow {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *BTree) nodeSet(cn **node, item Entry, hint *pathHint, depth int, dirty *dirtyNodes) (split bool, err error) {
	n := tr.cowLoad(cn)
	items, getErr := n.entries(tr.file, tr.cache, true)
	if getErr != nil {
		err = getErr
		return
	}
	i, _, found, findErr := tr.find(n, item.Key(), hint, depth, true)
	if findErr != nil {
		err = findErr
		return
	}
	if found {
		items.replaceEntry(i, item)
		dirty.nodes[n.idx] = n
		return false, nil
	}
	if n.leaf() {
		if items.size() == maxItems {
			return true, nil
		}
		items.setEntry(i, item)
		dirty.nodes[n.idx] = n
		return false, nil
	}
	split, err = tr.nodeSet(&(*n.children)[i], item, hint, depth+1, dirty)
	if err != nil {
		return
	}
	if split {
		if items.size() == maxItems {
			return true, nil
		}
		right, median, splitErr := tr.nodeSplit((*n.children)[i], dirty)
		if splitErr != nil {
			err = splitErr
			return
		}
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		items.setEntry(i, median)
		dirty.nodes[n.idx] = n
		return tr.nodeSet(&n, item, hint, depth, dirty)
	}
	return false, nil
}

func (tr *BTree) Get(key []byte) ([]byte, bool, error) {
	if len(key) > maxKeyLen {
		return nil, false, fmt.Errorf("key is too large")
	}
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()
	if tr.root == nil {
		return nil, false, nil
	}
	hint := &pathHint{}
	n := tr.root
	depth := 0
	for {
		i, e, found, err := tr.find(n, key, hint, depth, false)
		if err != nil {
			return nil, false, err
		}
		if found {
			return e.Value(), true, nil
		}
		if n.leaf() {
			return nil, false, nil
		}
		n = (*n.children)[i]
		depth++
	}
}

func (tr *BTree) commit(dirty *dirtyNodes) (err error) {
	for _, n := range dirty.nodes {
		err = n.update(tr.file, tr.cache)
		if err != nil {
			return
		}
	}
	return
}

func (tr *BTree) load() (err error) {
	stat, statErr := tr.file.File().Stat()
	if statErr != nil {
		err = fmt.Errorf("btree load failed, %v", statErr)
		return
	}
	if stat.IsDir() {
		err = fmt.Errorf("data file of btree must be file")
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		return
	}
	// stats

	p, rErr := tr.file.ReadAt(8, 8)
	if rErr != nil {
		err = rErr
		return
	}
	if binary.BigEndian.Uint64(p) != 1 {
		err = NotSafelyClosedErr
		return
	}

	rootIdxP, readRootIdxErr := tr.file.ReadAt(0, 8)
	if readRootIdxErr != nil {
		err = readRootIdxErr
		return
	}
	rootIdx := int64(binary.BigEndian.Uint64(rootIdxP))
	if rootIdx <= 0 {
		return
	}
	root, rootErr := tr.readNode(rootIdx)
	if rootErr != nil {
		err = rootErr
		return
	}
	tr.root = root
	// mark open
	binary.BigEndian.PutUint64(p, 0)
	wErr := tr.file.WriteAt(8, p)
	if wErr != nil {
		err = wErr
		return
	}
	syncErr := tr.file.Sync()
	if syncErr != nil {
		err = syncErr
		return
	}
	return
}

func (tr *BTree) readNode(idx int64) (n *node, err error) {
	off := (idx-1)*nodeSize + headSize
	np, readErr := tr.file.ReadAt(off, nodeSize)
	if readErr != nil {
		err = readErr
		return
	}
	children := make([]int64, 0, 1)
	head := np[0:nodeHeadLen]
	for i := 0; i < (maxItems + 1); i++ {
		c := binary.BigEndian.Uint64(head[i*8 : (i+1)*8])
		if c == 0 {
			break
		}
		children = append(children, int64(c))
	}
	items := NewEntries(maxItems)
	copy(items, np[nodeHeadLen:])
	n = tr.newNode(len(children) == 0, idx)
	tr.cache.Add(n.idx, items)
	for _, child := range children {
		c, cErr := tr.readNode(child)
		if cErr != nil {
			err = cErr
			return
		}
		*n.children = append(*n.children, c)
	}
	tr.size++
	return
}

func (tr *BTree) Close() (err error) {
	tr.counter.Wait()
	tr.counter.Add(1)
	close(tr.closeCh)
	tr.counter.Wait()
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(1))
	wErr := tr.file.WriteAt(8, p)
	if wErr != nil {
		err = wErr
		return
	}
	err = tr.updateRoot()
	if err != nil {
		return
	}
	_ = tr.file.Sync()
	_ = tr.file.Close()
	return
}
