package btree

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/tapedb/internal/ioutils"
	"github.com/aacfactory/tapedb/internal/lru"
)

const (
	nodeHeadLen = 8 * (maxItems + 1)
	nodeBodyLen = entrySize*(maxItems) + entriesHeadLen
	nodeSize    = nodeHeadLen + nodeBodyLen // 36k
)

type cow struct {
	_ int
}

type pathHint struct {
	used [8]bool
	path [8]uint8
}

type node struct {
	cow      *cow
	key      []byte
	idx      int64
	items    Entries
	children *[]*node
}

func (n *node) entries(reader *ioutils.File, cache *lru.LRU, hold bool) (v Entries, err error) {
	if hold {
		if n.items != nil && len(n.items) > 0 {
			v = n.items
			return
		}
		v, err = n.load(reader, cache)
		if err == nil {
			n.items = v
		}
	} else {
		v, err = n.load(reader, cache)
	}
	return
}

func (n *node) load(reader *ioutils.File, cache *lru.LRU) (v Entries, err error) {
	vv, has := cache.Get(n.idx)
	if has {
		v, has = vv.(Entries)
		if !has {
			err = fmt.Errorf("btree load node from cache failed, type of cached node is not matched")
			return
		}
		return
	}
	fmt.Println("no cache", n.idx, binary.BigEndian.Uint64(n.key))
	off := (n.idx-1)*nodeSize + headSize
	m, readErr := reader.ReadRegion(off, nodeSize)
	if readErr != nil {
		err = readErr
		return
	}
	v = NewEntries(maxItems)
	copy(v, m.Bytes()[nodeHeadLen:])
	_ = m.Close()
	cache.Add(n.idx, v)
	return
}

func (n *node) update(file *ioutils.File, cache *lru.LRU) (err error) {
	if n.items == nil || len(n.items) == 0 {
		err = fmt.Errorf("btree can not update node which has nil entries failed")
		return
	}
	p := make([]byte, nodeSize)

	if !n.leaf() {
		for i, child := range *n.children {
			copy(p[i*8:(i+1)*8], child.key)
		}
	}
	items := NewEntries(maxItems)
	copy(items, n.items)
	copy(p[nodeHeadLen:], items)
	off := (n.idx-1)*nodeSize + headSize
	err = file.WriteAt(off, p)
	if err != nil {
		return
	}
	cache.Add(n.idx, items)
	return
}

func (n *node) release() {
	n.items = nil
}

func (n *node) leaf() bool {
	return n.children == nil || len(*n.children) == 0
}

func (n *node) String() (v string) {
	v = fmt.Sprintf("%d", n.idx)
	if n.leaf() {
		return
	}
	s := ""
	for _, child := range *n.children {
		s = s + ", " + child.String()
	}
	s = s[2:]
	v = v + "\n\t" + fmt.Sprintf("[%d]", len(*n.children)) + s
	return
}

type dirtyNodes struct {
	nodes map[int64]*node
}
