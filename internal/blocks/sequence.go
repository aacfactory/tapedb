package blocks

import (
	"runtime"
	"sync/atomic"
	"time"
)

func NewSequence(limit int64, value int64) (v *Sequence) {
	v = &Sequence{
		value:   value,
		tbc:     value + 1,
		limit:   limit,
		padding: [5]int64{},
	}
	return
}

type Sequence struct {
	value   int64
	tbc     int64
	limit   int64
	padding [5]int64
}

func (seq *Sequence) Value() (v int64) {
	v = atomic.LoadInt64(&seq.value)
	return
}

func (seq *Sequence) HasRemains() (ok bool) {
	ok = seq.Value() < seq.limit
	return
}

func (seq *Sequence) Next(n int64) (i int64, ok bool) {
	if seq.HasRemains() {
		i = atomic.AddInt64(&seq.value, n) - n + 1
	}
	return
}

func (seq *Sequence) Confirm(n int64, span int64) (ok bool, err error) {
	loops := 10
	for {
		if atomic.CompareAndSwapInt64(&seq.tbc, n, n+span) {
			break
		}
		loops--
		if loops < 1 {
			loops = 10
			runtime.Gosched()
		}
		time.Sleep(50 * time.Microsecond)
	}
	return
}
