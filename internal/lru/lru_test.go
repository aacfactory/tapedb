package lru_test

import (
	"fmt"
	"github.com/aacfactory/tapedb/internal/lru"
	"testing"
)

func TestNewLRU(t *testing.T) {
	cache, err := lru.NewLRU(10, func(key interface{}, value interface{}) {
		fmt.Println("out", key, value)
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		cache.Add(i, i)
	}
	for i := 0; i < 20; i++ {
		fmt.Println(cache.Get(i))
	}
}
