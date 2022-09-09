package blocks_test

import (
	"fmt"
	"testing"
)

func TestArray(t *testing.T) {
	s := make([]byte, 0, 8)
	fmt.Println(len(s), cap(s))
	s = append(s, []byte("012345678")...)
	fmt.Println(len(s), cap(s), cap(s)/len(s))
}
