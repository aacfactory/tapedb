package blocks_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/tapedb/internal/blocks"
	"testing"
	"time"
)

func TestSegment_Read(t *testing.T) {
	p := []byte(time.Now().String())
	seg := blocks.NewSegment(p, 32)
	r, err := seg.Content()
	fmt.Println(len(p) == len(r), bytes.Equal(p, r), string(r), err)
}
