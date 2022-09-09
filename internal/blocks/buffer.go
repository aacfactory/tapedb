package blocks

type Buffer struct {
	capacity int64
}

func (buf *Buffer) Write(p []byte) (pos Position, err error) {

	return
}

func (buf *Buffer) ReadAt(pos Position) (p []byte, err error) {

	return
}

func (buf *Buffer) Size() (n int64) {

	return
}

func (buf *Buffer) Position(offset int64) (pos Position, err error) {

	return
}
