package tapedb

type Player interface {
	Key() (key []byte)
	Play(pos int64, size int64) (values [][]byte, err error)
	Save(pos int64, comment []byte) (err error)
	LatestSavedPos() (pos int64, comment []byte, err error)
}
