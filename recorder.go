package tapedb

type Recorder interface {
	Key() (key []byte)
	Record(values ...[]byte) (err error)
	Player() (p Player, err error)
}
