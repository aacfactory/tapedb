package tapedb

type Tape interface {
	Name() (name []byte)
	Recorder(key []byte) (r Recorder)
	Player(key []byte) (p Player)
}
