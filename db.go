package tapedb

import (
	"errors"
)

var (
	NotSafelyClosedErr = errors.New("tapedb is not safely closed")
)

type Option struct {
}

type DB interface {
	Tape() (v Tape)
	Close() (err error)
}
