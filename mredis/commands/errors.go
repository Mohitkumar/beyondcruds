package commands

import "errors"

var (
	ErrUnknownCommand = errors.New("ERR unknown command")
	ErrKeyNotFound    = errors.New("no such key")
)
