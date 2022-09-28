package main

import (
	"github.com/siddontang/go-log/log"
)

// Panic on err and return just v
func unwrap[T any](v T, err error) T {
	must(err)

	return v
}

// Panic on err
func must(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
