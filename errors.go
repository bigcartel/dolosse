package main

import "github.com/siddontang/go-log/log"

func unwrap[T any](v T, err error) T {
	must(err)

	return v
}

func must(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
