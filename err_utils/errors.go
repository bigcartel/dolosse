package err_utils

import (
	"github.com/siddontang/go-log/log"
)

// Panic on err and return just v
func Unwrap[T any](v T, err error) T {
	Must(err)

	return v
}

// Panic on err
func Must(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
