package main

import (
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type testField struct {
	val string
}

func (f *testField) AsString() []byte {
	return []byte(f.val)
}

func (f *testField) Value() interface{} {
	return f.val
}

func mysqlFormatTime(t time.Time) string {
	return t.Format(mysql.TimeFormat)
}
