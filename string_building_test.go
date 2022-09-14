package main

import (
	"fmt"
	"strings"
	"bytes"
	"testing"
)

func BenchmarkSprintfTableString(b *testing.B) {
	table := "thing"
	columnPath := "other.thing"
	test := "asdf"

	for n := 0; n < b.N; n++ {
		s := fmt.Sprintf("%s.%s", table, columnPath)
		strings.Contains(test, s)
	}
}

func BenchmarkStringBuilderString(b *testing.B) {
	table := "thing"
	columnPath := "other.thing"
	test := "other asdf"

	for n := 0; n < b.N; n++ {
		var sb strings.Builder
		sb.WriteString(table)
		sb.WriteString(columnPath)
		strings.Contains(test, sb.String())
	}
}

func BenchmarkCopyString(b *testing.B) {
	table := "thing"
	columnPath := "other.thing"
	test := []byte("other asdf")

	for n := 0; n < b.N; n++ {
		bs := make([]byte, len(table) + len(columnPath) + 1)
		l := 0
		l += copy(bs[l:], table)
		l += copy(bs[l:], ".")
		copy(bs[l:], columnPath)
		bytes.Contains(bs, test)
	}
}
