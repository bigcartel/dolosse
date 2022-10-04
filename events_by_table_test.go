package main

import (
	"fmt"
	"testing"
)

func BenchmarkEventByTableReset(b *testing.B) {
	ebt := make(EventsByTable, 0)
	for i := 0; i < 10; i++ {
		d := make([]*RowInsertData, 100000)

		for i := 0; i < 100000; i++ {
			d[i] = RowInsertDataPool.Get().(*RowInsertData)
		}

		ebt[fmt.Sprintf("s%d", i)] = &d
	}

	for n := 0; n < b.N; n++ {
		ebt.Reset(true)
	}
}
