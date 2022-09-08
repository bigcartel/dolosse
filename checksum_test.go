package main

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type StringMap map[string]interface{}

func makeMap() StringMap {
	m := make(StringMap)
	m["id"] = 12
	m["name"] = "sweet"
	m["created_at"] = time.Unix(0, 0)
	m["price"] = decimal.NewFromFloat(10.99)
	return m
}

func makeColumns() []ClickhouseQueryColumn {
	columns := []ClickhouseQueryColumn{
		{Name: "id"},
		{Name: "name"},
		{Name: "created_at"},
		{Name: "price"},
	}

	return columns
}

func TestChecksumMap(t *testing.T) {
	columns := makeColumns()
	m := makeMap()
	m2 := makeMap()
	m2["id"] = uint32(12)

	x := checksumMapValues(m, columns)
	y := checksumMapValues(m2, columns)
	if x != y {
		t.Fatal("Checksums don't match", x, y)
	}

	m2["id"] = uint32(130)
	x = checksumMapValues(m, columns)
	y = checksumMapValues(m2, columns)
	if x == y {
		t.Fatal("Checksums shouldn't match", x, y)
	}
}

func BenchmarkChecksumMap(b *testing.B) {
	columns := makeColumns()
	m := makeMap()

	for n := 0; n < b.N; n++ {
		checksumMapValues(m, columns)
	}
}
