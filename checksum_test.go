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
	m["price"] = decimal.Decimal{}
	return m
}

func TestChecksumMap(t *testing.T) {
	m := makeMap()
	m2 := makeMap()
	m2["id"] = uint32(12)

	x := checksumMapValues(m)
	y := checksumMapValues(m2)
	if x != y {
		t.Fatal("Checksums don't match", x, y)
	}

	m2["id"] = uint32(130)
	x = checksumMapValues(m)
	y = checksumMapValues(m2)
	if x == y {
		t.Fatal("Checksums shouldn't match", x, y)
	}
}

func BenchmarkChecksumMap(b *testing.B) {
	m := makeMap()

	for n := 0; n < b.N; n++ {
		checksumMapValues(m)
	}
}
