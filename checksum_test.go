package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-faster/city"
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

func clickhouseCompatibleChecksumMap(m map[string]interface{}, columns []ClickhouseQueryColumn) uint64 {
	var b bytes.Buffer

	for _, c := range columns {
		v := m[c.Name]
		vs := ""

		if v != nil {
			switch c := v.(type) {
			case time.Time:
				vs = c.UTC().Format("2006-01-02 15:04:05")
			default:
				vs = fmt.Sprintf("%v", v)
			}

			b.WriteString(vs)
			b.WriteRune(',')
		}
	}

	b.Truncate(b.Len() - 1)

	return city.CH64(b.Bytes())
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

	local := checksumMapValues(m, columns)
	clickhouse := clickhouseCompatibleChecksumMap(m, columns)
	if local != clickhouse {
		t.Fatal("Checksums don't match", local, clickhouse)
	}

	m2["id"] = uint32(130)
	x = checksumMapValues(m, columns)
	y = checksumMapValues(m2, columns)
	if x == y {
		t.Fatal("Checksums don't match", x, y)
	}
}

func BenchmarkChecksumMap(b *testing.B) {
	columns := makeColumns()
	m := makeMap()

	for n := 0; n < b.N; n++ {
		checksumMapValues(m, columns)
	}
}

func BenchmarkNaiveChecksumMap(b *testing.B) {
	columns := makeColumns()
	m := makeMap()

	for n := 0; n < b.N; n++ {
		clickhouseCompatibleChecksumMap(m, columns)
	}
}
