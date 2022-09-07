package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-faster/city"
)

// TODO add divider and sort fields by clickhouse column order
func checksumMapValues(m map[string]interface{}) uint64 {
	i := 0

	values := make([]string, len(m))
	for _, v := range m {
		if v != nil {
			switch c := v.(type) {
			case time.Time:
				values[i] = c.UTC().Format("2006-01-02 15:04:05")
			default:
				values[i] = fmt.Sprintf("%v", v)
			}
			i++
		}
	}

	sort.Strings(values)
	concatenated := []byte(strings.Join(values, ""))
	return city.CH64(concatenated)
}
