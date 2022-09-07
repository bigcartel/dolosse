package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-faster/city"
)

func checksumMapValues(m map[string]interface{}, ignoredColumns ...string) uint64 {
	lookup := make(map[string]bool, len(ignoredColumns))
	for _, v := range ignoredColumns {
		lookup[v] = true
	}

	i := 0

	values := make([]string, len(m)-len(ignoredColumns))
	for k, v := range m {
		if !lookup[k] && v != nil {
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
