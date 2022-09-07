package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-faster/city"
)

// TODO also use xxhash to redact sensitive fields - auto detect certain field names
// and also allow config of sensitive field names

func checksumMapValues(m map[string]interface{}, ignoredColumns ...string) uint64 {
	lookup := make(map[string]bool, len(ignoredColumns))
	for _, v := range ignoredColumns {
		lookup[v] = true
	}

	i := 0

	values := make([]string, len(m)-len(ignoredColumns))
	for k, v := range m {
		if !lookup[k] {
			values[i] = fmt.Sprintf("%v", v)
			i++
		}
	}

	sort.Strings(values)
	return city.CH64([]byte(strings.Join(values, "")))
}
