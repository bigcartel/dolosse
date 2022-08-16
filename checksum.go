package main

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash"
)

// TODO also use xxhash to redact sensitive fields - auto detect certain field names
// and also allow config of sensitive field names

func checksumMapValues(m map[string]interface{}, ignoredColumns ...string) uint64 {
	d := xxhash.New()

	lookup := make(map[string]bool, len(ignoredColumns))
	for _, v := range ignoredColumns {
		lookup[v] = true
	}

	i := 0

	keys := make([]string, len(m)-len(ignoredColumns))
	for k := range m {
		if !lookup[k] {
			keys[i] = k
			i++
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		d.Write([]byte(fmt.Sprintf("%v", m[k])))
	}

	return d.Sum64()
}
