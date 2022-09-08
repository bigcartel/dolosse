package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/go-faster/city"
)

func checksumMapValues(m map[string]interface{}, columns []ClickhouseQueryColumn) uint64 {
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
