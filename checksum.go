package main

import (
	"bytes"
	"fmt"
	"time"

	"strconv"

	"github.com/go-faster/city"
	"github.com/shopspring/decimal"
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
			case string:
				vs = c
			case []byte:
				vs = string(c)
			case decimal.Decimal:
				vs = c.String()
			case int8:
				vs = strconv.FormatInt(int64(c), 10)
			case int16:
				vs = strconv.FormatInt(int64(c), 10)
			case int32:
				vs = strconv.FormatInt(int64(c), 10)
			case int64:
				vs = strconv.FormatInt(int64(c), 10)
			case uint32:
				vs = strconv.FormatInt(int64(c), 10)
			case uint64:
				vs = strconv.FormatInt(int64(c), 10)
			default:
				vs = fmt.Sprintf("%v", v)
			}

			b.WriteString(vs)
			b.WriteRune(',')
		}
	}

	// lop off the trailing comma
	b.Truncate(b.Len() - 1)

	return city.CH64(b.Bytes())
}
