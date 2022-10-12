package main

import (
	"github.com/siddontang/go-log/log"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type Pk struct {
	ColumnName string
	Value      interface{}
}

type Pks []Pk

func compare[T constraints.Ordered](v1, v2 T) int {
	if v1 < v2 {
		return -1
	} else if v1 == v2 {
		return 0
	} else {
		return 1
	}
}

func (p Pks) Compare(s Pks) int {
	return slices.CompareFunc(p, s, func(e1, e2 Pk) int {
		v2 := e2.Value
		switch v1 := e1.Value.(type) {
		case uint:
			return compare(v1, v2.(uint))
		case uint8:
			return compare(v1, v2.(uint8))
		case uint16:
			return compare(v1, v2.(uint16))
		case uint32:
			return compare(v1, v2.(uint32))
		case uint64:
			return compare(v1, v2.(uint64))
		case int:
			return compare(v1, v2.(int))
		case int8:
			return compare(v1, v2.(int8))
		case int16:
			return compare(v1, v2.(int16))
		case int32:
			return compare(v1, v2.(int32))
		case int64:
			return compare(v1, v2.(int64))
		case string:
			return compare(v1, v2.(string))
		case nil:
			return 0
		default:
			log.Panicln("Unsupported primary key type ", e1, v1)
		}

		return 0
	})
}
