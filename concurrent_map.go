package main

import "sync"

type ConcurrentMap[V any] struct {
	m sync.Map
}

func NewConcurrentMap[V any]() ConcurrentMap[V] {
	return ConcurrentMap[V]{
		m: sync.Map{},
	}
}

func (m *ConcurrentMap[V]) Reset() {
	*m = NewConcurrentMap[V]()
}

func (m *ConcurrentMap[V]) Get(k string) *V {
	v, _ := (m.m).Load(k)
	if v != nil {
		return v.(*V)
	} else {
		return nil
	}
}

func (m *ConcurrentMap[V]) Set(k string, v *V) {
	m.m.Store(k, v)
}
