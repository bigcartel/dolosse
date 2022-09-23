package main

import "sync"

type ConcurrentMap[V any] struct {
	mu sync.RWMutex
	m  *map[string]*V
}

func NewConcurrentMap[V any]() ConcurrentMap[V] {
	m := make(map[string]*V)

	return ConcurrentMap[V]{
		mu: sync.RWMutex{},
		m:  &m,
	}
}

func (m *ConcurrentMap[V]) Get(k string) *V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return (*m.m)[k]
}

func (m *ConcurrentMap[V]) Set(k string, v *V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	(*m.m)[k] = v
}
