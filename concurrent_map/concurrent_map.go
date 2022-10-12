package concurrent_map

import "sync"

type ConcurrentMap[V any] struct {
	sync.Map
}

func NewConcurrentMap[V any]() ConcurrentMap[V] {
	return ConcurrentMap[V]{
		sync.Map{},
	}
}

func (m *ConcurrentMap[V]) Reset() {
	*m = NewConcurrentMap[V]()
}

func (m *ConcurrentMap[V]) Get(k string) *V {
	v, _ := m.Load(k)
	if v != nil {
		return v.(*V)
	} else {
		return nil
	}
}

func (m *ConcurrentMap[V]) Set(k string, v *V) {
	m.Store(k, v)
}
