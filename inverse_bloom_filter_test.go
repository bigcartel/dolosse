package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInverseBloomFilter(t *testing.T) {
	f := NewBatchDuplicatesFilter(100)
	v := f.TestAndAdd("hi")
	v2 := f.TestAndAdd("hi")

	assert.False(t, v)
	assert.True(t, v2)
	assert.NotEqual(t, v, v2)
}

func newFilter() BatchDuplicatesFilter {
	return NewBatchDuplicatesFilter(10)
}

type MockCh struct {
	lastGetStateKey   string
	lastSetStateKey   string
	lastSetStateValue string
}

func (ch *MockCh) GetStateString(k string) string {
	ch.lastGetStateKey = k
	return ch.lastSetStateValue
}

func (ch *MockCh) SetStateString(k string, v string) {
	ch.lastSetStateKey = k
	ch.lastSetStateValue = v
}

func TestPersistedInverseBloomFilter(t *testing.T) {
	ch := &MockCh{}
	f := newFilter()
	v1 := f.TestAndAdd("hi")
	v2 := f.TestAndAdd("hey")
	f.snapshotState()
	f.writeState(ch)

	assert.False(t, v1)
	assert.False(t, v2)

	f2 := newFilter()
	f2.loadState(ch)
	v3 := f2.TestAndAdd("hi")
	v4 := f2.TestAndAdd("hey")
	v5 := f2.TestAndAdd("different")
	v6 := f2.TestAndAdd("different")
	v7 := f2.TestAndAdd("zef")
	f2.snapshotState()
	f2.writeState(ch)

	assert.Equal(t, ch.lastGetStateKey, batchDuplicatesFilterKey)
	assert.Equal(t, ch.lastSetStateKey, batchDuplicatesFilterKey)

	assert.True(t, v3)
	assert.True(t, v4)
	assert.False(t, v5)
	assert.True(t, v6)
	assert.False(t, v7)

	f3 := newFilter()
	f3.loadState(ch)
	v8 := f3.TestAndAdd("hi")
	v9 := f3.TestAndAdd("zef")
	v10 := f3.TestAndAdd("asdlflkjasldghqwuoehgow")

	assert.True(t, v8)
	assert.True(t, v9)
	assert.False(t, v10)
}
