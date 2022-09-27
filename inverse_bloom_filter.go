package main

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/go-log/log"
	boom "github.com/tylertreat/BoomFilters"
)

const batchDuplicatesFilterKey = "batch-duplicates-filter-state"

type BatchDuplicatesFilter struct {
	// The Inverse Bloom Filter may report a false negative but can never report a false positive.
	// behaves a bit like a fixed size hash map that doesn't handle conflicts
	f *boom.InverseBloomFilter
}

func NewBatchDuplicatesFilter(size uint) BatchDuplicatesFilter {
	return BatchDuplicatesFilter{
		f: boom.NewInverseBloomFilter(size),
	}
}

func (f *BatchDuplicatesFilter) loadState(ch ClickhouseDb) {
	s := ch.GetStateString(batchDuplicatesFilterKey)

	bytes, err := f.f.ImportElementsFrom(strings.NewReader(s))
	if err != nil {
		if err == io.EOF {
			log.Infoln("local deduplication state was corrupted, skipping")
			return
		} else {
			must(err)
		}
	}

	log.Debugf("Read %d bytes for deduplication state", bytes)
}

func (f *BatchDuplicatesFilter) TestAndAdd(data string) bool {
	// todo cast this without an allocation?
	return f.f.TestAndAdd([]byte(data))
}

var bfMu = sync.Mutex{}

func (f *BatchDuplicatesFilter) writeState(ch ClickhouseDb) {
	bfMu.Lock()
	defer bfMu.Unlock()
	start := time.Now()
	w := bytes.NewBuffer(make([]byte, 0))
	unwrap(f.f.WriteTo(w))
	ch.SetStateString(batchDuplicatesFilterKey, w.String())
	// Code to measure
	log.Infoln("Wrote bloom filter state in", time.Since(start))
}
