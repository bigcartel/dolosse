package main

import (
	"bytes"
	"io"
	"strings"

	"github.com/siddontang/go-log/log"
	boom "github.com/tylertreat/BoomFilters"
)

// The Inverse Bloom Filter may report a false negative but can never report a false positive.
// behaves a bit like a fixed size hash map that doesn't handle conflicts
var batchDuplicatesFilter = boom.NewInverseBloomFilter(1000000)

const batchDuplicatesFilterKey = "batch-duplicates-filter-state"

func readBloomFilterState(ch ClickhouseDb) {
	s := ch.GetStateString(batchDuplicatesFilterKey)

	bytes, err := batchDuplicatesFilter.ImportElementsFrom(strings.NewReader(s))
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

func writeBloomFilterState(ch ClickhouseDb) {
	w := bytes.NewBuffer(make([]byte, 0))
	bytes := unwrap(batchDuplicatesFilter.WriteTo(w))
	ch.SetStateString(batchDuplicatesFilterKey, w.String())
	log.Debugf("Wrote %d bytes", bytes)
}
