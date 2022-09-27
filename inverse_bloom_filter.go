package main

import (
	"io"
	"os"
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

func (f *BatchDuplicatesFilter) TestAndAdd(data string) bool {
	// todo cast this without an allocation?
	return f.f.TestAndAdd([]byte(data))
}

func (f *BatchDuplicatesFilter) resetStateFile() {
	f.stateFile(true)
}

func (fl *BatchDuplicatesFilter) stateFile(truncate bool) *os.File {
	fArgs := os.O_RDWR | os.O_CREATE | os.O_APPEND
	if truncate {
		fArgs = fArgs | os.O_TRUNC
	}

	path, err := os.Getwd()
	must(err)

	f, err := os.OpenFile(path+"/duplicates_state", fArgs, 0600)
	must(err)

	return f
}

func (fl *BatchDuplicatesFilter) loadState(ch ClickhouseDb) {
	f := fl.stateFile(false)
	defer f.Close()

	info := unwrap(f.Stat())
	if info.Size() > 0 {
		bytes, err := fl.f.ImportElementsFrom(f)
		if err != nil {
			if err == io.EOF {
				log.Infoln("local deduplication state was corrupted, skipping")
				return
			} else {
				must(err)
			}
		}

		log.Debugf("Read %d bytes for deduplication state from %s", bytes, f.Name())
	}
}

var bfMu = sync.Mutex{}

func (fi *BatchDuplicatesFilter) writeState(ch ClickhouseDb) {
	bfMu.Lock()
	defer bfMu.Unlock()
	start := time.Now()
	defer log.Infoln("Wrote bloom filter state in", time.Since(start))

	f := fi.stateFile(true)
	defer f.Close()

	bytes, err := fi.f.WriteTo(f)
	must(err)
	log.Infof("Wrote %d bytes to %s", bytes, f.Name())
}
