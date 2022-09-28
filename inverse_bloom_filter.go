package main

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/siddontang/go-log/log"
	boom "github.com/tylertreat/BoomFilters"
)

type BatchDuplicatesFilter struct {
	// The Inverse Bloom Filter may report a false negative but can never report a false positive.
	// behaves a bit like a fixed size hash map that doesn't handle conflicts
	f        *boom.InverseBloomFilter
	fileName string
}

func NewBatchDuplicatesFilter(size uint) BatchDuplicatesFilter {
	return BatchDuplicatesFilter{
		f:        boom.NewInverseBloomFilter(size),
		fileName: "duplicates_state.dat.lz4",
	}
}

func (f *BatchDuplicatesFilter) TestAndAdd(data string) bool {
	// todo cast this without an allocation?
	return f.f.TestAndAdd([]byte(data))
}

func (f *BatchDuplicatesFilter) resetStateFile() {
	f.stateFile(true, func(_ *os.File) {})
}

func (fl *BatchDuplicatesFilter) stateFile(truncate bool, cb func(*os.File)) {
	filePath := unwrap(os.Getwd()) + "/" + fl.fileName

	var f *os.File
	_, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) || truncate {
		f = unwrap(os.Create(filePath))
	} else {
		f = unwrap(os.Open(filePath))
	}
	defer f.Close()
	cb(f)
}

func (fl *BatchDuplicatesFilter) loadState() {
	fl.stateFile(false, func(f *os.File) {
		if unwrap(f.Stat()).Size() > 0 {
			r := lz4.NewReader(f)
			bytes, err := fl.f.ReadFrom(r)
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
	})
}

var bfMu = sync.Mutex{}

func (fi *BatchDuplicatesFilter) writeState() {
	bfMu.Lock()
	defer bfMu.Unlock()
	start := time.Now()
	defer log.Infoln("Wrote bloom filter state in", time.Since(start))

	fi.stateFile(true, func(f *os.File) {
		w := lz4.NewWriter(f)
		defer w.Close()
		unwrap(fi.f.WriteTo(w))
		log.Infof("Wrote %d bytes to %s", unwrap(f.Stat()).Size(), f.Name())
	})
}
