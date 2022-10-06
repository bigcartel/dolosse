package main

import (
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/siddontang/go-log/log"
	boom "github.com/tylertreat/BoomFilters"
)

// the whole motivating reason for this to store in clickhouse is to improve performance on a rewind.
// I'm not sure that's great or worth it.
// If we really want to do local persisted dedupe I think there are other approaches.
type GetSetBatchDuplicatesFilterState = interface {
	GetStateString(string) string
	SetStateString(string, string)
}

type BatchDuplicatesFilter struct {
	// The Inverse Bloom Filter may report a false negative but can never report a false positive.
	// behaves a bit like a fixed size hash map that doesn't handle conflicts
	f             *boom.InverseBloomFilter
	stateSnapshot string
}

func NewBatchDuplicatesFilter(size uint) BatchDuplicatesFilter {
	return BatchDuplicatesFilter{
		f: boom.NewInverseBloomFilter(size),
	}
}

func (f *BatchDuplicatesFilter) TestAndAdd(data string) bool {
	bfMu.RLock()
	defer bfMu.RUnlock()
	// todo cast this without an allocation?
	return f.f.TestAndAdd(StringToByteSlice(data))
}

var bfMu = sync.RWMutex{}

func (fi *BatchDuplicatesFilter) snapshotState() {
	bfMu.Lock()
	sb := strings.Builder{}
	sb.Grow(BatchDuplicatesFilterSize)
	w := lz4.NewWriter(&sb)
	unwrap(fi.f.WriteTo(w))
	w.Close()
	bfMu.Unlock()
	fi.stateSnapshot = sb.String()
}

const batchDuplicatesFilterKey = "batch_duplicates_state.dat.lz4"

func (f *BatchDuplicatesFilter) resetState(ch GetSetBatchDuplicatesFilterState) {
	ch.SetStateString(batchDuplicatesFilterKey, "")
}

func (f *BatchDuplicatesFilter) loadState(ch GetSetBatchDuplicatesFilterState) {
	s := ch.GetStateString(batchDuplicatesFilterKey)
	r := lz4.NewReader(strings.NewReader(s))

	bytes, err := f.f.ReadFrom(r)
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

func (fi *BatchDuplicatesFilter) writeState(ch GetSetBatchDuplicatesFilterState) {
	if fi.stateSnapshot == "" {
		return
	}

	start := time.Now()
	ch.SetStateString(batchDuplicatesFilterKey, fi.stateSnapshot)
	log.Infof("Wrote %d bytes to bloom filter state in %s", len(fi.stateSnapshot), time.Since(start))
	fi.stateSnapshot = ""
}