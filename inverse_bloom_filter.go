package main

import (
	"io"
	"os"

	"github.com/siddontang/go-log/log"
	boom "github.com/tylertreat/BoomFilters"
)

// The Inverse Bloom Filter may report a false negative but can never report a false positive.
// behaves a bit like a fixed size hash map that doesn't handle conflicts
var batchDuplicatesFilter = boom.NewInverseBloomFilter(10000000)

func boomFilterStateFile(truncate bool) *os.File {
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

func readBloomFilterState() {
	f := boomFilterStateFile(false)
	defer f.Close()

	fi, err := f.Stat()
	must(err)
	if fi.Size() > 0 {
		bytes, err := batchDuplicatesFilter.ImportElementsFrom(f)
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

func writeBloomFilterState() {
	f := boomFilterStateFile(true)
	defer f.Close()

	bytes, err := batchDuplicatesFilter.WriteTo(f)
	must(err)
	log.Infof("Wrote %d bytes to %s", bytes, f.Name())
}
