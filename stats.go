package main

import (
	"sync/atomic"

	"github.com/siddontang/go-log/log"
)

type GlobalStats struct {
	DeliveredRows,
	EnqueuedRows,
	ProcessedRows,
	SkippedRowLevelDuplicates,
	SkippedLocallyFilterDuplicates,
	SkippedPersistedDuplicates,
	SkippedDumpDuplicates uint64
}

func (s *GlobalStats) Init() {
	*s = GlobalStats{}
}

func (s *GlobalStats) AddDelivered(n uint64) {
	atomic.AddUint64(&s.DeliveredRows, n)
}

func (s *GlobalStats) IncrementEnqueued() {
	s.incrementStat(&s.EnqueuedRows)
}

func (s *GlobalStats) IncrementProcessed() {
	s.incrementStat(&s.ProcessedRows)
}

func (s *GlobalStats) IncrementSkippedRowLevelDuplicates() {
	s.incrementStat(&s.SkippedRowLevelDuplicates)
}

func (s *GlobalStats) IncrementSkippedLocallyFilteredDuplicates() {
	s.incrementStat(&s.SkippedLocallyFilterDuplicates)
}

func (s *GlobalStats) IncrementSkippedPersistedDuplicates() {
	s.incrementStat(&s.SkippedPersistedDuplicates)
}

func (s *GlobalStats) IncrementSkippedDumpDuplicates() {
	s.incrementStat(&s.SkippedDumpDuplicates)
}

func (s GlobalStats) incrementStat(counter *uint64) {
	atomic.AddUint64(counter, 1)
}

func (s GlobalStats) Print() {
	log.Infoln(s.ProcessedRows, "processed rows")
	log.Infoln(s.EnqueuedRows, "enqueued rows")
	log.Infoln(s.DeliveredRows, "delivered rows")
	log.Infoln(s.SkippedRowLevelDuplicates, "skipped row level duplicate rows")
	log.Infoln(s.SkippedLocallyFilterDuplicates, "skipped locally filtered duplicate rows")
	log.Infoln(s.SkippedPersistedDuplicates, "skipped persisted duplicate rows")
	log.Infoln(s.SkippedDumpDuplicates, "skipped dump duplicate rows")
}

var Stats = GlobalStats{}
