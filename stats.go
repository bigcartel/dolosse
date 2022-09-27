package main

import (
	"fmt"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
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

	promDeliveredRows,
	promEnqueuedRows,
	promProcessedRows,
	promSkippedRowLevelDuplicates,
	promSkippedLocallyFilterDuplicates,
	promSkippedPersistedDuplicates,
	promSkippedDumpDuplicates prometheus.Counter
}

func newPrometheusCounter(name, description string, testing bool) *prometheus.Counter {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: fmt.Sprintf("mysql_clickhouse_sink_%s", name),
		Help: description,
	})

	if !testing {
		prometheus.MustRegister(counter)
	}

	return &counter
}

func (s *GlobalStats) Init(testing bool) {
	*s = GlobalStats{}
	s.promDeliveredRows = *newPrometheusCounter("delivered_rows", "Total number of rows delivered to clickhouse", testing)
	s.promEnqueuedRows = *newPrometheusCounter("enqueued_rows", "Total number of rows enqueued for processing", testing)
	s.promProcessedRows = *newPrometheusCounter("processed_rows", "Total number of rows parsed (processed)", testing)
	s.promSkippedRowLevelDuplicates = *newPrometheusCounter("skipped_row_level_duplicates", "Total number of rows skipped because current and previous row don't differ with respect to the columns included in the clickhouse table", testing)
	s.promSkippedLocallyFilterDuplicates = *newPrometheusCounter("skipped_locally_filtered_duplicates", "Total number of rows skipped because they caught by in memory inverse bloom filter", testing)
	s.promSkippedPersistedDuplicates = *newPrometheusCounter("skipped_persisted_duplicates", "Total number of rows skipped because duplicates were found in clickhouse", testing)
	s.promSkippedDumpDuplicates = *newPrometheusCounter("skipped_dump_duplicates", "Total number of rows skipped during dump because rows with the same id already exist in clickhouse", testing)
}

func (s *GlobalStats) AddDelivered(n uint64) {
	atomic.AddUint64(&s.DeliveredRows, n)
	s.promDeliveredRows.Add(float64(n))
}

func (s *GlobalStats) IncrementEnqueued() {
	s.incrementStat(&s.EnqueuedRows)
	s.promEnqueuedRows.Add(1)
}

func (s *GlobalStats) IncrementProcessed() {
	s.incrementStat(&s.ProcessedRows)
	s.promProcessedRows.Add(1)
}

func (s *GlobalStats) IncrementSkippedRowLevelDuplicates() {
	s.incrementStat(&s.SkippedRowLevelDuplicates)
	s.promSkippedRowLevelDuplicates.Add(1)
}

func (s *GlobalStats) IncrementSkippedLocallyFilteredDuplicates() {
	s.incrementStat(&s.SkippedLocallyFilterDuplicates)
	s.promSkippedLocallyFilterDuplicates.Add(1)
}

func (s *GlobalStats) IncrementSkippedPersistedDuplicates() {
	s.incrementStat(&s.SkippedPersistedDuplicates)
	s.promSkippedPersistedDuplicates.Add(1)
}

func (s *GlobalStats) IncrementSkippedDumpDuplicates() {
	s.incrementStat(&s.SkippedDumpDuplicates)
	s.promSkippedDumpDuplicates.Add(1)
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
