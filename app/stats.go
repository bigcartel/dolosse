package app

import (
	"fmt"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/siddontang/go-log/log"
)

type Stats struct {
	DeliveredRows,
	EnqueuedRows,
	ProcessedRows,
	SkippedRowLevelDuplicates,
	SkippedPersistedDuplicates,
	SkippedColumnMismatch,
	SkippedDumpDuplicates uint64

	promDeliveredRows,
	promEnqueuedRows,
	promProcessedRows,
	promSkippedRowLevelDuplicates,
	promSkippedPersistedDuplicates,
	promSkippedColumnMismatch,
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

func NewStats(testing bool) *Stats {
	return &Stats{
		promDeliveredRows:              *newPrometheusCounter("delivered_rows", "Total number of rows delivered to clickhouse", testing),
		promEnqueuedRows:               *newPrometheusCounter("enqueued_rows", "Total number of rows enqueued for processing", testing),
		promProcessedRows:              *newPrometheusCounter("processed_rows", "Total number of rows parsed (processed)", testing),
		promSkippedRowLevelDuplicates:  *newPrometheusCounter("skipped_row_level_duplicates", "Total number of rows skipped because current and previous row don't differ with respect to the columns included in the clickhouse table", testing),
		promSkippedPersistedDuplicates: *newPrometheusCounter("skipped_persisted_duplicates", "Total number of rows skipped because duplicates were found in clickhouse", testing),
		promSkippedColumnMismatch:      *newPrometheusCounter("skipped_column_mismatch", "Total number of rows skipped due to event having different columns tham current mysql schema", testing),
		promSkippedDumpDuplicates:      *newPrometheusCounter("skipped_dump_duplicates", "Total number of rows skipped during dump because rows with the same id already exist in clickhouse", testing),
	}
}

func (s *Stats) AddDelivered(n uint64) {
	atomic.AddUint64(&s.DeliveredRows, n)
	s.promDeliveredRows.Add(float64(n))
}

func (s *Stats) IncrementEnqueued() {
	s.incrementStat(&s.EnqueuedRows)
	s.promEnqueuedRows.Add(1)
}

func (s *Stats) IncrementProcessed() {
	s.incrementStat(&s.ProcessedRows)
	s.promProcessedRows.Add(1)
}

func (s *Stats) IncrementSkippedRowLevelDuplicates() {
	s.incrementStat(&s.SkippedRowLevelDuplicates)
	s.promSkippedRowLevelDuplicates.Add(1)
}

func (s *Stats) IncrementSkippedPersistedDuplicates() {
	s.incrementStat(&s.SkippedPersistedDuplicates)
	s.promSkippedPersistedDuplicates.Add(1)
}

func (s *Stats) IncrementSkippedColumnMismatch() {
	s.incrementStat(&s.SkippedColumnMismatch)
	s.promSkippedColumnMismatch.Add(1)
}

func (s *Stats) IncrementSkippedDumpDuplicates() {
	s.incrementStat(&s.SkippedDumpDuplicates)
	s.promSkippedDumpDuplicates.Add(1)
}

func (s Stats) incrementStat(counter *uint64) {
	atomic.AddUint64(counter, 1)
}

func (s Stats) Print() {
	log.Infoln(s.ProcessedRows, "processed rows")
	log.Infoln(s.EnqueuedRows, "enqueued rows")
	log.Infoln(s.DeliveredRows, "delivered rows")
	log.Infoln(s.SkippedRowLevelDuplicates, "skipped row level duplicate rows")
	log.Infoln(s.SkippedPersistedDuplicates, "skipped persisted duplicate rows")
	log.Infoln(s.SkippedColumnMismatch, "skipped column mismatch rows")
	log.Infoln(s.SkippedDumpDuplicates, "skipped dump duplicate rows")
}
