package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/pkg/profile"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sync"

	"github.com/siddontang/go-log/log"
)

type LookupMap map[string]bool

type ClickhouseBatchColumns = []interface{}
type ClickhouseBatchColumnsByTable = map[string]ClickhouseBatchColumns
type ClickhouseBatchRow struct {
	InsertColumns ClickhouseBatchColumns
	Table         string
}

func processEventWorker(input <-chan *MysqlReplicationRowEvent, output chan<- *RowInsertData) {
	for {
		select {
		case <-State.ctx.Done():
			return
		case event := <-input:
			columns, hasColumns := State.chColumns.ColumnsForTable(event.Table.Name)

			if !hasColumns {
				Stats.IncrementProcessed()
				continue
			}

			insertData, isDup := event.ToClickhouseRowData(columns)

			if isDup {
				Stats.IncrementSkippedRowLevelDuplicates()
			} else {
				output <- insertData
			}

			Stats.IncrementProcessed()
		}
	}
}

func startProcessEventsWorkers() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go processEventWorker(State.processRows, State.batchWrite)
	}
}

func OnRow(e *MysqlReplicationRowEvent) {
	State.processRows <- e
	Stats.IncrementEnqueued()
}

func tableWithDb(table string) string {
	return fmt.Sprintf("%s.%s", *Config.ClickhouseDb, table)
}

func logNoRows(table string) {
	log.Infoln("no rows to send for", table, "skipping...")
}

func deliverBatchForTable(clickhouseDb ClickhouseDb, table string, rows *[]*RowInsertData) {
	columns, hasColumns := State.chColumns.ColumnsForTable(table)

	if !hasColumns {
		log.Infoln("No columns in clickhouse found for", table)
		return
	}

	if len(*rows) == 0 {
		logNoRows(table)
		return
	}

	tableWithDb := tableWithDb(table)

	batchColumnArrays, writeCount := buildClickhouseBatchRows(clickhouseDb, tableWithDb, rows, columns.columns)

	if writeCount == 0 {
		logNoRows(table)
		return
	}

	log.Infof("sending batch of %d records for %s", writeCount, table)

	sendClickhouseBatch(clickhouseDb, tableWithDb, batchColumnArrays, columns.columns)

	Stats.AddDelivered(uint64(writeCount))

	log.Infoln("batch sent for", tableWithDb)
}

func buildClickhouseBatchRows(clickhouseDb ClickhouseDb, tableWithDb string, processedRows *[]*RowInsertData, chColumns []ClickhouseQueryColumn) (ClickhouseBatchColumns, int) {
	minMaxValues := getMinMaxValues(processedRows)

	hasStoredIds := false
	var storedIdsMap Set[int64]

	if minMaxValues.MinDumpId > 0 {
		hasStoredIds, storedIdsMap = clickhouseDb.QueryIdRange(tableWithDb,
			minMaxValues.MinDumpId,
			minMaxValues.MaxDumpId)
	}

	hasDuplicates, duplicatesMap := clickhouseDb.QueryDuplicates(tableWithDb, minMaxValues)

	batchColumnArrays := make(ClickhouseBatchColumns, len(chColumns))
	writeCount := 0

	for _, rowInsertData := range *processedRows {
		if rowInsertData.EventAction == "dump" && hasStoredIds && storedIdsMap.Contains(rowInsertData.Id) {
			Stats.IncrementSkippedDumpDuplicates()
			continue
		}

		if hasDuplicates && duplicatesMap.Contains(rowInsertData.DedupeKey()) {
			if rowInsertData.EventAction != "dump" {
				Stats.IncrementSkippedPersistedDuplicates()
			} else {
				Stats.IncrementSkippedDumpDuplicates()
			}

			continue
		}

		if rowInsertData.EventCreatedAt.Year() == 1 {
			log.Panicln(rowInsertData)
		}

		writeCount++

		for i, col := range chColumns {
			val := rowInsertData.Event[col.Name]
			newColumnAry, err := reflectAppend(col.Type, batchColumnArrays[i], val, len(*processedRows))

			if err != nil {
				log.Infoln(err)
				log.Infoln("Column:", col.Name)
				log.Fatalf("Insert Data: %+v\n", rowInsertData)
			}

			batchColumnArrays[i] = newColumnAry
		}
	}

	return batchColumnArrays, writeCount
}

func commaSeparatedColumnNames(columns []ClickhouseQueryColumn) string {
	columnNames := ""
	count := 0
	columnCount := len(columns)
	for _, c := range columns {
		count++
		columnNames += c.Name
		if count != columnCount {
			columnNames += ","
		}
	}

	return columnNames
}

func sendClickhouseBatch(clickhouseDb ClickhouseDb, tableWithDb string, batchColumnArrays ClickhouseBatchColumns, chColumns []ClickhouseQueryColumn) {
	batch := unwrap(clickhouseDb.conn.PrepareBatch(context.Background(),
		fmt.Sprintf("INSERT INTO %s (%s)", tableWithDb, commaSeparatedColumnNames(chColumns))))

	for i, col := range batchColumnArrays {
		err := batch.Column(i).Append(col)
		if err != nil {
			log.Panicln(err, tableWithDb, col)
		}
	}

	err := batch.Send()
	if err != nil {
		log.Panic(err, tableWithDb)
	}
}

type MinMaxValues struct {
	MinDumpId,
	MaxDumpId int64
	ValuesByServerId map[string]struct {
		MinTransactionId,
		MaxTransactionId uint64
	}
}

type MinMaxValuesMap map[string]struct {
	MinTransactionId,
	MaxTransactionId uint64
}

func getMinMaxValues(rows *[]*RowInsertData) MinMaxValues {
	valuesByServerId := make(MinMaxValuesMap, 1)
	var minDumpId,
		maxDumpId int64

	for _, r := range *rows {
		comparingTransactionId := r.TransactionId
		currentMinMax := valuesByServerId[r.ServerId]

		if r.EventAction == "dump" {
			if minDumpId == 0 || minDumpId > r.Id {
				minDumpId = r.Id
			}
			if maxDumpId < r.Id {
				maxDumpId = r.Id
			}
		} else {
			if currentMinMax.MaxTransactionId < comparingTransactionId {
				currentMinMax.MaxTransactionId = comparingTransactionId
			}
			if currentMinMax.MinTransactionId == 0 || currentMinMax.MinTransactionId > comparingTransactionId {
				currentMinMax.MinTransactionId = comparingTransactionId
			}

			valuesByServerId[r.ServerId] = currentMinMax
		}
	}

	return MinMaxValues{
		MinDumpId:        minDumpId,
		MaxDumpId:        maxDumpId,
		ValuesByServerId: valuesByServerId,
	}
}

type EventsByTable map[string]*[]*RowInsertData

func (es *EventsByTable) Reset(recycleSlices bool) {
	for k, vSlice := range *es {
		for _, v := range *vSlice {
			RowInsertDataPool.Put(v)
		}
		if recycleSlices {
			newSlice := (*vSlice)[:0]
			(*es)[k] = &newSlice
		}
	}
}

func batchWrite() {
	clickhouseDb := unwrap(establishClickhouseConnection())

	eventsByTable := make(EventsByTable)
	var firstGtidInBatch string
	var timer *time.Timer
	var lastEventsInBatchCount,
		eventsInBatchCount,
		batchNumber int

	resetBatch := func() {
		firstGtidInBatch = ""
		timer = time.NewTimer(*Config.BatchWriteInterval)
		lastEventsInBatchCount = eventsInBatchCount
		eventsInBatchCount = 0
	}

	resetBatch()

	deliver := func() {
		// snapshot every 60 seconds with default settings
		shouldSnapshotDuplicatesFilter := batchNumber%6 == 0

		if shouldSnapshotDuplicatesFilter {
			State.batchDuplicatesFilter.snapshotState()
		}

		// Could also get delay by peeking events periodicially to avoid computing it
		delay := replicationDelay.Load()
		log.Infoln("replication delay is", delay, "seconds")

		deliverBatch(clickhouseDb, eventsByTable, firstGtidInBatch)

		if shouldSnapshotDuplicatesFilter {
			go State.batchDuplicatesFilter.writeState(&clickhouseDb)
		}
		batchNumber++
		// if the batch sizes are relatively consistent, we want to recycle slices,
		// if the difference between previous and current batch size is large enough
		// we want to re-allocate to avoid ever expanding memory usage
		recycleSlices := lastEventsInBatchCount-eventsInBatchCount < 10000
		eventsByTable.Reset(recycleSlices)
		Stats.Print()
		resetBatch()

		if !State.initiatedDump.Load() && *Config.StartFromGtid == "" && (delay < 10 || *Config.DumpImmediately) {
			State.initiatedDump.Store(true)
			go DumpMysqlDb(&clickhouseDb, *Config.Dump || *Config.DumpImmediately)
			log.Infoln("Started mysql db dump")
		}
	}

	for {
		select {
		case <-State.ctx.Done():
			log.Infoln("Closing...")
			return
		case <-timer.C:
			if eventsInBatchCount > 0 {
				deliver()
			} else {
				resetBatch()
			}
		case e := <-State.batchWrite:
			if e.EventAction != "dump" && State.batchDuplicatesFilter.TestAndAdd(e.EventId) {
				Stats.IncrementSkippedLocallyFilteredDuplicates()
				continue
			}

			if firstGtidInBatch == "" && !e.RawEvent.IsDumpEvent() {
				firstGtidInBatch = e.RawEvent.GtidRangeString()
			}

			if eventsByTable[e.EventTable] == nil {
				// saves some memory if a given table doesn't have large batch sizes
				// - since this slice is re-used any growth that happens only happens once
				eventSlice := make([]*RowInsertData, 0, *Config.BatchSize/20)
				eventsByTable[e.EventTable] = &eventSlice
			}

			events := append(*eventsByTable[e.EventTable], e)
			eventsByTable[e.EventTable] = &events

			if eventsInBatchCount == *Config.BatchSize {
				deliver()
			} else {
				eventsInBatchCount++
			}
		}
	}
}

func deliverBatch(clickhouseDb ClickhouseDb, eventsByTable EventsByTable, lastGtidSet string) {
	State.chColumns.Sync(clickhouseDb)

	var wg sync.WaitGroup
	working := make(chan bool, *Config.ConcurrentBatchWrites)

	for table, rows := range eventsByTable {
		_, hasColumns := State.chColumns.ColumnsForTable(table)
		if !hasColumns {
			continue
		}

		wg.Add(1)

		go func(table string, rows *[]*RowInsertData) {
			working <- true
			deliverBatchForTable(clickhouseDb, table, rows)
			<-working
			wg.Done()
		}(table, rows)
	}

	wg.Wait()

	if lastGtidSet != "" {
		go clickhouseDb.SetGTIDString(lastGtidSet)
	}
}

func initState(testing bool) {
	Stats.Init(testing)
	State.Init()
}

func startSync() {
	startProcessEventsWorkers()
	go batchWrite()

	// TODO how do we determine what should be dumped on a startup?
	// should it check a clickhouse table config to determine status?
	minimumStartingGtid := getEarliestGtidStartPoint()

	clickhouseDb := unwrap(establishClickhouseConnection())

	if *Config.Dump || *Config.DumpImmediately || *Config.Rewind {
		clickhouseDb.SetGTIDString(minimumStartingGtid)
	} else if *Config.StartFromGtid != "" {
		clickhouseDb.SetGTIDString(*Config.StartFromGtid)
	}

	retryCount := 0

	var err error
	var errWas error
	for retryCount < 5 {
		errWas = err
		err = startReplication(clickhouseDb.GetGTIDSet(minimumStartingGtid))

		if errWas == nil && err != nil {
			retryCount = 0
		}

		if err == context.Canceled {
			break
		} else {
			time.Sleep(1 * time.Second)
			retryCount++
		}
	}

	if err != context.Canceled {
		log.Fatal("Replication failed after 5 retries: ", err)
	}
}

func main() {
	Config.ParseFlags(os.Args[1:])
	initState(false)

	var p *profile.Profile
	if *Config.RunProfile {
		p = profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook).(*profile.Profile)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Infoln("Exiting...")

		if *Config.RunProfile && p != nil {
			p.Stop()
		}

		State.cancel()
		time.Sleep(2 * time.Second)

		os.Exit(1)
	}()

	go func() {
		log.Infoln("Now listening at :3003 for prometheus")
		must(http.ListenAndServe(":3003", promhttp.Handler()))
	}()

	startSync()
}
