package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/pkg/profile"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/maps"

	"sync"

	"github.com/siddontang/go-log/log"
)

type LookupMap map[string]bool

type RowData map[string]interface{}

type ClickhouseBatchColumns = []interface{}
type ClickhouseBatchColumnsByTable = map[string]ClickhouseBatchColumns
type ClickhouseBatchRow struct {
	InsertColumns ClickhouseBatchColumns
	Table         string
}

type RowInsertData struct {
	Id             int64
	EventTable     string
	EventCreatedAt time.Time
	EventAction    string
	EventId        string
	Event          RowData
}

func (d *RowInsertData) Reset() {
	maps.Clear(d.Event)
}

var RowInsertDataPool = sync.Pool{
	New: func() any {
		insertData := new(RowInsertData)
		insertData.Event = make(RowData, 20)
		return insertData
	},
}

func gtidRangeString(sid string, gtid int64) string {
	return sid + ":1-" + strconv.FormatInt(gtid, 10)
}

func processEventWorker(input <-chan *MysqlReplicationRowEvent, output chan<- *RowInsertData) {
	for {
		select {
		case <-State.ctx.Done():
			return
		case event := <-input:
			if event.Action != "dump" && event.ServerId != "" {
				State.latestProcessingGtid <- gtidRangeString(event.ServerId, event.Gtid)
			}

			columns, hasColumns := State.chColumns.ColumnsForTable(event.Table.Name)

			if !hasColumns {
				Stats.IncrementProcessed()
				continue
			}

			insertData, isDup := eventToClickhouseRowData(event, columns)

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

type IsDuplicate bool

// TODO test with all types we care about - yaml conversion, etc.
// dedupe for yaml columns according to filtered values?
func eventToClickhouseRowData(e *MysqlReplicationRowEvent, columns *ChColumnSet) (*RowInsertData, IsDuplicate) {
	insertData := RowInsertDataPool.Get().(*RowInsertData)
	insertData.Reset()

	var previousRow []interface{}
	tableName := e.Table.Name
	hasPreviousEvent := len(e.Rows) == 2

	newEventIdx := len(e.Rows) - 1

	if hasPreviousEvent {
		previousRow = e.Rows[0]
	}
	row := e.Rows[newEventIdx]

	isDuplicate := false
	if e.Action == "update" {
		isDuplicate = true
	}

	for i, c := range e.Table.Columns {
		columnName := c.Name
		if columns.columnLookup[columnName] {
			if isDuplicate &&
				hasPreviousEvent &&
				!memoizedRegexpsMatch(columnName, Config.IgnoredColumnsForDeduplication) &&
				!reflect.DeepEqual(row[i], previousRow[i]) {
				isDuplicate = false
			}

			convertedValue := parseValue(row[i], c.Type, tableName, columnName)
			insertData.Event[c.Name] = convertedValue
		}
	}

	var timestamp time.Time
	if !e.Timestamp.IsZero() {
		timestamp = e.Timestamp
	} else {
		timestamp = time.Now()
	}

	insertData.Event[eventCreatedAtColumnName] = timestamp

	if isDuplicate {
		return insertData, true
	}

	insertData.Event[actionColumnName] = e.Action

	var id int64
	maybeInt := reflect.ValueOf(insertData.Event["id"])
	if maybeInt.CanInt() {
		id = reflect.ValueOf(insertData.Event["id"]).Int()
	}

	insertData.Id = id
	var eventId string
	if e.Action != "dump" {
		eventId = e.EventId()
	} else {
		eventId = fmt.Sprintf("dump:%d#0", id)
	}

	insertData.Event[eventIdColumnName] = eventId
	insertData.EventId = eventId
	insertData.EventTable = tableName
	insertData.EventCreatedAt = timestamp
	insertData.EventAction = e.Action

	return insertData, false
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

	if minMaxValues.maxDumpId > 0 {
		hasStoredIds, storedIdsMap = clickhouseDb.QueryIdRange(tableWithDb,
			minMaxValues.minDumpId,
			minMaxValues.maxDumpId)
	}

	hasDuplicates, duplicatesMap := clickhouseDb.QueryDuplicates(tableWithDb,
		minMaxValues.minCreatedAt,
		minMaxValues.maxCreatedAt)

	batchColumnArrays := make(ClickhouseBatchColumns, len(chColumns))
	writeCount := 0

	for _, rowInsertData := range *processedRows {
		if rowInsertData.EventAction != "dump" && hasDuplicates && duplicatesMap.Contains(rowInsertData.EventId) {
			Stats.IncrementSkippedPersistedDuplicates()
			continue
		}

		if rowInsertData.EventCreatedAt.Year() == 1 {
			log.Panicln(rowInsertData)
		}

		if rowInsertData.EventAction == "dump" && hasStoredIds && storedIdsMap.Contains(rowInsertData.Id) {
			Stats.IncrementSkippedDumpDuplicates()
			continue
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

type minMaxValues struct {
	minCreatedAt time.Time
	maxCreatedAt time.Time
	minDumpId    int64
	maxDumpId    int64
}

func getMinMaxValues(rows *[]*RowInsertData) minMaxValues {
	minCreatedAt := time.Time{}
	maxCreatedAt := time.Time{}
	var minDumpId int64 = 0
	var maxDumpId int64 = 0

	for _, r := range *rows {
		// we only care about min and max ids of dump events
		// this is important since dump is unique in that ids
		// are sequential
		if r.EventAction == "dump" {
			if minDumpId == 0 {
				minDumpId = r.Id
			} else if minDumpId > r.Id {
				minDumpId = r.Id
			}

			if maxDumpId < r.Id {
				maxDumpId = r.Id
			}
		} else {
			comparingCreatedAt := r.EventCreatedAt

			if minCreatedAt.Year() == 1 {
				minCreatedAt = comparingCreatedAt
			} else if minCreatedAt.After(comparingCreatedAt) {
				minCreatedAt = comparingCreatedAt
			}

			if maxCreatedAt.Before(comparingCreatedAt) {
				maxCreatedAt = comparingCreatedAt
			}
		}
	}

	return minMaxValues{
		minCreatedAt: minCreatedAt,
		maxCreatedAt: maxCreatedAt,
		minDumpId:    minDumpId,
		maxDumpId:    maxDumpId,
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
	var lastGtidSet string
	var timer *time.Timer
	var lastEventsInBatchCount,
		eventsInBatchCount,
		batchNumber int

	resetCountAndTimer := func() {
		timer = time.NewTimer(*Config.BatchWriteInterval)
		lastEventsInBatchCount = eventsInBatchCount
		eventsInBatchCount = 0
	}

	resetCountAndTimer()

	deliver := func() {
		// snapshot every 60 seconds with default settings
		shouldSnapshotDuplicatesFilter := batchNumber%6 == 0

		if shouldSnapshotDuplicatesFilter {
			State.batchDuplicatesFilter.snapshotState()
		}

		// Could also get delay by peeking events periodicially to avoid computing it
		delay := replicationDelay.Load()
		log.Infoln("replication delay is", delay, "seconds")

		deliverBatch(clickhouseDb, eventsByTable, lastGtidSet)

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
		resetCountAndTimer()

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
				resetCountAndTimer()
			}
		case gtid := <-State.latestProcessingGtid:
			lastGtidSet = gtid
		case e := <-State.batchWrite:
			if e.EventAction != "dump" && State.batchDuplicatesFilter.TestAndAdd(e.EventId) {
				Stats.IncrementSkippedLocallyFilteredDuplicates()
				continue
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

	err := startReplication(clickhouseDb.GetGTIDSet(minimumStartingGtid))
	if err != context.Canceled {
		log.Panicln(err)
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
