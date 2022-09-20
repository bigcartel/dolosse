package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/peterbourgon/ff/v3"
	"github.com/pkg/profile"
	boom "github.com/tylertreat/BoomFilters"

	"sync"

	"github.com/siddontang/go-log/log"
)

var ignoredColumnsForDeduplication = map[string]bool{
	"updated_at": true,
}

var clickhouseDb,
	clickhouseAddr,
	mysqlAddr,
	mysqlUser,
	mysqlPassword,
	mysqlDb *string

var forceImmediateDump *bool

var mysqlDbByte []byte

type ChColumnSet struct {
	columns      []ClickhouseQueryColumn
	columnLookup LookupMap
}

// TODO if columns are stale and we get an error the processed data for the batch would need to
// be re-processed or we just restart replication from last gtid set? Maybe that's the move -
// if it fails due to a schema change just restart from a known good point. Fail early and retry Erlang style.
var chColumns = NewConcurrentMap[ChColumnSet]()

// The Inverse Bloom Filter may report a false negative but can never report a false positive.
// behaves a bit like a fixed size hash map that doesn't handle conflicts
var batchDuplicatesFilter = boom.NewInverseBloomFilter(1000000)
var batchSize = 100000
var concurrentBatchWrites = 10
var concurrentMysqlDumpSelects = 10
var processRowsChannel = make(chan *MysqlReplicationRowEvent, batchSize)
var batchWriteChannel = make(chan *RowInsertData, batchSize)
var latestProcessingGtid = make(chan string)
var deliveredRows = new(uint64)
var enqueuedRows = new(uint64)
var processedRows = new(uint64)
var skippedRowLevelDuplicates = new(uint64)
var skippedBatchLevelDuplicates = new(uint64)
var skippedPersistedDuplicates = new(uint64)
var skippedDumpDuplicates = new(uint64)
var dumping = atomic.Bool{}
var shouldDump = atomic.Bool{} // TODO set to true if no gtid key is found in clickhouse? that way it always dumps on first run

type LookupMap map[string]bool

type RowData map[string]interface{}

type ClickhouseBatchColumns = []interface{}
type ClickhouseBatchColumnsByTable = map[string]ClickhouseBatchColumns
type ClickhouseBatchRow struct {
	InsertColumns ClickhouseBatchColumns
	Table         string
}

type RowInsertData struct {
	Id               int64
	EventTable       string
	EventCreatedAt   time.Time
	EventChecksum    uint64
	EventAction      string
	DeduplicationKey string
	Event            RowData
}

func (d *RowInsertData) Reset() {
	for k := range d.Event {
		delete(d.Event, k)
	}
}

var RowInsertDataPool = sync.Pool{
	New: func() any {
		insertData := new(RowInsertData)
		insertData.Event = make(RowData, 20)
		return insertData
	},
}

type EventsByTable map[string]*[]*RowInsertData

func (es *EventsByTable) Reset() {
	for k, vSlice := range *es {
		for _, v := range *vSlice {
			RowInsertDataPool.Put(v)
		}
		newSlice := (*vSlice)[:0]
		(*es)[k] = &newSlice
	}
}

func syncChColumns(clickhouseDb ClickhouseDb) {
	existingClickhouseTables := clickhouseDb.ColumnsForMysqlTables()

	for table := range existingClickhouseTables {
		cols, lookup := clickhouseDb.Columns(table)

		chColumns.set(table, &ChColumnSet{
			columns:      cols,
			columnLookup: lookup,
		})
	}
}

func printStats() {
	log.Infoln(*processedRows, "processed rows")
	log.Infoln(*enqueuedRows, "enqueued rows")
	log.Infoln(*deliveredRows, "delivered rows")
	log.Infoln(*skippedRowLevelDuplicates, "skipped row level duplicate rows")
	log.Infoln(*skippedBatchLevelDuplicates, "skipped batch level duplicate rows")
	log.Infoln(*skippedPersistedDuplicates, "skipped persisted duplicate rows")
	log.Infoln(*skippedDumpDuplicates, "skipped dump duplicate rows")
}

func incrementStat(counter *uint64) {
	atomic.AddUint64(counter, 1)
}

func checkErr(err error) {
	if err != nil {
		log.Panicln(err)
	}
}

func cachedChColumnsForTable(table string) (*ChColumnSet, bool) {
	columns := chColumns.get(table)
	return columns, (columns != nil && len(columns.columns) > 0)
}

func processEventWorker(input <-chan *MysqlReplicationRowEvent, output chan<- *RowInsertData) {
	for event := range input {
		if event.Gtid != "" {
			latestProcessingGtid <- event.Gtid
		}

		columns, hasColumns := cachedChColumnsForTable(event.Table.Name)

		if !hasColumns {
			incrementStat(processedRows)
			continue
		}

		insertData, isDup := eventToClickhouseRowData(event, columns)

		if isDup {
			incrementStat(skippedRowLevelDuplicates)
		} else if event.Action != "dump" && batchDuplicatesFilter.TestAndAdd([]byte(insertData.DeduplicationKey)) {
			incrementStat(skippedBatchLevelDuplicates)
		} else {
			output <- insertData
		}

		incrementStat(processedRows)
	}
}

func startProcessEventsWorkers() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go processEventWorker(processRowsChannel, batchWriteChannel)
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
				!ignoredColumnsForDeduplication[columnName] &&
				!reflect.DeepEqual(row[i], previousRow[i]) {
				isDuplicate = false
			}

			convertedValue := parseValue(row[i], c.Type, tableName, columnName)
			insertData.Event[c.Name] = convertedValue
		}
	}

	// make this use updated_at if it exists on the row?
	timestamp := time.Now()
	if e.Timestamp > 0 {
		timestamp = time.Unix(int64(e.Timestamp), 0)
	}

	insertData.Event[eventCreatedAtColumnName] = timestamp

	if isDuplicate {
		return insertData, true
	}

	checksum := checksumMapValues(insertData.Event, columns.columns)
	insertData.Event[actionColumnName] = e.Action

	var id int64
	maybeInt := reflect.ValueOf(insertData.Event["id"])
	if maybeInt.CanInt() {
		id = reflect.ValueOf(insertData.Event["id"]).Int()
	}

	insertData.Id = id
	insertData.EventTable = tableName
	insertData.EventCreatedAt = timestamp
	insertData.EventChecksum = checksum
	insertData.EventAction = e.Action
	insertData.DeduplicationKey = dupIdString(id, checksum)

	return insertData, false
}

func OnRow(e *MysqlReplicationRowEvent) {
	processRowsChannel <- e
	incrementStat(enqueuedRows)
}

func tableWithDb(table string) string {
	return fmt.Sprintf("%s.%s", *clickhouseDb, table)
}

func dupIdString(id interface{}, checksum interface{}) string {
	return fmt.Sprintf("%d %d", id, checksum)
}

func deliverBatch(clickhouseDb ClickhouseDb, eventsByTable EventsByTable, lastGtidSet string) {
	syncChColumns(clickhouseDb)

	var wg sync.WaitGroup
	working := make(chan bool, concurrentBatchWrites)

	for table, rows := range eventsByTable {
		_, hasColumns := cachedChColumnsForTable(table)
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
		clickhouseDb.SetGTIDString(lastGtidSet)
	}
}

func logNoRows(table string) {
	log.Infoln("no rows to send for", table, "skipping...")
}

func deliverBatchForTable(clickhouseDb ClickhouseDb, table string, rows *[]*RowInsertData) {
	columns := chColumns.get(table)
	if columns == nil {
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

	atomic.AddUint64(deliveredRows, uint64(writeCount))

	log.Infoln("batch sent for", tableWithDb)
}

func buildClickhouseBatchRows(clickhouseDb ClickhouseDb, tableWithDb string, processedRows *[]*RowInsertData, chColumns []ClickhouseQueryColumn) (ClickhouseBatchColumns, int) {

	minMaxValues := getMinMaxValues(processedRows)

	hasStoredIds := false
	var storedIdsMap map[int64]bool

	if dumping.Load() && minMaxValues.maxDumpId > 0 {
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
		dedupeKey := rowInsertData.DeduplicationKey

		if hasDuplicates && duplicatesMap[dedupeKey] {
			incrementStat(skippedPersistedDuplicates)
			continue
		}

		if rowInsertData.EventCreatedAt.Year() == 1 {
			log.Panicln(rowInsertData)
		}

		if dumping.Load() && rowInsertData.EventAction == "dump" && hasStoredIds && storedIdsMap[rowInsertData.Id] {
			incrementStat(skippedDumpDuplicates)
			continue
		}

		writeCount++

		for i, col := range chColumns {
			val := rowInsertData.Event[col.Name]
			newColumnAry, err := reflectAppend(col.Type, batchColumnArrays[i], val)

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
	batch, err := clickhouseDb.conn.PrepareBatch(context.Background(),
		fmt.Sprintf("INSERT INTO %s (%s)", tableWithDb, commaSeparatedColumnNames(chColumns)))

	checkErr(err)

	for i, col := range batchColumnArrays {
		err := batch.Column(i).Append(col)
		if err != nil {
			log.Panicln(err, tableWithDb, col)
		}
	}

	err = batch.Send()
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

func batchWrite() {
	clickhouseDb := establishClickhouseConnection()

	timer := time.NewTimer(10 * time.Second)
	eventsByTable := make(EventsByTable)
	var lastGtidSet string
	counter := 0

	deliver := func() {
		// Could also get delay by peeking events periodicially to avoid computing it
		log.Infoln("replication delay is", replicationDelay.Load(), "seconds")
		if shouldDump.Load() && (replicationDelay.Load() < 10 || *forceImmediateDump) {
			shouldDump.Store(false)
			go DumpMysqlDb()
			log.Infoln("Started mysql db dump")
		}

		deliverBatch(clickhouseDb, eventsByTable, lastGtidSet)
		eventsByTable.Reset()
		printStats()
		timer = time.NewTimer(10 * time.Second)
		counter = 0
	}

	for {
		select {
		case <-timer.C:
			deliver()
		case gtid := <-latestProcessingGtid:
			lastGtidSet = gtid
		case e := <-batchWriteChannel:
			if eventsByTable[e.EventTable] == nil {
				// saves some memory if a given table doesn't have large batch sizes
				// - since this slice is re-used any growth that happens only happens once
				eventSlice := make([]*RowInsertData, 0, batchSize/20)
				eventsByTable[e.EventTable] = &eventSlice
			}

			events := append(*eventsByTable[e.EventTable], e)
			eventsByTable[e.EventTable] = &events

			if counter == batchSize {
				deliver()
			} else {
				counter++
			}
		}
	}
}

func main() {
	fs := flag.NewFlagSet("MySQL -> Clickhouse binlog replicator", flag.ContinueOnError)

	var (
		forceDump     = fs.Bool("force-dump", false, "Reset stored binlog position and start mysql data dump after replication has caught up to present")
		runProfile    = fs.Bool("profile", false, "Outputs pprof profile to cpu.pprof for performance analysis")
		startFromGtid = fs.String("start-from-gtid", "", "Start from gtid set")
		_             = fs.String("config", "", "config file (optional)")
	)

	forceImmediateDump = fs.Bool("force-immediate-dump", false, "Force full immediate data dump and reset stored binlog position")
	mysqlDb = fs.String("mysql-db", "bigcartel", "mysql db to dump (also available via MYSQL_DB")
	mysqlAddr = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db (also via MYSQL_ADDR)")
	mysqlUser = fs.String("mysql-user", "metabase", "mysql user (also via MYSQL_USER)")
	mysqlPassword = fs.String("mysql-password", "password", "mysql password (also via MYSQL_PASSWORD)")
	clickhouseAddr = fs.String("clickhouse-addr", "10.100.0.56:9000", "ip/url and port for destination clickhouse db (also via CLICKHOUSE_ADDR)")
	clickhouseDb = fs.String("clickhouse-db", "mysql_bigcartel_binlog", "db to write binlog data to (also available via CLICKHOUSE_DB)")
	// TODO make batch count and delivery timeout configurable

	err := ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)

	if err != nil {
		log.Fatal(err)
	}

	var p *profile.Profile
	if *runProfile {
		p = profile.Start(profile.TraceProfile, profile.ProfilePath("."), profile.NoShutdownHook).(*profile.Profile)
	}

	mysqlDbByte = []byte(*mysqlDb)

	clickhouseDb := establishClickhouseConnection()
	clickhouseDb.Setup()
	initMysqlConnectionPool()

	// TODO validate all clickhouse table columns are compatible with mysql table columns
	// and that clickhouse tables have event_created_at DateTime, id same_as_mysql, action string
	// use the clickhouse mysql table function with the credentials provided to this command
	// to do it using clickhouse translated types for max compat
	clickhouseDb.CheckSchema()

	syncChColumns(clickhouseDb)

	startProcessEventsWorkers()
	go batchWrite()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		if *runProfile && p != nil {
			p.Stop()
		}

		os.Exit(1)
	}()

	// TODO how do we determine what should be dumped on a startup?
	// should it check a clickhouse table config to determine status?
	minimumStartingGtid := getEarliestGtidStartPoint()

	if *forceDump || *forceImmediateDump {
		shouldDump.Store(true)
		clickhouseDb.SetGTIDString(minimumStartingGtid)
		startReplication(clickhouseDb.GetGTIDSet(minimumStartingGtid))
	} else {
		if *startFromGtid != "" {
			clickhouseDb.SetGTIDString(*startFromGtid)
		}

		startReplication(clickhouseDb.GetGTIDSet(minimumStartingGtid))
	}
}
