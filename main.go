package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/xxhash"
	"github.com/peterbourgon/ff/v3"
	boom "github.com/tylertreat/BoomFilters"

	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/siddontang/go-log/log"
)

var ignoredColumnsForDeduplication = map[string]bool{
	"updated_at": true,
}

// TODO either move to config or infer from string text if a config is set to true?
// could this also pick what fields I want to extract?
// Could this also work with json columns/ruby serialized columns in a generic way?
var yamlColumns = map[string]map[string]bool{
	"theme_instances": {
		"settings":          true,
		"images_sort_order": true,
	},
	"order_transactions": {
		"params": true,
	},
}

// could this be just a giant regexp?
// ugly but easier to make fast
// or a per-table regexp?
// TODO flesh this out
var anonymizeFields = regexp.MustCompile(".*(address|street|password|salt|email|longitude|latitude).*|payment_methods.properties|products.description")

var clickhouseDb *string
var clickhouseAddr *string
var mysqlDb *string

// TODO use this for global cache of clickhouse columns and update before each batch
type ChColumnSet struct {
	columns      []ClickhouseQueryColumn
	columnLookup LookupMap
}

// TODO if columns are stale and we get an error the processed data for the batch would need to be re-processed or we just restart replication from last gtid set? Maybe that's the move - if it fails due to a schema change just restart from a known good point. Fail early and retry Erlang style.
var chColumns = NewConcurrentMap[ChColumnSet]()

// The Inverse Bloom Filter may report a false negative but can never report a false positive.
// behaves a bit like a fixed size hash map that doesn't handle conflicts
var batchDuplicatesFilter = boom.NewInverseBloomFilter(1000000)

var processRowsChannel = make(chan RowEvent, 10000)
var batchWriteChannel = make(chan RowInsertData, 10000)
var latestProcessingGtid = make(chan string)
var syncCanal *canal.Canal
var deliveredRows = new(uint64)
var enqueuedRows = new(uint64)
var processedRows = new(uint64)
var skippedRowLevelDuplicates = new(uint64)
var skippedBatchLevelDuplicates = new(uint64)
var skippedPersistedDuplicates = new(uint64)

type ProcessRow struct {
	canal.DummyEventHandler
}

type RowEvent struct {
	e                 *canal.RowsEvent
	lastSyncedGtidSet string
}

type RowInsertData struct {
	EventTable     string
	EventCreatedAt time.Time
	EventChecksum  uint64
	Event          RowData
}

type EventsByTable map[string][]RowInsertData

type LookupMap map[string]bool

type RowData map[string]interface{}

type ClickhouseBatchColumns = []interface{}
type ClickhouseBatchColumnsByTable = map[string]ClickhouseBatchColumns
type ClickhouseBatchRow struct {
	InsertColumns ClickhouseBatchColumns
	Table         string
}

func syncChColumns(clickhouseDb ClickhouseDb) {
	existingClickhouseTables := clickhouseDb.ColumnsForMysqlTables(syncCanal)

	for table := range existingClickhouseTables {
		cols, lookup := clickhouseDb.Columns(table)

		chColumns.set(table, &ChColumnSet{
			columns:      cols,
			columnLookup: lookup,
		})
	}
}

func printStats() {
	log.Infoln(*deliveredRows, "delivered rows")
	log.Infoln(*enqueuedRows, "enqueued rows")
	log.Infoln(*processedRows, "processed rows")
	log.Infoln(*skippedRowLevelDuplicates, "skipped row level duplicate rows")
	log.Infoln(*skippedBatchLevelDuplicates, "skipped batch level duplicate rows")
	log.Infoln(*skippedPersistedDuplicates, "skipped persisted duplicate rows")
}

func incrementStat(counter *uint64) {
	atomic.AddUint64(counter, 1)
}

func (r RowInsertData) eventDeduplicationKey() string {
	return dupIdString(r.Event["id"], r.EventChecksum)
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

func processEventWorker(input <-chan RowEvent, output chan<- RowInsertData) {
	for event := range input {
		latestProcessingGtid <- event.lastSyncedGtidSet
		columns, hasColumns := cachedChColumnsForTable(event.e.Table.Name)

		if !hasColumns {
			incrementStat(processedRows)
			continue
		}

		insertData, isDup := eventToClickhouseRowData(event.e, columns.columnLookup)

		if isDup {
			incrementStat(skippedRowLevelDuplicates)
		} else if batchDuplicatesFilter.TestAndAdd([]byte(insertData.eventDeduplicationKey())) {
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

// TODO use for yaml too
func anonymizeValue(table string, columnPath string, value interface{}) interface{} {
	fieldString := fmt.Sprintf("%s.%s", table, columnPath)

	if anonymizeFields.Match([]byte(fieldString)) {
		switch v := value.(type) {
		case string:
			return fmt.Sprint(xxhash.Sum64String(v))
		}
	}

	return value
}

func eventToClickhouseRowData(e *canal.RowsEvent, columnLookup LookupMap) (RowInsertData, bool) {
	var previousRow []interface{}
	tableName := e.Table.Name
	hasPreviousEvent := len(e.Rows) == 2

	newEventIdx := len(e.Rows) - 1
	Data := make(RowData, len(e.Rows[newEventIdx]))
	if hasPreviousEvent {
		previousRow = e.Rows[0]
	}
	row := e.Rows[newEventIdx]

	isDuplicate := true

	for i, c := range e.Table.Columns {
		columnName := c.Name
		if columnLookup[columnName] {
			convertedValue := convertMysqlValue(&c, parseValue(row[i], tableName, columnName))
			Data[c.Name] = anonymizeValue(tableName, columnName, convertedValue)
			if isDuplicate &&
				hasPreviousEvent &&
				!ignoredColumnsForDeduplication[columnName] &&
				(convertedValue != convertMysqlValue(&c, parseValue(previousRow[i], tableName, columnName))) {
				isDuplicate = false
			}
		}
	}

	if isDuplicate {
		return RowInsertData{}, true
	}

	timestamp := time.Now()

	if e.Header != nil {
		timestamp = time.Unix(int64(e.Header.Timestamp), 0)
	} else {
		log.Infoln("Event header is nil, using now as event created at")
	}

	Data[eventCreatedAtColumnName] = timestamp

	checksum := checksumMapValues(Data)

	Data[actionColumnName] = e.Action
	Data[checksumColumnName] = checksum

	return RowInsertData{
		EventTable:     tableName,
		EventCreatedAt: timestamp,
		EventChecksum:  checksum,
		Event:          Data,
	}, false
}

func (h *ProcessRow) OnRow(e *canal.RowsEvent) error {
	_, hasColumns := cachedChColumnsForTable(e.Table.Name)

	if hasColumns {
		processRowsChannel <- RowEvent{
			e:                 e,
			lastSyncedGtidSet: syncCanal.SyncedGTIDSet().String(),
		}

		incrementStat(enqueuedRows)
	}

	return nil
}

func tableWithDb(table string) string {
	return fmt.Sprintf("%s.%s", *clickhouseDb, table)
}

func dupIdString(id interface{}, checksum interface{}) string {
	return fmt.Sprintf("id: %d %s: %v", id, checksumColumnName, checksum)
}

func deliverBatch(clickhouseDb ClickhouseDb, eventsByTable EventsByTable, lastGtidSet string) {
	syncChColumns(clickhouseDb)

	var wg sync.WaitGroup

	for table, rows := range eventsByTable {
		_, hasColumns := cachedChColumnsForTable(table)
		if !hasColumns {
			continue
		}

		wg.Add(1)

		go func(table string, rows []RowInsertData) {
			deliverBatchForTable(clickhouseDb, table, rows)
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

func deliverBatchForTable(clickhouseDb ClickhouseDb, table string, rows []RowInsertData) {
	columns := chColumns.get(table)
	if columns == nil {
		log.Infoln("No columns in clickhouse found for", table)
		return
	}

	if len(rows) == 0 {
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

func buildClickhouseBatchRows(clickhouseDb ClickhouseDb, tableWithDb string, processedRows []RowInsertData, chColumns []ClickhouseQueryColumn) (ClickhouseBatchColumns, int) {

	minCreatedAt, maxCreatedAt := getMinMaxCreatedAt(processedRows)
	hasDuplicates, duplicatesMap := clickhouseDb.QueryDuplicates(tableWithDb,
		minCreatedAt,
		maxCreatedAt)

	batchColumnArrays := make(ClickhouseBatchColumns, len(chColumns))
	writeCount := 0

	for _, rowInsertData := range processedRows {
		dedupeKey := rowInsertData.eventDeduplicationKey()

		if hasDuplicates && duplicatesMap[dedupeKey] {
			incrementStat(skippedPersistedDuplicates)
			continue
		}

		if rowInsertData.EventCreatedAt.Year() == 1 {
			log.Panicln(rowInsertData)
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

	checkErr(batch.Send())
}

func getMinMaxCreatedAt(rows []RowInsertData) (time.Time, time.Time) {
	minCreatedAt := time.Time{}
	maxCreatedAt := time.Time{}

	for _, r := range rows {
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

	return minCreatedAt, maxCreatedAt
}

func batchWrite() {
	clickhouseDb := establishClickhouseConnection()

	timer := time.NewTimer(10 * time.Second)
	eventsByTable := make(EventsByTable)
	var lastGtidSet string
	counter := 0

	deliver := func() {
		deliverBatch(clickhouseDb, eventsByTable, lastGtidSet)
		printStats()
		timer = time.NewTimer(10 * time.Second)
		eventsByTable = make(EventsByTable)
		counter = 0
	}

	for {
		select {
		case <-timer.C:
			deliver()
		case gtid := <-latestProcessingGtid:
			lastGtidSet = gtid
		case e := <-batchWriteChannel:
			eventsByTable[e.EventTable] = append(eventsByTable[e.EventTable], e)

			if counter == 100000 {
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
		mysqlAddr     = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db (also via MYSQL_ADDR)")
		mysqlUser     = fs.String("mysql-user", "metabase", "mysql user (also via MYSQL_USER)")
		mysqlPassword = fs.String("mysql-password", "password", "mysql password (also via MYSQL_PASSWORD)")
		forceDump     = fs.Bool("force-dump", false, "Force full data dump and reset stored binlog position")
		startFromGtid = fs.String("start-from-gtid", "", "Start from gtid set")
		_             = fs.String("config", "", "config file (optional)")
	)

	mysqlDb = fs.String("mysql-db", "bigcartel", "mysql db to dump (also available via MYSQL_DB")
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

	cfg := canal.NewDefaultConfig()
	cfg.Addr = *mysqlAddr
	cfg.User = *mysqlUser
	cfg.Password = *mysqlPassword
	cfg.Dump.TableDB = *mysqlDb
	cfg.Dump.Tables = []string{"plans"}
	log.Infoln(fmt.Sprintf("%s.*", *mysqlDb))
	cfg.IncludeTableRegex = []string{fmt.Sprintf("%s..*", *mysqlDb)}

	if err != nil {
		log.Fatal(err)
	}

	clickhouseDb := establishClickhouseConnection()
	clickhouseDb.Setup()
	// TODO validate all clickhouse table columns are compatible with mysql table columns
	// and that clickhouse tables have event_created_at DateTime, id same_as_mysql, action string
	// use the clickhouse mysql table function with the credentials provided to this command
	// to do it using clickhouse translated types for max compat

	syncCanal, err = canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	clickhouseDb.CheckSchema(syncCanal)

	syncCanal.SetEventHandler(&ProcessRow{})

	syncChColumns(clickhouseDb)

	startProcessEventsWorkers()
	go batchWrite()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		close(processRowsChannel)
		close(batchWriteChannel)
		close(latestProcessingGtid)
		log.Infoln(syncCanal.SyncedGTIDSet())
		os.Exit(1)
	}()

	if *forceDump {
		clickhouseDb.SetGTIDString("")
		checkErr(syncCanal.Run())
	} else {
		if *startFromGtid != "" {
			clickhouseDb.SetGTIDString(*startFromGtid)
		}

		checkErr(syncCanal.StartFromGTID(clickhouseDb.GetGTIDSet()))
	}
}
