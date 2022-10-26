package app

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"bigcartel/dolosse/clickhouse"
	"bigcartel/dolosse/clickhouse/cached_columns"
	"bigcartel/dolosse/config"
	"bigcartel/dolosse/err_utils"
	"bigcartel/dolosse/mysql"
	"bigcartel/dolosse/reflect_utils"
	"bigcartel/dolosse/set"

	"sync"

	"github.com/siddontang/go-log/log"
)

type ClickhouseBatchColumns = []interface{}
type ClickhouseBatchColumnsByTable = map[string]ClickhouseBatchColumns
type ClickhouseBatchRow struct {
	InsertColumns ClickhouseBatchColumns
	Table         string
}

type App struct {
	Ctx             context.Context
	Shutdown        context.CancelFunc
	Mysql           mysql.Mysql
	ProcessRows     chan mysql.MysqlReplicationRowEvent
	BatchWrite      chan mysql.MysqlReplicationRowEvent
	EventTranslator mysql.EventTranslator
	Stats           *Stats
	Config          config.Config
	ChColumns       *cached_columns.ChColumns
}

func (app App) processEventWorker() {
	for {
		select {
		case <-app.Ctx.Done():
			return
		case event := <-app.ProcessRows:
			columns, hasColumns := app.ChColumns.ColumnsForTable(event.Table.Name)

			if !hasColumns {
				app.Stats.IncrementProcessed()
				continue
			}

			isDup, isColumnMismatch := app.EventTranslator.PopulateInsertData(&event, columns)

			if isDup {
				app.Stats.IncrementSkippedRowLevelDuplicates()
			} else if isColumnMismatch {
				app.Stats.IncrementSkippedColumnMismatch()
			} else {
				app.BatchWrite <- event
			}

			app.Stats.IncrementProcessed()
		}
	}
}

func (app App) startProcessEventsWorkers() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go app.processEventWorker()
	}
}

func (app App) OnRow(e mysql.MysqlReplicationRowEvent) {
	app.ProcessRows <- e
	app.Stats.IncrementEnqueued()
}

func (app App) tableWithDb(table string) string {
	return fmt.Sprintf("%s.%s", *app.Config.ClickhouseDb, table)
}

func logNoRows(table string) {
	log.Infoln("no rows to send for", table, "skipping...")
}

func (app App) deliverBatchForTable(clickhouseDb clickhouse.ClickhouseDb, table string, rows []mysql.MysqlReplicationRowEvent) {
	columns, hasColumns := app.ChColumns.ColumnsForTable(table)

	if !hasColumns {
		log.Infoln("No columns in clickhouse found for", table)
		return
	}

	if len(rows) == 0 {
		logNoRows(table)
		return
	}

	batchColumnArrays, writeCount := app.buildClickhouseBatchRows(clickhouseDb, table, rows, columns.Columns)

	if writeCount == 0 {
		logNoRows(table)
		return
	}

	log.Infof("sending batch of %d records for %s", writeCount, table)

	app.sendClickhouseBatch(clickhouseDb, table, batchColumnArrays, columns.Columns)

	app.Stats.AddDelivered(uint64(writeCount))

	log.Infof("batch sent for %s.%s", *app.Config.MysqlDb, table)
}

func (app App) buildClickhouseBatchRows(clickhouseDb clickhouse.ClickhouseDb, table string, processedRows []mysql.MysqlReplicationRowEvent, chColumns []cached_columns.ClickhouseQueryColumn) (ClickhouseBatchColumns, int) {
	minMaxValues := mysql.GetMinMaxValues(processedRows)

	tableWithDb := app.tableWithDb(table)

	hasStoredIds := false
	var storedPksMap set.Set[string]

	if minMaxValues.MinDumpPks != nil {
		hasStoredIds, storedPksMap = clickhouseDb.QueryIdRange(tableWithDb,
			minMaxValues.MinDumpPks,
			minMaxValues.MaxDumpPks)
	}

	hasDuplicates, duplicatesMap := clickhouseDb.QueryDuplicates(tableWithDb, minMaxValues)

	batchColumnArrays := make(ClickhouseBatchColumns, len(chColumns))
	writeCount := 0

	for _, event := range processedRows {
		if event.Action == "dump" && hasStoredIds && storedPksMap.Contains(event.PkString()) {
			app.Stats.IncrementSkippedDumpDuplicates()
			continue
		}

		if hasDuplicates && duplicatesMap.Contains(event.DedupeKey()) {
			if event.Action != "dump" {
				app.Stats.IncrementSkippedPersistedDuplicates()
			} else {
				app.Stats.IncrementSkippedDumpDuplicates()
			}

			continue
		}

		if event.Timestamp.Year() == 1 {
			log.Panicln(event)
		}

		writeCount++

		for i, col := range chColumns {
			val := event.InsertData[col.Name]
			newColumnAry, err := reflect_utils.ReflectAppend(col.Type, batchColumnArrays[i], val, len(processedRows))

			if err != nil {
				log.Infoln(err)
				log.Infoln("Column:", col.Name)
				log.Fatalf("Event struct: %+v\n", event)
			}

			batchColumnArrays[i] = newColumnAry
		}
	}

	return batchColumnArrays, writeCount
}

func commaSeparatedColumnNames(columns []cached_columns.ClickhouseQueryColumn) string {
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

func (app App) sendClickhouseBatch(clickhouseDb clickhouse.ClickhouseDb, table string, batchColumnArrays ClickhouseBatchColumns, chColumns []cached_columns.ClickhouseQueryColumn) {
	tableWithDb := app.tableWithDb(table)

	batch := err_utils.Unwrap(clickhouseDb.Conn.PrepareBatch(context.Background(),
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

type EventsByTable map[string][]mysql.MysqlReplicationRowEvent

func (es *EventsByTable) Reset(recycleSlices bool) {
	if recycleSlices {
		for k, vSlice := range *es {
			newSlice := (vSlice)[:0]
			(*es)[k] = newSlice
		}
	}
}

func (app App) ClickhouseConn() clickhouse.ClickhouseDb {
	return err_utils.Unwrap(clickhouse.EstablishClickhouseConnection(
		app.Ctx,
		clickhouse.Config{
			Address:  *app.Config.ClickhouseAddr,
			Username: *app.Config.ClickhouseUsername,
			Password: *app.Config.ClickhousePassword,
			DbName:   *app.Config.ClickhouseDb,
		}))
}

func (app App) batchWrite() {
	clickhouseDb := app.ClickhouseConn()

	eventsByTable := make(EventsByTable)
	var firstGtidInBatch string
	var timer *time.Timer
	var lastEventsInBatchCount,
		eventsInBatchCount,
		batchNumber int

	resetBatch := func() {
		firstGtidInBatch = ""
		timer = time.NewTimer(*app.Config.BatchWriteInterval)
		lastEventsInBatchCount = eventsInBatchCount
		eventsInBatchCount = 0
	}

	resetBatch()

	deliver := func() {

		// Could also get delay by peeking events periodicially to avoid computing it
		delay := mysql.ReplicationDelay.Load()
		log.Infoln("replication delay is", delay, "seconds")

		app.deliverBatch(clickhouseDb, eventsByTable, firstGtidInBatch)
		batchNumber++
		// if the batch sizes are relatively consistent, we want to recycle slices,
		// if the difference between previous and current batch size is large enough
		// we want to re-allocate to avoid ever expanding memory usage
		recycleSlices := lastEventsInBatchCount-eventsInBatchCount < 10000
		eventsByTable.Reset(recycleSlices)
		app.Stats.Print()
		resetBatch()

		if !app.Mysql.InitiatedDump.Load() && *app.Config.StartFromGtid == "" && (delay < 10 || *app.Config.DumpImmediately) {
			app.Mysql.InitiatedDump.Store(true)
			go app.Mysql.DumpMysqlDb(&clickhouseDb, *app.Config.Dump || *app.Config.DumpImmediately, app.OnRow)
			log.Infoln("Started mysql db dump")
		}
	}

	for {
		select {
		case <-app.Ctx.Done():
			log.Infoln("Closing...")
			return
		case <-timer.C:
			if eventsInBatchCount > 0 {
				deliver()
			} else {
				resetBatch()
			}
		case e := <-app.BatchWrite:
			if firstGtidInBatch == "" && !e.IsDumpEvent() {
				firstGtidInBatch = e.GtidRangeString()
			}

			if eventsByTable[e.Table.Name] == nil {
				// saves some memory if a given table doesn't have large batch sizes
				// - since this slice is re-used any growth that happens only happens once
				eventSlice := make([]mysql.MysqlReplicationRowEvent, 0, *app.Config.BatchSize/20)
				eventsByTable[e.Table.Name] = eventSlice
			}

			events := append(eventsByTable[e.Table.Name], e)
			eventsByTable[e.Table.Name] = events

			if eventsInBatchCount == *app.Config.BatchSize {
				deliver()
			} else {
				eventsInBatchCount++
			}
		}
	}
}

func (app App) deliverBatch(clickhouseDb clickhouse.ClickhouseDb, eventsByTable EventsByTable, lastGtidSet string) {
	app.ChColumns.Sync(&clickhouseDb, app.Mysql)

	var wg sync.WaitGroup
	working := make(chan bool, *app.Config.ConcurrentBatchWrites)

	for table, rows := range eventsByTable {
		_, hasColumns := app.ChColumns.ColumnsForTable(table)
		if !hasColumns {
			continue
		}

		wg.Add(1)

		go func(table string, rows []mysql.MysqlReplicationRowEvent) {
			working <- true
			app.deliverBatchForTable(clickhouseDb, table, rows)
			<-working
			wg.Done()
		}(table, rows)
	}

	wg.Wait()

	if lastGtidSet != "" {
		go clickhouseDb.SetGTIDString(lastGtidSet)
	}
}

// TODO rename to InitSchema
func (app App) InitState(testing bool) {
	clickhouseDb := app.ClickhouseConn()
	clickhouseDb.Setup()

	// TODO validate all clickhouse table columns are compatible with mysql table columns
	// and that clickhouse tables have event_created_at DateTime, id same_as_mysql, action string
	// use the clickhouse mysql table function with the credentials provided to this command
	// to do it using clickhouse translated types for max compat
	clickhouseDb.CheckSchema(app.Mysql)
	app.ChColumns.Sync(&clickhouseDb, app.Mysql)
}

func (app App) StartSync() {
	app.startProcessEventsWorkers()
	go app.batchWrite()

	// TODO how do we determine what should be dumped on a startup?
	// should it check a clickhouse table config to determine status?
	minimumStartingGtid := app.Mysql.GetEarliestGtidStartPoint()

	clickhouseDb := app.ClickhouseConn()

	if *app.Config.Dump || *app.Config.DumpImmediately || *app.Config.Rewind {
		clickhouseDb.SetGTIDString(minimumStartingGtid)
	} else if *app.Config.StartFromGtid != "" {
		clickhouseDb.SetGTIDString(*app.Config.StartFromGtid)
	}

	retryCount := 0

	var err error
	var errWas error
	for retryCount < 5 {
		errWas = err
		err = app.Mysql.StartReplication(clickhouseDb.GetGTIDSet(minimumStartingGtid), app.OnRow)

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

func NewApp(testing bool, flags []string) App {
	ctx, cancel := context.WithCancel(context.Background())

	config, err := config.NewFromFlags(flags)

	if err != nil {
		os.Exit(1)
	}

	chColumns := cached_columns.NewChColumns()

	my := mysql.InitMysql(ctx, chColumns, mysql.Config{
		Address:               *config.MysqlAddr,
		User:                  *config.MysqlUser,
		Password:              *config.MysqlPassword,
		DbName:                *config.MysqlDb,
		DumpTables:            config.DumpTables,
		ConcurrentDumpQueries: *config.ConcurrentMysqlDumpQueries,
	})

	return App{
		Ctx:      ctx,
		Shutdown: cancel,
		Mysql:    my,
		Stats:    NewStats(testing),
		Config:   config,
		// chan of structs that aren't references take more space - but I think perf wise
		// they're basically allocated once and then that's it.
		ProcessRows: make(chan mysql.MysqlReplicationRowEvent, (*config.BatchSize)*2),
		BatchWrite:  make(chan mysql.MysqlReplicationRowEvent, (*config.BatchSize)*2),
		EventTranslator: mysql.NewEventTranslator(mysql.EventTranslatorConfig{
			AnonymizeFields:                config.AnonymizeFields,
			IgnoredColumnsForDeduplication: config.IgnoredColumnsForDeduplication,
			YamlColumns:                    config.YamlColumns,
		}),
		ChColumns: chColumns,
	}
}
