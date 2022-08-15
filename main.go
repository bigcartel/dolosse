package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/peterbourgon/ff/v3"
	"gopkg.in/yaml.v3"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

var yamlColumns = map[string]map[string]bool{
	"theme_instances": {
		"settings":          true,
		"images_sort_order": true,
	},
	"order_transactions": {
		"params": true,
	},
}

var ignoreColumns = map[string]map[string]bool{
	"theme_instances": {
		"images_marked_for_destruction": true,
	},
}

type ProcessRow struct {
	canal.DummyEventHandler
}

type RowData map[string]interface{}

type RowEvent struct {
	e          *canal.RowsEvent
	gtidSet    mysql.GTIDSet
	insertData RowInsertData
}

type RowInsertData struct {
	Id             int64     `ch:"id"`
	EventCreatedAt time.Time `ch:"event_created_at"`
	Action         string    `ch:"action"`
	Event          string    `ch:"event"`
}

var importChannel chan RowEvent
var clickhouseConn driver.Conn
var syncCanal *canal.Canal
var tables map[string]bool

func interfaceToInt64(v interface{}) int64 {
	switch i := v.(type) {
	case int64:
		return i
	case int32:
		return int64(i)
	case uint32:
		return int64(i)
	default:
		return 0
	}
}

func columnInMap(tableName string, columnName string, lookup map[string]map[string]bool) bool {
	if v, ok := lookup[tableName]; ok {
		return v[columnName]
	} else {
		return false
	}
}

func isYamlColumn(tableName string, columnName string) bool {
	return columnInMap(tableName, columnName, yamlColumns)
}

/* FIXME we might go a different approach here and instead
   infer what to insert based on clickhouse table column names */
func isIgnoredColumn(tableName string, columnName string) bool {
	return columnInMap(tableName, columnName, ignoreColumns)
}

var weirdYamlKeyMatcher = regexp.MustCompile("^:(.*)")

func handleValue(value interface{}, tableName string, columnName string) interface{} {
	switch v := value.(type) {
	case []uint8:
		value = string(v)
	case string:
		if isYamlColumn(tableName, columnName) {
			y := make(map[string]interface{})
			err := yaml.Unmarshal([]byte(v), y)

			if err != nil {
				y["rawYaml"] = v
				y["errorParsingYaml"] = err
				log.Errorln(v)
				log.Errorln(err)
			}

			for k, v := range y {
				delete(y, k)
				y[weirdYamlKeyMatcher.ReplaceAllString(k, "$1")] = v
			}

			value = y
		}
	}

	return value
}

func (h *ProcessRow) OnRow(e *canal.RowsEvent) error {
	gtidSet := syncCanal.SyncedGTIDSet()
	Data := make(RowData, len(e.Rows[0]))
	var id int64
	row := e.Rows[0]

	for i, c := range e.Table.Columns {
		if !isIgnoredColumn(e.Table.Name, c.Name) {
			if c.Name == "id" {
				id = interfaceToInt64(row[i])
			}

			Data[c.Name] = handleValue(row[i], e.Table.Name, c.Name)
		}
	}

	// TODO use the row's updated_at instead of this if present
	timestamp := time.Now()

	if e.Header != nil {
		timestamp = time.Unix(int64(e.Header.Timestamp), 0)
	}

	updated_at := Data["updated_at"]

	if v, ok := updated_at.(time.Time); ok && v.Before(timestamp) {
		log.Infoln("using updated at")
		timestamp = v
	}

	j, err := json.Marshal(Data)

	if err != nil {
		log.Fatal(err)
	}

	importChannel <- RowEvent{
		e:       e,
		gtidSet: gtidSet,
		insertData: RowInsertData{
			Id:             id,
			EventCreatedAt: timestamp,
			Action:         e.Action,
			Event:          string(j),
		},
	}

	return nil
}

func createLogTable(table string) {
	// TODO experiment with using create table from select empty using a mysql table function
	if !tables[table] {
		err := clickhouseConn.Exec(context.Background(), fmt.Sprintf(`
			create table if not exists mysql_bigcartel_binlog.%s (
				id             	 Int64 codec(DoubleDelta, ZSTD),
				event_created_at DateTime codec(DoubleDelta, ZSTD),
				action           LowCardinality(String),
				event            JSON CODEC(ZSTD)
			) Engine = MergeTree()
			ORDER BY (event_created_at)
		`, table))

		if err != nil {
			log.Fatal(err)
		}

		tables[table] = true
	}
}

type Duplicate struct {
	Id             int64     `ch:"id"`
	EventCreatedAt time.Time `ch:"event_created_at"`
}

func batchWrite() {
	timer := time.NewTimer(10 * time.Second)
	eventsByTable := make(map[string][]RowInsertData)
	tables = make(map[string]bool)
	var lastGtidSet mysql.GTIDSet

	counter := 0

	deliver := func() {
		timer = time.NewTimer(10 * time.Second)

		// TODO add position saving to clickhouse based on last row written and recovery on restart

		for table, rows := range eventsByTable {
			createLogTable(table)

			ctx := context.Background()

			tableWithDb := fmt.Sprintf("mysql_bigcartel_binlog.%s", table)

			var duplicates []Duplicate

			err := clickhouseConn.Select(ctx,
				&duplicates,
				fmt.Sprintf("SELECT id, event_created_at FROM %s where event_created_at >= $1 and event_created_at <= $2", tableWithDb),
				rows[0].EventCreatedAt, rows[len(rows)-1].EventCreatedAt,
			)

			if err != nil {
				log.Fatal(err)
			}

			var duplicatesMap = make(map[Duplicate]bool)

			for _, dup := range duplicates {
				duplicatesMap[dup] = true
			}

			batch, err := clickhouseConn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableWithDb))

			if err != nil {
				log.Fatal(err)
			}

			duplicateCheck := Duplicate{}
			hasDuplicates := len(duplicates) > 0

			for _, row := range rows {
				if hasDuplicates {
					duplicateCheck.Id = row.Id
					duplicateCheck.EventCreatedAt = row.EventCreatedAt

					if duplicatesMap[duplicateCheck] {
						log.Infoln("is dupe ", duplicateCheck)
						continue
					}
				}

				err = batch.AppendStruct(&row)

				if err != nil {
					log.Fatal(err)
				}

				err = batch.Flush()

				if err != nil {
					log.Fatal(err)
				}
			}

			log.Infoln("sending batch for ", table)

			err = batch.Send()

			if err != nil {
				log.Fatal(err)
			}

			// because I write batches by table it's not 1 to 1 writing in the same order as the binlog
			//
			// TODO save gtidset in clickhouse and recover on startup. Test this to make sure it's working correctly.

			log.Infoln("batch sent for ", table)

			if err != nil {
				log.Fatal(err)
			}
		}

		if lastGtidSet != nil {
			setGTIDString(lastGtidSet.String())
		}

		eventsByTable = make(map[string][]RowInsertData)
		counter = 0
	}

	for {
		select {
		case <-timer.C:
			deliver()
		case e := <-importChannel:
			eventsByTable[e.e.Table.Name] = append(eventsByTable[e.e.Table.Name], e.insertData)
			lastGtidSet = e.gtidSet

			if counter == 10000 {
				deliver()
			} else {
				counter++
			}
		}
	}
}

var gtidSetKey string = "last_synced_gtid_set"

func getGTIDSet() mysql.GTIDSet {
	type storedGtidSet struct {
		Value string `ch:"value"`
	}

	var rows []storedGtidSet

	err := clickhouseConn.Select(context.Background(),
		&rows,
		"select value from mysql_bigcartel_binlog.binlog_sync_state where key = $1",
		gtidSetKey)

	if err != nil {
		log.Fatal(err)
	}

	gtidString := ""
	if len(rows) > 0 {
		gtidString = rows[0].Value
	}

	log.Infoln("read gtid set", gtidString)
	set, err := mysql.ParseMysqlGTIDSet(gtidString)

	if err != nil {
		log.Fatal(err)
	}

	return set
}

func setGTIDString(s string) {
	err := clickhouseConn.Exec(context.Background(),
		"insert into mysql_bigcartel_binlog.binlog_sync_state (key, value) values ($1, $2)",
		gtidSetKey,
		s)

	if err != nil {
		log.Fatal(err)
	}

	log.Infoln("persisted gtid set", s)
}

func setupClickhouseDb() {
	ctx := context.Background()
	err := clickhouseConn.Exec(ctx, "create database if not exists mysql_bigcartel_binlog")

	if err != nil {
		log.Fatal(err)
	}

	err = clickhouseConn.Exec(ctx, `
		create table if not exists mysql_bigcartel_binlog.binlog_sync_state (
			key String,
			value String
	 ) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)`)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fs := flag.NewFlagSet("MySQL -> Clickhouse binlog replicator", flag.ContinueOnError)
	var (
		mysqlAddr      = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db (also via MYSQL_ADDR)")
		mysqlUser      = fs.String("mysql-user", "metabase", "mysql user (also via MYSQL_USER)")
		mysqlPassword  = fs.String("mysql-password", "password", "mysql password (also via MYSQL_PASSWORD)")
		clickhouseAddr = fs.String("clickhouse-addr", "10.100.0.56:9000", "ip/url and port for destination clickhouse db (also via CLICKHOUSE_ADDR)")
		_              = fs.String("config", "", "config file (optional)")
	)

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
	cfg.Dump.TableDB = "bigcartel"
	cfg.Dump.Tables = []string{"plans"}
	cfg.IncludeTableRegex = []string{"products"}

	importChannel = make(chan RowEvent, 20000)

	clickhouseConn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{*clickhouseAddr},
		Settings: clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": 1,
		},
		Compression: &clickhouse.Compression{clickhouse.CompressionLZ4, 1},
	})

	if err != nil {
		log.Fatal(err)
	}

	setupClickhouseDb()

	syncCanal, err = canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	syncCanal.SetEventHandler(&ProcessRow{})

	go batchWrite()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Infoln(syncCanal.SyncedGTIDSet())
		os.Exit(1)
	}()

	syncCanal.StartFromGTID(getGTIDSet())
}
