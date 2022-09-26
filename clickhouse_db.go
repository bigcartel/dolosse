package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type ClickhouseDb struct {
	conn driver.Conn
}

const eventCreatedAtColumnName = "changelog_event_created_at"
const actionColumnName = "changelog_action"
const eventIdColumnName = "changelog_id"

func establishClickhouseConnection() (ClickhouseDb, error) {
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{*clickhouseAddr},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4, Level: 1},
		Settings: clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": 1,
		},
	})

	return ClickhouseDb{
		conn: clickhouseConn,
	}, err
}

func (db *ClickhouseDb) Query(q string, args ...interface{}) [][]interface{} {
	rows, err := db.conn.Query(context.Background(), q, args...)
	if err != nil {
		log.Panicln(err, q, args)
	}

	rowColumnTypes := rows.ColumnTypes()
	columnTypes := make([]reflect.Type, len(rowColumnTypes))
	for i, c := range rowColumnTypes {
		columnTypes[i] = c.ScanType()
	}

	scannedRows := make([][]interface{}, 0)

	makeVarsForRow := func() []interface{} {
		vars := make([]interface{}, len(columnTypes))

		for i, columnType := range columnTypes {
			value := reflect.New(columnType).Elem()
			vars[i] = pointerToValue(value).Interface()
		}

		return vars
	}

	for rows.Next() {
		vars := makeVarsForRow()

		if err := rows.Scan(vars...); err != nil {
			must(err)
		}

		for i, v := range vars {
			if columnTypes[i].Kind() != reflect.Pointer {
				// dereference non-pointer columns so they're easier to deal with elsewhere
				vars[i] = reflect.ValueOf(v).Elem().Interface()
			}
		}

		var row []interface{}
		scannedRows = append(scannedRows, append(row, vars...))
	}

	return scannedRows
}

func (db *ClickhouseDb) QueryIdRange(tableWithDb string, minId int64, maxId int64) (bool, map[int64]bool) {
	queryString := fmt.Sprintf(`
		SELECT
		id
		FROM %s where id >= $1 and id <= $2`,
		tableWithDb)

	ids := db.Query(queryString, minId, maxId)
	log.Infoln("number of ids returned", len(ids))

	idsMap := make(map[int64]bool, len(ids))

	for _, row := range ids {
		idsMap[reflect.ValueOf(row[0]).Int()] = true
	}

	return len(idsMap) > 0, idsMap
}

func (db *ClickhouseDb) QueryDuplicates(tableWithDb string, start time.Time, end time.Time) (bool, map[string]bool) {
	queryString := fmt.Sprintf(`
		SELECT
		%s
		FROM %s where %s >= $1 and %s <= $2`,
		eventIdColumnName,
		tableWithDb,
		eventCreatedAtColumnName,
		eventCreatedAtColumnName)

	duplicates := db.Query(queryString, start.Add(-1*time.Second), end.Add(1*time.Second))

	// TODO is this something where I could re-use memory - would it be beneficial?
	duplicatesMap := make(map[string]bool)

	for _, dup := range duplicates {
		v, ok := dup[0].(string)
		if !ok {
			log.Fatalf("Failed converting %v to string", v)
		}

		duplicatesMap[v] = true
	}

	return len(duplicates) > 0, duplicatesMap
}

func (db *ClickhouseDb) Setup() {
	ctx := context.Background()
	err := db.conn.Exec(ctx, fmt.Sprintf("create database if not exists %s", *clickhouseDb))

	if err != nil {
		log.Fatal(err)
	}

	err = db.conn.Exec(ctx, fmt.Sprintf(`
		create table if not exists %s.binlog_sync_state (
			key String,
			value String
	 ) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)`, *clickhouseDb))

	if err != nil {
		log.Fatal(err)
	}
}

// unfortunately we can't get reflect types when querying all tables at once
// so this is a separate type from ClickhouseQueryColumn
type ChColumnInfo struct {
	Name  string `ch:"name"`
	Table string `ch:"table"`
	Type  string `ch:"type"`
}

type ChColumnMap map[string][]ChColumnInfo

func (db *ClickhouseDb) ColumnsForMysqlTables() ChColumnMap {
	mysqlTables := getMysqlTableNames()
	clickhouseTableMap := db.getColumnMap()
	columnsForTables := make(ChColumnMap, len(mysqlTables))

	for _, name := range mysqlTables {
		columns := clickhouseTableMap[name]
		if len(columns) > 0 {
			columnsForTables[name] = columns
		}
	}

	return columnsForTables
}

func (db *ClickhouseDb) getColumnMap() ChColumnMap {
	rows := unwrap(db.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT table, name, type FROM system.columns where database='%s'`, *clickhouseDb)))

	columns := make(ChColumnMap, 0)

	for rows.Next() {
		columnInfo := ChColumnInfo{}
		must(rows.ScanStruct(&columnInfo))

		tableName := columnInfo.Table

		if columns[tableName] == nil {
			columns[tableName] = make([]ChColumnInfo, 0)
		}

		columns[tableName] = append(columns[tableName], columnInfo)
	}

	return columns
}

func (db *ClickhouseDb) CheckSchema() {
	clickhouseColumnsByTable := db.ColumnsForMysqlTables()

	invalidTableMessages := make([]string, 0, len(clickhouseColumnsByTable))

	for table, columns := range clickhouseColumnsByTable {
		if len(columns) == 0 {
			continue
		}

		requiredCreatedAtType := "DateTime64(9)"
		requiredActionType := "LowCardinality(String)"
		requiredIdType := "String"
		validEventCreatedAt := false
		validAction := false
		validId := false

		for _, column := range columns {
			switch column.Name {
			case eventCreatedAtColumnName:
				validEventCreatedAt = column.Type == requiredCreatedAtType
			case actionColumnName:
				validAction = column.Type == requiredActionType
			case eventIdColumnName:
				validId = column.Type == requiredIdType
			}
		}

		if !validEventCreatedAt || !validAction || !validId {
			baseError := fmt.Sprintf("Clickhouse destination table %s requires columns",
				table)

			columnStrings := make([]string, 0)

			if !validEventCreatedAt {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s %s", eventCreatedAtColumnName, requiredCreatedAtType))
			}
			if !validAction {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s %s", actionColumnName, requiredActionType))
			}
			if !validId {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s %s", eventIdColumnName, requiredIdType))
			}

			invalidTableMessages = append(invalidTableMessages, fmt.Sprintf("%s %s", baseError, strings.Join(columnStrings, ", ")))
		}
	}

	if len(invalidTableMessages) > 0 {
		for i := range invalidTableMessages {
			log.Errorln(invalidTableMessages[i])
		}
		os.Exit(0)
	}
}

type ClickhouseQueryColumn struct {
	Name string
	Type reflect.Type
}

// Used to get reflect types for each column value that can then be used for
// safe value casting
func (db *ClickhouseDb) Columns(table string) ([]ClickhouseQueryColumn, LookupMap) {
	queryString := fmt.Sprintf(`select * from %s.%s limit 0`, *clickhouseDb, table)
	rows, err := db.conn.Query(context.Background(), queryString)

	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return make([]ClickhouseQueryColumn, 0), LookupMap{}
		} else {
			log.Panicln(err, "- query -", queryString)
		}
	}

	columnTypes := rows.ColumnTypes()
	var columns = make([]ClickhouseQueryColumn, len(columnTypes))
	columnNameLookup := make(LookupMap, len(columnTypes))

	for i, columnType := range columnTypes {
		columnName := columnType.Name()
		columnReflectScanType := columnType.ScanType()

		columnNameLookup[columnName] = true

		columns[i] = ClickhouseQueryColumn{
			Name: columnName,
			Type: columnReflectScanType,
		}
	}

	return columns, columnNameLookup
}

var gtidSetKey string = "last_synced_gtid_set"

type storedKeyValue struct {
	Value string `ch:"value"`
}

func (db *ClickhouseDb) GetGTIDSet(fallback string) mysql.GTIDSet {
	gtidString := db.GetStateString(gtidSetKey)

	if gtidString == "" {
		gtidString = fallback
	}

	log.Infoln("read gtid set", gtidString)
	set, err := mysql.ParseMysqlGTIDSet(gtidString)

	if err != nil {
		log.Fatal(err)
	}

	return set
}

func (db *ClickhouseDb) SetGTIDString(s string) {
	db.SetStateString(gtidSetKey, s)
	log.Infoln("persisted gtid set", s)
}

func (db *ClickhouseDb) tableDumpedKey(table string) string {
	return fmt.Sprintf("dumped-%s", table)
}

func (db *ClickhouseDb) GetTableDumped(table string) bool {
	tableDumped := db.GetStateString(db.tableDumpedKey(table))
	return tableDumped == "true"
}

func (db *ClickhouseDb) SetTableDumped(table string, dumped bool) {
	val := "false"
	if dumped {
		val = "true"
	}

	db.SetStateString(db.tableDumpedKey(table), val)
}

func (db *ClickhouseDb) GetStateString(key string) string {
	var rows []storedKeyValue

	err := db.conn.Select(context.Background(),
		&rows,
		fmt.Sprintf("select value from %s.binlog_sync_state where key = $1", *clickhouseDb),
		gtidSetKey)

	if err != nil {
		log.Fatal(err)
	}

	value := ""
	if len(rows) > 0 {
		value = rows[0].Value
	}

	return value
}

func (db *ClickhouseDb) SetStateString(key, value string) {
	err := db.conn.Exec(context.Background(),
		fmt.Sprintf("insert into %s.binlog_sync_state (key, value) values ($1, $2)", *clickhouseDb),
		key,
		value)

	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("persisted key %s", key)
}
