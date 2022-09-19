package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type ClickhouseDb struct {
	conn driver.Conn
}

const eventCreatedAtColumnName = "changelog_event_created_at"
const actionColumnName = "changelog_action"

func establishClickhouseConnection() ClickhouseDb {
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{*clickhouseAddr},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4, Level: 1},
		Settings: clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": 1,
		},
	})

	checkErr(err)

	return ClickhouseDb{
		conn: clickhouseConn,
	}
}

func (db ClickhouseDb) Query(q string, args ...interface{}) [][]interface{} {
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
			checkErr(err)
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

func (db ClickhouseDb) QueryIdRange(tableWithDb string, minId int64, maxId int64) (bool, map[int64]bool) {
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

func (db ClickhouseDb) QueryDuplicates(tableWithDb string, start time.Time, end time.Time) (bool, map[string]bool) {
	queryString := fmt.Sprintf(`
		SELECT
		id,
		cityHash64(
			arrayStringConcat(
				arrayFilter(
					x -> isNotNull(x),
					array(* except(%s) apply toString)
				)
			, ',')
		) as checksum
		FROM %s where %s >= $1 and %s <= $2`,
		actionColumnName,
		tableWithDb,
		eventCreatedAtColumnName,
		eventCreatedAtColumnName)

	duplicates := db.Query(queryString, start, end)

	duplicatesMap := make(map[string]bool)

	for _, dup := range duplicates {
		dupString := dupIdString(dup[0], dup[1])

		duplicatesMap[dupString] = true
	}

	return len(duplicates) > 0, duplicatesMap
}

func (db ClickhouseDb) Setup() {
	ctx := context.Background()
	err := db.conn.Exec(ctx, "create database if not exists mysql_bigcartel_binlog")

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

func (db ClickhouseDb) ColumnsForMysqlTables(mysqlConn *client.Conn) ChColumnMap {
	mysqlTables := getMysqlTableNames(mysqlConn)
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

func (db ClickhouseDb) getColumnMap() ChColumnMap {
	rows, err := db.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT table, name, type FROM system.columns where database='%s'`, *clickhouseDb))

	if err != nil {
		log.Panicln(err)
	}

	columns := make(ChColumnMap, 0)

	for rows.Next() {
		columnInfo := ChColumnInfo{}
		err := rows.ScanStruct(&columnInfo)

		if err != nil {
			log.Panicln(err)
		}

		tableName := columnInfo.Table

		if columns[tableName] == nil {
			columns[tableName] = make([]ChColumnInfo, 0)
		}

		columns[tableName] = append(columns[tableName], columnInfo)
	}

	return columns
}

func (db ClickhouseDb) CheckSchema(mysqlConn *client.Conn) error {
	clickhouseColumnsByTable := db.ColumnsForMysqlTables(mysqlConn)

	for table, columns := range clickhouseColumnsByTable {
		if len(columns) == 0 {
			continue
		}

		hasEventCreatedAt := false
		hasAction := false
		for _, column := range columns {
			switch column.Name {
			case eventCreatedAtColumnName:
				hasEventCreatedAt = true
			case actionColumnName:
				hasAction = true
			}
		}

		if !hasEventCreatedAt || !hasAction {
			baseError := fmt.Sprintf("Clickhouse destination table %s is missing columns",
				table)

			columnStrings := make([]string, 0)

			if !hasEventCreatedAt {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s DateTime", eventCreatedAtColumnName))
			}
			if !hasAction {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s LowCardinality(string)", actionColumnName))
			}

			log.Panicf("%s %s", baseError, strings.Join(columnStrings, ", "))
		}
	}

	return nil
}

type ClickhouseQueryColumn struct {
	Name string
	Type reflect.Type
}

// Used to get reflect types for each column value that can then be used for
// safe value casting
func (db ClickhouseDb) Columns(table string) ([]ClickhouseQueryColumn, LookupMap) {
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

func (db ClickhouseDb) GetGTIDSet(fallback string) mysql.GTIDSet {
	type storedGtidSet struct {
		Value string `ch:"value"`
	}

	var rows []storedGtidSet

	err := db.conn.Select(context.Background(),
		&rows,
		"select value from mysql_bigcartel_binlog.binlog_sync_state where key = $1",
		gtidSetKey)

	if err != nil {
		log.Fatal(err)
	}

	gtidString := fallback
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

func (db ClickhouseDb) SetGTIDString(s string) {
	err := db.conn.Exec(context.Background(),
		"insert into mysql_bigcartel_binlog.binlog_sync_state (key, value) values ($1, $2)",
		gtidSetKey,
		s)

	if err != nil {
		log.Fatal(err)
	}

	log.Infoln("persisted gtid set", s)
}
