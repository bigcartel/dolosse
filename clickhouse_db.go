package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type ClickhouseDb struct {
	conn driver.Conn
}

const eventCreatedAtColumnName = "changelog_event_created_at"
const eventActionColumnName = "changelog_action"
const eventServerIdColumnName = "changelog_gtid_server_id"
const eventTransactionIdColumnName = "changelog_gtid_transaction_id"
const eventTransactionEventNumberColumnName = "changelog_gtid_transaction_event_number"
const gtidSetKey = "last_synced_gtid_set"

func establishClickhouseConnection() (ClickhouseDb, error) {
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*Config.ClickhouseAddr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: *Config.ClickhouseUsername,
			Password: *Config.ClickhousePassword,
		},
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

func (db *ClickhouseDb) QueryIdRange(tableWithDb string, minPks Pks, maxPks Pks) (bool, Set[string]) {
	var pkq string
	whereClauses := make([]string, 0, len(minPks))

	for i, minPk := range minPks {
		maxPk := maxPks[i]
		var minVal, maxVal string
		switch m := minPk.Value.(type) {
		case string:
			minVal = "'" + m + "'"
			maxVal = "'" + maxPk.Value.(string) + "'"
		default:
			minVal = fmt.Sprintf("%v", minPk.Value)
			maxVal = fmt.Sprintf("%v", maxPk.Value)
		}

		whereClauses = append(whereClauses,
			fmt.Sprintf("(%s >= %s AND %s <= %s)",
				minPk.ColumnName,
				minVal,
				minPk.ColumnName,
				maxVal))
	}

	for i, pk := range minPks {
		pkq += "toString(" + pk.ColumnName + ")"
		if i < len(minPks)-1 {
			pkq += "||'-'||"
		} else {
			pkq += " AS pk"
		}
	}

	whereClause := strings.Join(whereClauses, " AND ")
	queryString := fmt.Sprintf(`
               SELECT
               %s
               FROM %s where %s`,
		pkq,
		tableWithDb,
		whereClause)

	pksSet := make(Set[string], 10000)

	rows := unwrap(db.conn.Query(State.ctx, queryString))
	for rows.Next() {
		var s string
		rows.Scan(&s)
		pksSet.Add(s)
	}

	log.Infoln("number of pks returned", len(pksSet))

	return len(pksSet) > 0, pksSet
}

func (db *ClickhouseDb) QueryDuplicates(tableWithDb string, minMaxValues MinMaxValues) (bool, Set[DuplicateBinlogEventKey]) {
	whereClauses := make([]string, 0, len(minMaxValues.ValuesByServerId))
	for serverId, minMax := range minMaxValues.ValuesByServerId {
		whereClauses = append(whereClauses,
			fmt.Sprintf("(%s = '%s' and %s >= %d and %s <= %d)",
				eventServerIdColumnName,
				serverId,
				eventTransactionIdColumnName,
				minMax.MinTransactionId,
				eventTransactionIdColumnName,
				minMax.MaxTransactionId))
	}

	duplicatesMap := make(Set[DuplicateBinlogEventKey], 2)

	if len(whereClauses) == 0 {
		return false, duplicatesMap
	}

	whereClause := strings.Join(whereClauses, " OR ")

	queryString := fmt.Sprintf(`
		SELECT
		%s, %s, %s
		FROM %s WHERE %s`,
		eventServerIdColumnName,
		eventTransactionIdColumnName,
		eventTransactionEventNumberColumnName,
		tableWithDb,
		whereClause)

	duplicates := unwrap(db.conn.Query(State.ctx, queryString))

	for duplicates.Next() {
		duplicateKey := DuplicateBinlogEventKey{}
		must(duplicates.ScanStruct(&duplicateKey))
		duplicatesMap.Add(duplicateKey)
	}

	return len(duplicatesMap) > 0, duplicatesMap
}

func (db *ClickhouseDb) Setup() {
	must(db.conn.Exec(State.ctx, fmt.Sprintf("create database if not exists %s", *Config.ClickhouseDb)))

	must(db.conn.Exec(State.ctx, fmt.Sprintf(`
		create table if not exists %s.binlog_sync_state (
			key String,
			value String
	 ) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)`, *Config.ClickhouseDb)))
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
		fmt.Sprintf(`SELECT table, name, type FROM system.columns where database='%s'`, *Config.ClickhouseDb)))

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

type RequiredColumn = struct {
	Name    string
	Type    string
	IsValid bool
}

func (db *ClickhouseDb) CheckSchema() {
	clickhouseColumnsByTable := db.ColumnsForMysqlTables()

	invalidTableMessages := make([]string, 0, len(clickhouseColumnsByTable))

	for table, columns := range clickhouseColumnsByTable {
		if len(columns) == 0 {
			continue
		}

		requiredColumns := []RequiredColumn{
			{eventCreatedAtColumnName, "DateTime64(9)", false},
			{eventActionColumnName, "LowCardinality(String)", false},
			{eventServerIdColumnName, "LowCardinality(String)", false},
			{eventTransactionIdColumnName, "UInt64", false},
			{eventTransactionEventNumberColumnName, "UInt32", false},
		}

		for _, column := range columns {
			for i, requiredColumn := range requiredColumns {
				if column.Name == requiredColumn.Name && column.Type == requiredColumn.Type {
					requiredColumn.IsValid = true
					requiredColumns[i] = requiredColumn
				}
			}
		}

		columnStrings := make([]string, 0, len(requiredColumns))

		for _, c := range requiredColumns {
			if !c.IsValid {
				columnStrings = append(columnStrings,
					fmt.Sprintf("%s %s", c.Name, c.Type))
			}
		}

		if len(columnStrings) > 0 {
			baseError := fmt.Sprintf("Clickhouse destination table %s requires columns",
				table)

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
	queryString := fmt.Sprintf(`select * from %s.%s limit 0`, *Config.ClickhouseDb, table)
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
	type storedKeyValue struct {
		Value string `ch:"value"`
	}

	var rows []storedKeyValue

	must(db.conn.Select(context.Background(),
		&rows,
		fmt.Sprintf("select value from %s.binlog_sync_state where key = $1", *Config.ClickhouseDb),
		key))

	value := ""
	if len(rows) > 0 {
		value = rows[0].Value
	}

	return value
}

func (db *ClickhouseDb) SetStateString(key, value string) {
	err := db.conn.Exec(context.Background(),
		fmt.Sprintf("insert into %s.binlog_sync_state (key, value) values ($1, $2)", *Config.ClickhouseDb),
		key,
		value)

	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("persisted key %s", key)
}
