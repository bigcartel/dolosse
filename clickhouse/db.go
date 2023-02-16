package clickhouse

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"bigcartel/dolosse/clickhouse/cached_columns"
	"bigcartel/dolosse/consts"
	"bigcartel/dolosse/err_utils"
	"bigcartel/dolosse/mysql"
	"bigcartel/dolosse/set"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type ClickhouseDb struct {
	Conn   driver.Conn
	Ctx    context.Context
	Config Config
}

type Config struct {
	Address  string
	Username string
	Password string
	DbName   string
}

func EstablishClickhouseConnection(ctx context.Context, config Config) (ClickhouseDb, error) {

	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: config.Username,
			Password: config.Password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4, Level: 1},
		Settings: clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": 1,
		},
	})

	return ClickhouseDb{
		Conn:   clickhouseConn,
		Config: config,
		Ctx:    ctx,
	}, err
}

func (db ClickhouseDb) QueryIdRange(tableWithDb string, minPks mysql.Pks, maxPks mysql.Pks) (bool, set.Set[string]) {
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

	pksSet := make(set.Set[string], 10000)

	rows := err_utils.Unwrap(db.Conn.Query(db.Ctx, queryString))
	for rows.Next() {
		var s string
		err_utils.Must(rows.Scan(&s))
		pksSet.Add(s)
	}

	log.Infoln("number of pks returned", len(pksSet))

	return len(pksSet) > 0, pksSet
}

func (db ClickhouseDb) QueryDuplicates(tableWithDb string, minMaxValues mysql.MinMaxValues) (bool, set.Set[mysql.DuplicateBinlogEventKey]) {
	whereClauses := make([]string, 0, len(minMaxValues.ValuesByServerId))
	for serverId, minMax := range minMaxValues.ValuesByServerId {
		whereClauses = append(whereClauses,
			fmt.Sprintf("(%s = '%s' and %s >= %d and %s <= %d)",
				consts.EventServerIdColumnName,
				serverId,
				consts.EventTransactionIdColumnName,
				minMax.MinTransactionId,
				consts.EventTransactionIdColumnName,
				minMax.MaxTransactionId))
	}

	duplicatesMap := make(set.Set[mysql.DuplicateBinlogEventKey], 2)

	if len(whereClauses) == 0 {
		return false, duplicatesMap
	}

	whereClause := strings.Join(whereClauses, " OR ")

	queryString := fmt.Sprintf(`
		SELECT
		%s, %s, %s
		FROM %s WHERE %s`,
		consts.EventServerIdColumnName,
		consts.EventTransactionIdColumnName,
		consts.EventTransactionEventNumberColumnName,
		tableWithDb,
		whereClause)

	duplicates := err_utils.Unwrap(db.Conn.Query(db.Ctx, queryString))

	for duplicates.Next() {
		duplicateKey := mysql.DuplicateBinlogEventKey{}
		err_utils.Must(duplicates.ScanStruct(&duplicateKey))
		duplicatesMap.Add(duplicateKey)
	}

	return len(duplicatesMap) > 0, duplicatesMap
}

func (db ClickhouseDb) Setup() {
	err_utils.Must(db.Conn.Exec(db.Ctx, fmt.Sprintf("create database if not exists %s", db.Config.DbName)))

	err_utils.Must(db.Conn.Exec(db.Ctx, fmt.Sprintf(`
		create table if not exists %s.binlog_sync_state (
			key String,
			value String
	 ) ENGINE = EmbeddedRocksDB PRIMARY KEY(key)`, db.Config.DbName)))
}

func (db ClickhouseDb) ColumnsForMysqlTables(my cached_columns.MyColumnQueryable) cached_columns.ChTableColumnMap {
	mysqlTables := my.GetMysqlTableNames()
	clickhouseTableMap := db.getColumnMap()
	columnsForTables := make(cached_columns.ChTableColumnMap, len(mysqlTables))

	for _, name := range mysqlTables {
		columns := clickhouseTableMap[name]
		if len(columns) > 0 {
			columnsForTables[name] = columns
		}
	}

	return columnsForTables
}

func (db ClickhouseDb) getColumnMap() cached_columns.ChTableColumnMap {
	rows := err_utils.Unwrap(db.Conn.Query(db.Ctx,
		fmt.Sprintf(`SELECT table, name, type FROM system.columns where database='%s'`, db.Config.DbName)))

	columns := make(cached_columns.ChTableColumnMap, 0)

	for rows.Next() {
		columnInfo := cached_columns.ChTableColumn{}
		err_utils.Must(rows.ScanStruct(&columnInfo))

		tableName := columnInfo.Table

		if columns[tableName] == nil {
			columns[tableName] = make([]cached_columns.ChTableColumn, 0)
		}

		columns[tableName] = append(columns[tableName], columnInfo)
	}

	return columns
}

// TODO: could also add a field that is a go template for adding this
// column to a given table as an alter table statement
type RequiredColumn = struct {
	Name    string
	Type    string
	IsValid bool
}

func formatString(format string, args ...string) string {
	r := strings.NewReplacer(args...)
	return r.Replace(format)
}

// TODO: figure out way to make this really fast if the table is already created, or just skip
func (db ClickhouseDb) AutoCreateMysqlTable(mysqlDbName, mysqlTableName string, matcher *regexp.Regexp, myCfg mysql.Config) {
	if !matcher.MatchString(mysqlTableName) {
		return
	}

	// TODO: configurable auto-added pks in hierarchical order
	// if mysql table includes columns - eg: account_id, product_id, product_option_id
	// or just always make it event created at unless you specify some override based on a match?
	// or base it on first encountered index if present?
	// I guess I maybe gravitate towards simpler, always ordering by binlog columns.
	err_utils.Must(db.Conn.Exec(db.Ctx,
		formatString(`
			CREATE TABLE {clickhouseSchema}.{mysqlTableName} AS
				SELECT
					*,
			FROM mysql(
				'{mysqlAddress}',
				'{mysqlDb}',
				'{mysqlTable}',
				'{mysqlUser}',
				'{mysqlPassword}'
			)`,
			"{clickhouseSchema}", db.Config.DbName,
			"{mysqlAddress}", myCfg.Address,
			"{mysqlDb}", mysqlDbName,
			"{mysqlTable}", mysqlTableName,
			"{mysqlUser}", myCfg.User,
			"{mysqlPassword}", myCfg.Password,
		)))
}

func (db ClickhouseDb) CheckSchema(my cached_columns.MyColumnQueryable) {
	clickhouseColumnsByTable := db.ColumnsForMysqlTables(my)

	invalidTableMessages := make([]string, 0, len(clickhouseColumnsByTable))

	for table, columns := range clickhouseColumnsByTable {
		if len(columns) == 0 {
			continue
		}

		// TODO: make a const and use this to create tables automatically as well
		requiredColumns := []RequiredColumn{
			{consts.EventCreatedAtColumnName, "DateTime64(9)", false},
			{consts.EventActionColumnName, "LowCardinality(String)", false},
			{consts.EventServerIdColumnName, "LowCardinality(String)", false},
			{consts.EventTransactionIdColumnName, "UInt64", false},
			{consts.EventTransactionEventNumberColumnName, "UInt32", false},
			{consts.EventUpdatedColumnsColumnName, "Array(LowCardinality(String))", false},
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

			invalidTableMessages = append(invalidTableMessages,
				fmt.Sprintf("%s %s", baseError, strings.Join(columnStrings, ", ")))
		}
	}

	if len(invalidTableMessages) > 0 {
		for i := range invalidTableMessages {
			log.Errorln(invalidTableMessages[i])
		}
		os.Exit(0)
	}
}

// Used to get reflect types for each column value that can then be used for
// safe value casting
func (db ClickhouseDb) ColumnsForInsert(table string) (cached_columns.ChInsertColumns, cached_columns.ChInsertColumnMap) {
	queryString := fmt.Sprintf(`select * from %s.%s limit 0`, db.Config.DbName, table)
	rows, err := db.Conn.Query(db.Ctx, queryString)

	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return make(cached_columns.ChInsertColumns, 0), cached_columns.ChInsertColumnMap{}
		} else {
			log.Panicln(err, "- query -", queryString)
		}
	}

	columnTypes := rows.ColumnTypes()
	var columns = make([]cached_columns.ChInsertColumn, len(columnTypes))
	columnNameLookup := make(cached_columns.ChInsertColumnMap, len(columnTypes))

	for i, columnType := range columnTypes {
		columnName := columnType.Name()

		queryColumn := cached_columns.ChInsertColumn{
			Name:             columnName,
			DatabaseTypeName: columnType.DatabaseTypeName(),
			Type:             columnType.ScanType(),
		}

		columnNameLookup[columnName] = queryColumn

		columns[i] = queryColumn
	}

	return columns, columnNameLookup
}

func (db ClickhouseDb) GetGTIDSet(fallback string) gomysql.GTIDSet {
	gtidString := db.GetStateString(consts.GtidSetKey)

	if gtidString == "" {
		gtidString = fallback
	}

	log.Infoln("read gtid set", gtidString)
	set, err := gomysql.ParseMysqlGTIDSet(gtidString)

	if err != nil {
		log.Fatal(err)
	}

	return set
}

func (db ClickhouseDb) SetGTIDString(s string) {
	db.SetStateString(consts.GtidSetKey, s)
	log.Infoln("persisted gtid set", s)
}

func (db ClickhouseDb) tableDumpedKey(table string) string {
	return fmt.Sprintf("dumped-%s", table)
}

func (db ClickhouseDb) GetTableDumped(table string) bool {
	tableDumped := db.GetStateString(db.tableDumpedKey(table))
	return tableDumped == "true"
}

func (db ClickhouseDb) SetTableDumped(table string, dumped bool) {
	val := "false"
	if dumped {
		val = "true"
	}

	db.SetStateString(db.tableDumpedKey(table), val)
}

func (db ClickhouseDb) GetStateString(key string) string {
	type storedKeyValue struct {
		Value string `ch:"value"`
	}

	var rows []storedKeyValue

	err_utils.Must(db.Conn.Select(context.Background(),
		&rows,
		fmt.Sprintf("select value from %s.binlog_sync_state where key = $1", db.Config.DbName),
		key))

	value := ""
	if len(rows) > 0 {
		value = rows[0].Value
	}

	return value
}

func (db ClickhouseDb) SetStateString(key, value string) {
	err := db.Conn.Exec(context.Background(),
		fmt.Sprintf("insert into %s.binlog_sync_state (key, value) values ($1, $2)", db.Config.DbName),
		key,
		value)

	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("persisted key %s", key)
}
