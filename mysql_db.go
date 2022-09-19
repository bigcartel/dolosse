package main

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"

	_ "github.com/go-mysql-org/go-mysql/driver"
)

var mysqlColumns = NewConcurrentMap[*schema.Table]()

func getEarliestGtidStartPoint(conn *client.Conn) string {
	rr, err := conn.Execute("select @@GLOBAL.gtid_purged;")

	if err != nil {
		log.Fatal(err)
	}

	return string(rr.Values[0][0].AsString())
}

func getMysqlTableNames(conn *client.Conn) []string {
	rr, err := conn.Execute(fmt.Sprintf("SHOW TABLES FROM %s", *mysqlDb))
	if err != nil {
		log.Fatal(err)
	}

	tables := make([]string, len(rr.Values))
	for i, v := range rr.Values {
		tables[i] = string(v[0].AsString())
	}

	return tables
}

// TODO could I use this same method for getting clickhouse tables?
// lazy load instead of doing a scheduled load at a particular time?
// TODO this needs to be reset as well on alter tables
func getMysqlTable(conn *client.Conn, db, table string) *schema.Table {
	v := mysqlColumns.get(table)

	if v != nil {
		return *v
	} else {
		t, err := schema.NewTable(conn, db, table)
		checkErr(err)
		mysqlColumns.set(table, &t)
		return t
	}
}

func establishMysqlConnection() *client.Conn {
	conn, err := client.Connect(*mysqlAddr, *mysqlUser, *mysqlPassword, *mysqlDb)
	checkErr(err)
	return conn
}

func DumpMysqlDb() {
	dumping.Store(true)

	conn := establishMysqlConnection()
	// TODO paralellize with either worker or waitgroup pattern.
	for table := range *chColumns.m {
		dumpTable(conn, *mysqlDb, table)
	}

	dumping.Store(false)
}

type DumpFieldVal interface {
	AsString() []byte
	Value() interface{}
}

func convertMysqlDumpDatesAndDecimalsToString(val DumpFieldVal, mysqlQueryFieldType uint8) interface{} {
	switch mysqlQueryFieldType {
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		vs := string(val.AsString())
		if len(vs) > 0 {
			return vs
		} else {
			return val.Value()
		}
	default:
		return val.Value()
	}
}

// TODO write some tests for this?
func dumpTable(conn *client.Conn, dbName, tableName string) {
	var result mysql.Result
	err := conn.ExecuteSelectStreaming("SELECT * FROM "+dbName+"."+tableName, &result, func(row []mysql.FieldValue) error {
		values := make([]interface{}, len(row))

		for idx, val := range row {
			values[idx] = convertMysqlDumpDatesAndDecimalsToString(&val, result.Fields[idx].Type)
		}

		processDumpData(conn, dbName, tableName, values)

		return nil
	}, nil)

	checkErr(err)
}

func processDumpData(conn *client.Conn, dbName string, tableName string, values []interface{}) {
	_, hasColumns := cachedChColumnsForTable(tableName)
	if !hasColumns {
		return
	}

	tableInfo := getMysqlTable(conn, dbName, tableName)

	event := MysqlReplicationRowEvent{
		Table:  tableInfo,
		Rows:   [][]interface{}{values},
		Action: "dump",
	}

	OnRow(&event)
}
