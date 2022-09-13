package main

import (
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"

	_ "github.com/go-mysql-org/go-mysql/driver"
)

func getEarliestGtidStartPoint(mysqlCanal *canal.Canal) string {
	rr, err := mysqlCanal.Execute("select @@GLOBAL.gtid_purged;")

	if err != nil {
		log.Fatal(err)
	}

	return string(rr.Values[0][0].AsString())
}

func getMysqlTableNames(mysqlCanal *canal.Canal) []string {
	rr, err := mysqlCanal.Execute(fmt.Sprintf("SHOW TABLES FROM %s", *mysqlDb))
	if err != nil {
		log.Fatal(err)
	}

	tables := make([]string, len(rr.Values))
	for i, v := range rr.Values {
		tables[i] = string(v[0].AsString())
	}

	return tables
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

func parseMysqlDumpField(val DumpFieldVal, mysqlQueryFieldType uint8) interface{} {
	switch mysqlQueryFieldType {
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		vs := string(val.AsString())
		if len(vs) > 0 {
			vt, err := time.ParseInLocation(mysql.TimeFormat, vs, time.UTC)
			checkErr(err)
			return vt
		} else {
			return val.Value()
		}
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		vs := string(val.AsString())
		if len(vs) > 0 {
			val, err := decimal.NewFromString(string(val.AsString()))
			checkErr(err)
			return val
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
			values[idx] = parseMysqlDumpField(&val, result.Fields[idx].Type)
		}

		processDumpData(dbName, tableName, values)

		return nil
	}, nil)

	checkErr(err)
}

func processDumpData(dbName string, tableName string, values []interface{}) error {
	tableInfo, err := syncCanal.GetTable(dbName, tableName)
	if err != nil {
		checkErr(err)
	}

	event := canal.RowsEvent{
		Table:  tableInfo,
		Action: "dump",
		Rows:   [][]interface{}{values},
	}

	return OnRow(&event)
}
