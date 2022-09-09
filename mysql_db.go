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

func establishMysqlConnection() *client.Conn {
	conn, err := client.Connect(*mysqlAddr, *mysqlUser, *mysqlPassword, *mysqlDb)
	checkErr(err)
	return conn
}

// TODO do I even need this? Should I instead refresh clickhouse columns in the dump routine?
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

func DumpMysqlDb() {
	dumping.Store(true)
	conn := establishMysqlConnection()
	dumpTable(conn, "bigcartel", "products")
	dumping.Store(false)
}

// TODO write some tests for this?
func dumpTable(conn *client.Conn, dbName, tableName string) {
	var result mysql.Result
	err := conn.ExecuteSelectStreaming("SELECT * FROM "+dbName+"."+tableName, &result, func(row []mysql.FieldValue) error {
		values := make([]interface{}, len(row))

		for idx, val := range row {
			field := result.Fields[idx]

			switch field.Type {
			case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
				vs := string(val.AsString())
				if len(vs) > 0 {
					vt, err := time.ParseInLocation(mysql.TimeFormat, vs, time.UTC)
					checkErr(err)
					values[idx] = vt
				} else {
					values[idx] = val.Value()
				}
			case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
				vs := string(val.AsString())
				if len(vs) > 0 {
					val, err := decimal.NewFromString(string(val.AsString()))
					checkErr(err)
					values[idx] = val
				} else {
					values[idx] = val.Value()
				}
			default:
				values[idx] = val.Value()
			}
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
