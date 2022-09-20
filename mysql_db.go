package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"

	_ "github.com/go-mysql-org/go-mysql/driver"
)

var mysqlPool *client.Pool
var mysqlColumns = NewConcurrentMap[*schema.Table]()

func getEarliestGtidStartPoint() string {
	return withMysqlConnection(func(conn *client.Conn) string {
		rr, err := conn.Execute("select @@GLOBAL.gtid_purged;")

		if err != nil {
			log.Fatal(err)
		}

		return string(rr.Values[0][0].AsString())
	})
}

func getMysqlTableNames() []string {
	return withMysqlConnection(func(conn *client.Conn) []string {
		rr, err := conn.Execute(fmt.Sprintf("SHOW TABLES FROM %s", *mysqlDb))
		if err != nil {
			log.Fatal(err)
		}

		tables := make([]string, len(rr.Values))
		for i, v := range rr.Values {
			tables[i] = string(v[0].AsString())
		}

		return tables
	})
}

// TODO could I use this same method for getting clickhouse tables?
// lazy load instead of doing a scheduled load at a particular time?
// TODO this needs to be reset as well on alter tables
func getMysqlTable(db, table string) *schema.Table {
	v := mysqlColumns.get(table)

	if v != nil {
		return *v
	} else {
		return withMysqlConnection(func(conn *client.Conn) *schema.Table {
			t, err := schema.NewTable(conn, db, table)
			checkErr(err)
			mysqlColumns.set(table, &t)
			return t
		})
	}
}

func initMysqlConnectionPool() {
	mysqlPool = client.NewPool(log.Debugf, 10, 20, 5, *mysqlAddr, *mysqlUser, *mysqlPassword, *mysqlDb)
}

func withMysqlConnection[T any](f func(c *client.Conn) T) T {
	conn, err := mysqlPool.GetConn(context.Background())
	defer mysqlPool.PutConn(conn)
	checkErr(err)
	rv := f(conn)
	return rv
}

func DumpMysqlDb() {
	dumping.Store(true)

	var wg sync.WaitGroup
	working := make(chan bool, concurrentMysqlDumpSelects)

	// TODO paralellize with either worker or waitgroup pattern.
	for table := range *chColumns.m {
		wg.Add(1)
		go func(t string) {
			working <- true
			dumpTable(*mysqlDb, t)
			wg.Done()
		}(table)
	}

	wg.Wait()

	dumping.Store(false)
}

type DumpFieldVal interface {
	AsString() []byte
	Value() interface{}
}

func convertMysqlDumpDatesAndDecimalsToString(val *mysql.FieldValue) interface{} {
	switch val.Type {
	case mysql.FieldValueTypeString:
		return string(val.AsString())
	default:
		return val.Value()
	}
}

// TODO write some tests for this?
func dumpTable(dbName, tableName string) {
	checkErr(withMysqlConnection(func(conn *client.Conn) error {
		var result mysql.Result
		return conn.ExecuteSelectStreaming("SELECT * FROM "+dbName+"."+tableName, &result, func(row []mysql.FieldValue) error {
			values := make([]interface{}, len(row))

			for idx, val := range row {
				values[idx] = convertMysqlDumpDatesAndDecimalsToString(&val)
			}

			processDumpData(dbName, tableName, values)

			return nil
		}, nil)
	}))
}

func processDumpData(dbName string, tableName string, values []interface{}) {
	_, hasColumns := cachedChColumnsForTable(tableName)
	if !hasColumns {
		return
	}

	tableInfo := getMysqlTable(dbName, tableName)

	event := MysqlReplicationRowEvent{
		Table:  tableInfo,
		Rows:   [][]interface{}{values},
		Action: "dump",
	}

	OnRow(&event)
}
