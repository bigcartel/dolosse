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

var mysqlPoolInitialized = false
var mysqlPool *client.Pool
var mysqlColumns = NewConcurrentMap[*schema.Table]()

func getEarliestGtidStartPoint() string {
	return withMysqlConnection(func(conn *client.Conn) string {
		gtidString := getMysqlVariable(conn, "@@GLOBAL.gtid_purged")

		if len(gtidString) == 0 {
			serverUuid := getMysqlVariable(conn, "@@GLOBAL.server_uuid")
			gtidString = serverUuid + ":1"
		}

		return gtidString
	})
}

func getMysqlVariable(conn *client.Conn, variable string) string {
	rr, err := conn.Execute("select " + variable)
	must(err)

	return string(rr.Values[0][0].AsString())
}

func getMysqlTableNames() []string {
	return withMysqlConnection(func(conn *client.Conn) []string {
		rr, err := conn.Execute(fmt.Sprintf("SHOW TABLES FROM %s", *Config.MysqlDb))
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
	v := mysqlColumns.Get(table)

	if v != nil {
		return *v
	} else {
		return withMysqlConnection(func(conn *client.Conn) *schema.Table {
			t, err := schema.NewTable(conn, db, table)
			must(err)
			mysqlColumns.Set(table, &t)
			return t
		})
	}
}

func initMysqlConnectionPool() {
	if !mysqlPoolInitialized {
		mysqlPool = client.NewPool(log.Debugf, 10, 20, 5, *Config.MysqlAddr, *Config.MysqlUser, *Config.MysqlPassword, *Config.MysqlDb)
		mysqlPoolInitialized = true
	}
}

func withMysqlConnection[T any](f func(c *client.Conn) T) T {
	conn, err := mysqlPool.GetConn(context.Background())
	defer mysqlPool.PutConn(conn)
	must(err)
	rv := f(conn)
	return rv
}

var dumpingTables = NewConcurrentMap[struct{}]()

func DumpMysqlDb(chConn *ClickhouseDb, forceDump bool) {
	var wg sync.WaitGroup
	working := make(chan bool, concurrentMysqlDumpSelects)

	// TODO paralellize with either worker or waitgroup pattern.
	for table := range *chColumns.m {
		if dumpingTables.Get(table) == nil && !chConn.GetTableDumped(table) || forceDump {
			dumpingTables.Set(table, &struct{}{})
			chConn.SetTableDumped(table, false) // reset if force dump

			wg.Add(1)

			go func(t string) {
				working <- true
				dumpTable(*Config.MysqlDb, t)
				chConn.SetTableDumped(table, true)
				wg.Done()
			}(table)
		}
	}

	wg.Wait()

	dumped.Store(true)
}

type DumpFieldVal interface {
	AsString() []byte
	Value() interface{}
}

// We copy these because each val passed in is using a shared and re-used buffer
// so data will become corrupted if we don't copy.
func copyDumpStringValues(val *mysql.FieldValue) interface{} {
	switch val.Type {
	case mysql.FieldValueTypeString:
		return string(val.AsString())
	default:
		return val.Value()
	}
}

// TODO write some tests for this?
func dumpTable(dbName, tableName string) {
	must(withMysqlConnection(func(conn *client.Conn) error {
		var result mysql.Result
		return conn.ExecuteSelectStreaming("SELECT * FROM "+dbName+"."+tableName, &result, func(row []mysql.FieldValue) error {
			values := make([]interface{}, len(row))

			for idx, val := range row {
				values[idx] = copyDumpStringValues(&val)
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
