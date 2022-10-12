package mysql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bigcartel/dolosse/clickhouse/cached_columns"
	"bigcartel/dolosse/concurrent_map"
	"bigcartel/dolosse/err_utils"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
	"go.uber.org/atomic"

	_ "github.com/go-mysql-org/go-mysql/driver"
)

type Mysql struct {
	Ctx           context.Context
	Pool          *client.Pool
	InitiatedDump atomic.Bool
	dbNameByte    []byte
	cfg           Config
	mysqlColumns  concurrent_map.ConcurrentMap[*schema.Table]
	dumpingTables concurrent_map.ConcurrentMap[struct{}]
	ChColumns     *cached_columns.ChColumns
}

type Config struct {
	Address               string
	User                  string
	Password              string
	DbName                string
	DumpTables            map[string]bool
	ConcurrentDumpQueries int
}

func InitMysql(ctx context.Context, cachedColumns *cached_columns.ChColumns, cfg Config) Mysql {
	return Mysql{
		Ctx:           ctx,
		Pool:          client.NewPool(log.Debugf, 10, 20, 5, cfg.Address, cfg.User, cfg.Password, cfg.DbName),
		cfg:           cfg,
		dbNameByte:    []byte(cfg.DbName),
		dumpingTables: concurrent_map.NewConcurrentMap[struct{}](),
		mysqlColumns:  concurrent_map.NewConcurrentMap[*schema.Table](),
		ChColumns:     cachedColumns,
	}
}

func (db *Mysql) GetEarliestGtidStartPoint() string {
	return withMysqlConnection(db, func(conn *client.Conn) string {
		gtidString := db.GetMysqlVariable(conn, "@@GLOBAL.gtid_purged")

		if len(gtidString) == 0 {
			serverUuid := db.GetMysqlVariable(conn, "@@GLOBAL.server_uuid")
			gtidString = serverUuid + ":1"
		}

		return gtidString
	})
}

func (db *Mysql) GetMysqlVariable(conn *client.Conn, variable string) string {
	rr, err := conn.Execute("select " + variable)
	err_utils.Must(err)

	return string(rr.Values[0][0].AsString())
}

func (db *Mysql) GetMysqlTableNames() []string {
	return withMysqlConnection(db, func(conn *client.Conn) []string {
		rr, err := conn.Execute(fmt.Sprintf("SHOW TABLES FROM %s", db.cfg.DbName))
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
func (db *Mysql) GetMysqlTable(dbName, table string) *schema.Table {
	v := db.mysqlColumns.Get(table)

	if v != nil {
		return *v
	} else {
		return withMysqlConnection(db, func(conn *client.Conn) *schema.Table {
			t, err := schema.NewTable(conn, dbName, table)
			err_utils.Must(err)
			db.mysqlColumns.Set(table, &t)
			return t
		})
	}
}

func withMysqlConnection[T any](db *Mysql, f func(c *client.Conn) T) T {
	conn := err_utils.Unwrap(db.Pool.GetConn(db.Ctx))
	defer db.Pool.PutConn(conn)
	return f(conn)
}

type RowHandler func(*MysqlReplicationRowEvent)

type tableDumpedStatusManager interface {
	SetTableDumped(string, bool)
	GetTableDumped(string) bool
}

func (db *Mysql) DumpMysqlDb(dtm tableDumpedStatusManager, forceDump bool, onRow RowHandler) {
	var wg sync.WaitGroup
	working := make(chan bool, db.cfg.ConcurrentDumpQueries)
	db.ChColumns.M.Range(func(k, _ any) bool {
		table := k.(string)
		if db.dumpingTables.Get(table) == nil && db.cfg.DumpTables[table] || forceDump || !dtm.GetTableDumped(table) {
			log.Infof("Begin dump of %s", table)
			db.dumpingTables.Set(table, &struct{}{})
			dtm.SetTableDumped(table, false) // reset if force dump

			wg.Add(1)

			go func(t string) {
				working <- true
				err_utils.Must(db.dumpTable(db.cfg.DbName, t, onRow))
				dtm.SetTableDumped(table, true)
				<-working
				wg.Done()
			}(table)
		}

		return true
	})

	wg.Wait()
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

func (db *Mysql) dumpTable(dbName, tableName string, onRow func(*MysqlReplicationRowEvent)) error {
	return withMysqlConnection(db, func(conn *client.Conn) error {
		var result mysql.Result
		var i uint64 = 1
		return conn.ExecuteSelectStreaming("SELECT * FROM "+dbName+"."+tableName, &result, func(row []mysql.FieldValue) error {
			values := make([]interface{}, len(row))

			for idx, val := range row {
				values[idx] = copyDumpStringValues(&val)
			}

			event := db.processDumpData(dbName, tableName, i, values)
			onRow(&event)
			i++

			return nil
		}, nil)
	})
}

func (db *Mysql) processDumpData(dbName string, tableName string, eventNumber uint64, values []interface{}) MysqlReplicationRowEvent {
	_, hasColumns := db.ChColumns.ColumnsForTable(tableName)
	if !hasColumns {
		return MysqlReplicationRowEvent{}
	}

	tableInfo := db.GetMysqlTable(dbName, tableName)

	return MysqlReplicationRowEvent{
		Table:     tableInfo,
		Rows:      [][]interface{}{values},
		Timestamp: time.Now(),
		Action:    DumpAction,
		// imperfect, but effective enough since when dumping rows are returned in a consistent order
		TransactionId: eventNumber,
		ServerId:      "dump",
	}
}
