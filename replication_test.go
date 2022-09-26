package main

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func withDbSetup(t *testing.T, f func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb)) {
	parseFlags([]string{
		"--mysql-addr=0.0.0.0:3307",
		"--mysql-user=root",
		"--mysql-db=test",
		"--mysql-password=",
		"--clickhouse-addr=0.0.0.0:9001",
		"--batch-write-interval=0",
		"--clickhouse-db=test",
	})

	mysqlConn, err := client.Connect(*mysqlAddr, *mysqlUser, "", "", func(c *client.Conn) {
		c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
	})

	if err != nil {
		t.Error(err)
	}

	clickhouseConn, err := establishClickhouseConnection()
	if err != nil {
		t.Error(err)
	}
	clickhouseConn.Setup()

	f(mysqlConn, clickhouseConn)
}

func checkMysqlResults(_ *mysql.Result, err error) {
	must(err)
}

func execMysqlStatements(mysqlConn *client.Conn, statements string) {
	unwrap(mysqlConn.ExecuteMultiple(statements, checkMysqlResults))
}

func execChStatements(chDb ClickhouseDb, statements ...string) {
	for i := range statements {
		err := chDb.conn.Exec(context.TODO(), statements[i])
		must(err)
	}
}

func getChRows(t *testing.T, chDb ClickhouseDb, query string, expectedRowCount int) [][]interface{} {
	waitingForRowAttempts := 0
	var r [][]interface{}
	for waitingForRowAttempts < 20 {
		r = chDb.Query("select * from test.test order by changelog_event_created_at")
		if len(r) < 2 {
			time.Sleep(50 * time.Millisecond)
			waitingForRowAttempts++
		} else {
			break
		}
	}

	if waitingForRowAttempts == 20 {
		t.Error("Failed to fetch expected rows, got ", r)
	}

	return r
}

func InitDbs(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
	execMysqlStatements(mysqlConn, `
			CREATE DATABASE IF NOT EXISTS test;
			USE test;
			DROP TABLE IF EXISTS test;
			CREATE TABLE test (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL DEFAULT '',
				price decimal(10,2) NOT NULL DEFAULT '0.00',
				visits int NOT NULL DEFAULT 0,
				description text,
				created_at datetime NOT NULL DEFAULT NOW(),
				PRIMARY KEY (id)
			);
			INSERT INTO test (id, name, price, description, created_at)
			VALUES (
				1,
				"test thing",
				"12.31",
				"my cool description",
				NOW()
			);

			UPDATE test SET visits=1 WHERE id = 1;
			UPDATE test SET price="12.33" WHERE id = 1;
			UPDATE test SET visits=2 WHERE id = 1;
			`)

	execChStatements(clickhouseConn,
		`DROP TABLE IF EXISTS test.test`,
		`CREATE TABLE test.test (
				id Int32,
				name String,
				price Decimal(10, 2),
				description Nullable(String),
				created_at DateTime,
				changelog_id String,
				changelog_action LowCardinality(String),
				changelog_event_created_at DateTime64(9)
			)
		  ENGINE = MergeTree
			ORDER BY (id)
			`)
}

func TestBasicReplication(t *testing.T) {
	withDbSetup(t, func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
		startGtid := getMysqlVariable(mysqlConn, "@@GLOBAL.gtid_executed")
		startFromGtid = &startGtid

		InitDbs(mysqlConn, clickhouseConn)

		go startSync()

		r := getChRows(t, clickhouseConn, "select * from test.test order by changelog_event_created_at asc", 2)

		if len(r) > 2 {
			t.Fatal("Expected 2 replicated rows, got ", len(r), " ", r)
		}

		assert.Equal(t, int32(1), r[0][0].(int32), "replicated id should match")
		assert.Equal(t, "test thing", r[0][1].(string), "replicated name should match")
		assert.Equal(t, unwrap(decimal.NewFromString("12.31")), r[0][2].(decimal.Decimal), "replicated price should match")
		assert.Equal(t, "my cool description", **r[0][3].(**string), "replicated description should match")
		assert.Equal(t, unwrap(decimal.NewFromString("12.33")), r[1][2].(decimal.Decimal), "second replicated price should match")
	})
}
