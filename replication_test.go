package main

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
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

func TestBasicReplication(t *testing.T) {
	withDbSetup(t, func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
		startGtid := getMysqlVariable(mysqlConn, "@@GLOBAL.gtid_executed")
		startFromGtid = &startGtid

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

		go startSync()

		// TODO this breaks now because the dump kicks off before any replication happens, so there's always a dump duplicate
		// I think I need to make it so that if you explicitly set a starting gtid it doesn't auto-dump.
		r := getChRows(t, clickhouseConn, "select * from test.test order by changelog_event_created_at asc", 2)

		if len(r) > 2 {
			t.Fatal("Expected 2 replicated rows, got ", len(r), " ", r)
		}

		expectedId1 := int32(1)
		gotId1 := r[0][0].(int32)
		if expectedId1 != gotId1 {
			t.Errorf("expected id %d for row 1 got %d", expectedId1, gotId1)
		}

		expectedName1 := "test thing"
		gotName1 := r[0][1].(string)
		if expectedName1 != gotName1 {
			t.Errorf("expected name %s for row 1 got %s", expectedName1, gotName1)
		}

		expectedPrice1, _ := decimal.NewFromString("12.31")
		gotPrice1 := r[0][2].(decimal.Decimal)
		if !expectedPrice1.Equal(gotPrice1) {
			t.Errorf("expected price %s for row 1 got %s", expectedPrice1, gotPrice1)
		}

		expectedDescription1 := "my cool description"
		gotDescription1 := **r[0][3].(**string)
		if expectedDescription1 != gotDescription1 {
			t.Errorf("expected description %s for row 1 got %s", expectedDescription1, gotDescription1)
		}

		expectedPrice2, _ := decimal.NewFromString("12.33")
		gotPrice2 := r[1][2].(decimal.Decimal)
		if !expectedPrice2.Equal(gotPrice2) {
			t.Errorf("expected price %s for row 2 got %s", expectedPrice2, gotPrice2)
		}
	})
}
