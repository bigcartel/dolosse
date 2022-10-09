package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/assert"
)

func withDbSetup(t testing.TB, f func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb)) {
	Config.ParseFlags([]string{
		"--mysql-addr=0.0.0.0:3307",
		"--mysql-user=root",
		"--mysql-db=test",
		"--mysql-password=",
		"--clickhouse-addr=0.0.0.0:9001",
		"--batch-write-interval=10ms",
		"--clickhouse-db=test",
	})

	mysqlConn, err := client.Connect(*Config.MysqlAddr, *Config.MysqlUser, "", "", func(c *client.Conn) {
		c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
	})

	if err != nil {
		t.Error(err)
	}

	clickhouseConn := unwrap(establishClickhouseConnection())

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
		must(chDb.conn.Exec(context.TODO(), statements[i]))
	}
}

func startTestSync(ch ClickhouseDb) {
	initState(true)
	State.batchDuplicatesFilter.resetState(&ch)
	startSync()
}

func getChRows[T any](t *testing.T, chDb ClickhouseDb, query string, expectedRowCount int) []T {
	time.Sleep(50 * time.Millisecond)

	waitingForRowAttempts := 0
	var r []T
	for waitingForRowAttempts < 20 {
		result := unwrap(chDb.conn.Query(State.ctx, query))

		ts := make([]T, 0, 1000)
		for result.Next() {
			var t T
			result.ScanStruct(&t)
			ts = append(ts, t)
		}

		if len(ts) < 2 {
			time.Sleep(50 * time.Millisecond)
			waitingForRowAttempts++
		} else {
			r = ts
			break
		}
	}

	if waitingForRowAttempts == 20 {
		t.Error("Failed to fetch expected rows, got ", r)
	}

	if len(r) > expectedRowCount {
		t.Fatalf("Expected %d replicated rows, got %d - %v", expectedRowCount, len(r), r)
	}

	return r
}

type TestRow = struct {
	Id              int32           `ch:"id"`
	Name            string          `ch:"name"`
	Price           decimal.Decimal `ch:"price"`
	Description     string          `ch:"description"`
	ChangelogAction string          `ch:"changelog_action"`
}

func getTestRows(t *testing.T, chDb ClickhouseDb, expectedRowCount int) []TestRow {
	return getChRows[TestRow](t, chDb, "select id, name, price, description, changelog_action from test.test order by changelog_event_created_at asc", expectedRowCount)
}

func InitDbs(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
	execMysqlStatements(mysqlConn, `
			RESET MASTER;
			CREATE DATABASE IF NOT EXISTS test;
			USE test;
			DROP TABLE IF EXISTS test;
			CREATE TABLE test (
				id int unsigned NOT NULL AUTO_INCREMENT,
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
		`DROP DATABASE IF EXISTS test`,
		`CREATE DATABASE IF NOT EXISTS test`,
		`CREATE TABLE test.test (
				id Int32,
				name String,
				price Decimal(10, 2),
				description Nullable(String),
				created_at DateTime,
				changelog_action LowCardinality(String),
				changelog_event_created_at DateTime64(9),
				changelog_gtid_server_id LowCardinality(String),
				changelog_gtid_transaction_id UInt64,
				changelog_gtid_transaction_event_number UInt32
			)
		  ENGINE = MergeTree
			ORDER BY (id)
			`)
}

func TestBasicReplication(t *testing.T) {
	withDbSetup(t, func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
		InitDbs(mysqlConn, clickhouseConn)

		go startTestSync(clickhouseConn)

		time.Sleep(100 * time.Millisecond)

		r := getTestRows(t, clickhouseConn, 2)
		log.Infoln(r)

		assert.Equal(t, int32(1), r[0].Id, "replicated id should match")
		assert.Equal(t, "test thing", r[0].Name, "replicated name should match")
		assert.Equal(t, unwrap(decimal.NewFromString("12.31")), r[0].Price, "replicated price should match")
		assert.Equal(t, "my cool description", r[0].Description, "replicated description should match")
		assert.Equal(t, unwrap(decimal.NewFromString("12.33")), r[1].Price, "second replicated price should match")

		State.cancel()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestReplicationAndDump(t *testing.T) {
	withDbSetup(t, func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
		InitDbs(mysqlConn, clickhouseConn)

		// Unsure why I have to start with ithis nsert statement, but replication
		// from GTID event id 1 won't pick up on the first 1 insert after a reset
		// master for some reason
		execMysqlStatements(mysqlConn, `
			RESET MASTER;
			INSERT INTO test (id, name, price, description, created_at)
			VALUES (
				2,
				"replicated",
				"1.77",
				"replicated desc",
				NOW()
			);
			INSERT INTO test (id, name, price, description, created_at)
			VALUES (
				3,
				"replicated",
				"1.77",
				"replicated desc",
				NOW()
			);
			INSERT INTO test (id, name, price, description, created_at)
			VALUES (
				4,
				"replicated",
				"1.77",
				"replicated desc",
				NOW()
			);
		`)

		go startTestSync(clickhouseConn)

		time.Sleep(100 * time.Millisecond)

		r := getChRows[TestRow](t, clickhouseConn, "select id, changelog_action from test.test order by id asc", 4)

		assert.Equal(t, int32(1), r[0].Id, "replicated id should match")
		assert.Equal(t, "dump", r[0].ChangelogAction)
		assert.Equal(t, int32(2), r[1].Id, "replicated id should match")
		assert.Equal(t, "dump", r[1].ChangelogAction)
		assert.Equal(t, int32(3), r[2].Id, "replicated id should match")
		assert.Equal(t, "insert", r[2].ChangelogAction)
		assert.Equal(t, int32(4), r[3].Id, "replicated id should match")
		assert.Equal(t, "insert", r[3].ChangelogAction)

		State.cancel()
		*Config.Rewind = true

		State.batchDuplicatesFilter.resetState(&clickhouseConn)
		go startTestSync(clickhouseConn)

		time.Sleep(100 * time.Millisecond)

		// it doesn't write any new rows
		getTestRows(t, clickhouseConn, 4)

		State.cancel()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, clickhouseConn.GetTableDumped("test"))
	})
}

func generateBenchMysqlStatements(n int) string {
	statement := strings.Builder{}
	statement.Grow(n * 50)
	for i := 2; i < n; i++ {
		statement.WriteString("INSERT INTO test (id, name, price, description, created_at) VALUES (")
		statement.WriteString(strconv.Itoa(i))
		statement.WriteString(`,"replicated",
			"1.77",
			"replicated desc",
			NOW());`)
	}

	return statement.String()
}

func BenchmarkFullRun(b *testing.B) {
	for n := 0; n < b.N; n++ {
		withDbSetup(b, func(mysqlConn *client.Conn, clickhouseConn ClickhouseDb) {
			Stats.Init(true)
			InitDbs(mysqlConn, clickhouseConn)

			// Unsure why I have to start with ithis nsert statement, but replication
			// from GTID event id 1 won't pick up on the first 1 insert after a reset
			// master for some reason
			statementCount := 1000
			execMysqlStatements(mysqlConn, generateBenchMysqlStatements(statementCount))

			go func() {
				for Stats.DeliveredRows < uint64(statementCount)-1 {
					time.Sleep(200 * time.Millisecond)
				}

				time.Sleep(500 * time.Millisecond)

				State.cancel()
				time.Sleep(100 * time.Millisecond)
			}()

			startTestSync(clickhouseConn)
		})
	}
}
