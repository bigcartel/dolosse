package app

import (
	"bigcartel/dolosse/clickhouse"
	"bigcartel/dolosse/err_utils"
	"fmt"
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

const batchSize = 10000

func withDbSetup(t testing.TB, f func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb)) {
	app := NewApp(true, []string{
		"--mysql-addr=0.0.0.0:3307",
		"--mysql-user=root",
		"--mysql-db=test",
		"--mysql-password=",
		"--clickhouse-addr=0.0.0.0:9001",
		"--batch-write-interval=10ms",
		fmt.Sprintf("--batch-size=%d", batchSize),
		"--clickhouse-db=test",
	})

	mysqlConn, err := client.Connect(*app.Config.MysqlAddr, *app.Config.MysqlUser, "", "", func(c *client.Conn) {
		c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
	})

	if err != nil {
		t.Fatal(err)
	}

	clickhouseConn := err_utils.Unwrap(clickhouse.EstablishClickhouseConnection(app.Ctx, clickhouse.Config{
		Address:  *app.Config.ClickhouseAddr,
		Username: *app.Config.ClickhouseUsername,
		Password: *app.Config.ClickhousePassword,
		DbName:   *app.Config.ClickhouseDb,
	}))

	f(&app, mysqlConn, clickhouseConn)
}

func checkMysqlResults(_ *mysql.Result, err error) {
	err_utils.Must(err)
}

func execMysqlStatements(mysqlConn *client.Conn, statements string) {
	err_utils.Unwrap(mysqlConn.ExecuteMultiple(statements, checkMysqlResults))
}

func execChStatements(chDb clickhouse.ClickhouseDb, statements ...string) {
	for i := range statements {
		err_utils.Must(chDb.Conn.Exec(chDb.Ctx, statements[i]))
	}
}

func getChRows[T any](t *testing.T, chDb clickhouse.ClickhouseDb, query string, expectedRowCount int) []T {
	time.Sleep(50 * time.Millisecond)

	waitingForRowAttempts := 0
	var r []T
	for waitingForRowAttempts < 20 {
		result := err_utils.Unwrap(chDb.Conn.Query(chDb.Ctx, query))

		ts := make([]T, 0, 1000)
		for result.Next() {
			var t T
			err_utils.Must(result.ScanStruct(&t))
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
	PriceTwo        uint64          `ch:"price_two"`
	Description     string          `ch:"description"`
	TsTwo           time.Time       `ch:"ts_two"`
	ChangelogAction string          `ch:"changelog_action"`
	Zerp            int32           `ch:"zerp"`
}

func getTestRows(t *testing.T, chDb clickhouse.ClickhouseDb, expectedRowCount int) []TestRow {
	return getChRows[TestRow](t, chDb, `
		select id,
			name,
			price,
			price_two,
			description,
			ts_two,
			changelog_action
			from test.test
			order by changelog_event_created_at asc
	  `, expectedRowCount)
}

func InitDbs(mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb, two_pks bool) {
	pks := "PRIMARY KEY (id)"

	if two_pks {
		pks = "PRIMARY KEY (id, label)"
	}

	execMysqlStatements(mysqlConn, fmt.Sprintf(`
			RESET MASTER;
			CREATE DATABASE IF NOT EXISTS test;
			USE test;
			DROP TABLE IF EXISTS test;
			CREATE TABLE test (
				id int unsigned NOT NULL AUTO_INCREMENT,
				account_id int unsigned NOT NULL DEFAULT 0,
				label varchar(100) NOT NULL DEFAULT 'asdf',
				name varchar(100) NOT NULL DEFAULT '',
				price decimal(10,2) NOT NULL DEFAULT '0.00',
				price_two decimal(10,2) NOT NULL DEFAULT '0.00',
				visits int NOT NULL DEFAULT 0,
				description text,
				created_at datetime NOT NULL DEFAULT NOW(),
				ts_two datetime NOT NULL DEFAULT NOW(),
				%s
			);
			INSERT INTO test (id, name, price, price_two, description, created_at)
			VALUES (
				1,
				"test thing",
				"12.31",
				"12.50",
				"my cool description",
				NOW()
			);

			UPDATE test SET visits=1 WHERE id = 1;
			UPDATE test SET price="12.33", price_two="15.33" WHERE id = 1;
			UPDATE test SET visits=2 WHERE id = 1;
			UPDATE test SET ts_two='2500-04-04' WHERE id = 1;
			UPDATE test SET ts_two='2001-01-22' WHERE id = 1;
			`, pks))

	execChStatements(clickhouseConn,
		`DROP DATABASE IF EXISTS test`,
		`CREATE DATABASE IF NOT EXISTS test`,
		`CREATE TABLE test.test (
				id Int32,
				name String,
				zerp Int32,
				price Decimal(10, 2),
				price_two UInt64,
				description Nullable(String),
				created_at DateTime,
				ts_two DateTime64(3),
				changelog_action LowCardinality(String),
				changelog_event_created_at DateTime64(9),
				changelog_gtid_server_id LowCardinality(String),
				changelog_gtid_transaction_id UInt64,
				changelog_gtid_transaction_event_number UInt32,
				changelog_updated_columns Array(LowCardinality(String))
			)
		  ENGINE = MergeTree
			ORDER BY (id)
			`)
}

func StartSync(app *App) {
	app.InitState(true)
	app.StartSync()
}

func TestBasicReplication(t *testing.T) {
	withDbSetup(t, func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb) {
		InitDbs(mysqlConn, clickhouseConn, false)

		go StartSync(app)

		time.Sleep(100 * time.Millisecond)

		r := getTestRows(t, clickhouseConn, 4)
		log.Infoln(r)

		assert.Equal(t, int32(1), r[0].Id, "replicated id should match")
		assert.Equal(t, "test thing", r[0].Name, "replicated name should match")
		assert.Equal(t, err_utils.Unwrap(decimal.NewFromString("12.31")), r[0].Price, "replicated price should match")
		assert.Equal(t, uint64(1250), r[0].PriceTwo, "replicated price_two should match")
		assert.Equal(t, "my cool description", r[0].Description, "replicated description should match")
		assert.Equal(t, err_utils.Unwrap(decimal.NewFromString("12.33")), r[1].Price, "second replicated price should match")

		app.Shutdown()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestCompositePkReplication(t *testing.T) {
	withDbSetup(t, func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb) {
		InitDbs(mysqlConn, clickhouseConn, true)
		execChStatements(clickhouseConn, "ALTER TABLE test.test ADD COLUMN label String")

		go StartSync(app)

		time.Sleep(100 * time.Millisecond)

		r := getTestRows(t, clickhouseConn, 4)

		assert.Equal(t, int32(1), r[0].Id, "replicated id should match")
		assert.Equal(t, "test thing", r[0].Name, "replicated name should match")
		assert.Equal(t, err_utils.Unwrap(decimal.NewFromString("12.31")), r[0].Price, "replicated price should match")
		assert.Equal(t, uint64(1250), r[0].PriceTwo, "replicated price_two should match")
		assert.Equal(t, "my cool description", r[0].Description, "replicated description should match")
		assert.Equal(t, err_utils.Unwrap(decimal.NewFromString("12.33")), r[1].Price, "second replicated price should match")
		fmt.Println(r[2].TsTwo)
		assert.Equal(t, 2261, r[2].TsTwo.Year(), "third replicated overflow year should be truncated")
		assert.Equal(t, 2001, r[3].TsTwo.Year(), "fourth replicated year should not be truncated")

		app.Shutdown()
		time.Sleep(100 * time.Millisecond)
	})
}

func TestReplicationAndDump(t *testing.T) {
	withDbSetup(t, func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb) {
		InitDbs(mysqlConn, clickhouseConn, false)

		// Unsure why I have to start with ithis nsert statement, but replication
		// from GTID event id 1 won't pick up on the first 1 insert after a reset
		// master for some reason
		execMysqlStatements(mysqlConn, `
			RESET MASTER;
			INSERT INTO test (id, name, price, price_two, description, created_at)
			VALUES (
				2,
				"replicated",
				"1.77",
				"12.50",
				"replicated deszzzc",
				NOW()
			);
			INSERT INTO test (id, name, price, price_two, description, created_at)
			VALUES (
				3,
				"replicated",
				"1.77",
				"12.50",
				"replicated desfeeeec",
				NOW()
			);
			INSERT INTO test (id, name, price, price_two, description, created_at)
			VALUES (
				4,
				"replicated",
				"1.77",
				"12.50",
				"replicated asdf",
				NOW()
			);

			alter table test add column zerp int;

			INSERT INTO test (id, name, price, price_two, description, created_at, zerp)
			VALUES (
				5,
				"zerped",
				"10000.77",
				"40000.91",
				"something else",
				NOW(),
				444
			);
		`)

		go StartSync(app)

		time.Sleep(100 * time.Millisecond)

		r := getChRows[TestRow](t, clickhouseConn, "select id, changelog_action, zerp from test.test order by id asc", 5)

		assert.Equal(t, int32(1), r[0].Id, "replicated id should match")
		assert.Equal(t, "dump", r[0].ChangelogAction)
		assert.Equal(t, int32(0), r[0].Zerp)
		assert.Equal(t, int32(2), r[1].Id, "replicated id should match")
		assert.Equal(t, "dump", r[1].ChangelogAction)
		assert.Equal(t, int32(0), r[1].Zerp)
		assert.Equal(t, int32(3), r[2].Id, "replicated id should match")
		assert.Equal(t, "insert", r[2].ChangelogAction)
		assert.Equal(t, int32(0), r[2].Zerp)
		assert.Equal(t, int32(4), r[3].Id, "replicated id should match")
		assert.Equal(t, "insert", r[3].ChangelogAction)
		assert.Equal(t, int32(0), r[3].Zerp)
		assert.Equal(t, int32(5), r[4].Id, "replicated id should match")
		assert.Equal(t, "insert", r[4].ChangelogAction)
		assert.Equal(t, int32(444), r[4].Zerp)

		app.Shutdown()
	})

	withDbSetup(t, func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb) {
		*app.Config.Rewind = true

		go StartSync(app)

		time.Sleep(100 * time.Millisecond)

		// it doesn't write any new rows
		getTestRows(t, clickhouseConn, 5)

		app.Shutdown()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, clickhouseConn.GetTableDumped("test"))
	})
}

func generateBenchMysqlStatements(n int) string {
	statement := strings.Builder{}
	statement.Grow(n * 50)
	for i := 2; i < n; i++ {
		statement.WriteString("INSERT INTO test (id, name, price, price_two, description, created_at) VALUES (")
		statement.WriteString(strconv.Itoa(i))
		statement.WriteString(`,"replicated",
			"1.77",
			"12.50",
			"replicated desc",
			NOW());`)
	}

	return statement.String()
}

func BenchmarkFullRun(b *testing.B) {
	for n := 0; n < b.N; n++ {
		withDbSetup(b, func(app *App, mysqlConn *client.Conn, clickhouseConn clickhouse.ClickhouseDb) {
			InitDbs(mysqlConn, clickhouseConn, false)

			// Unsure why I have to start with ithis nsert statement, but replication
			// from GTID event id 1 won't pick up on the first 1 insert after a reset
			// master for some reason
			execMysqlStatements(mysqlConn, generateBenchMysqlStatements(batchSize))

			go func() {
				for app.Stats.DeliveredRows < uint64(batchSize)-1 {
					time.Sleep(200 * time.Millisecond)
				}

				time.Sleep(500 * time.Millisecond)

				app.Shutdown()
				time.Sleep(100 * time.Millisecond)
			}()

			b.ResetTimer()
			StartSync(app)
		})
	}
}
