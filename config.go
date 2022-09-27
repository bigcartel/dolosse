package main

import (
	"flag"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v3"
)

type GlobalConfig struct {
	ClickhouseDb,
	ClickhouseUsername,
	ClickhouseAddr,
	ClickhousePassword,
	MysqlAddr,
	MysqlUser,
	MysqlPassword,
	MysqlDb,
	StartFromGtid *string

	MysqlDbByte []byte

	BatchWriteInterval *time.Duration
	DumpImmediately,
	Dump,
	Rewind,
	RunProfile *bool

	IgnoredColumnsForDeduplication []string

	flagsParsed bool
}

func (c *GlobalConfig) ParseFlags(args []string) {
	if c.flagsParsed {
		return
	}

	fs := flag.NewFlagSet("MySQL -> Clickhouse binlog replicator", flag.ContinueOnError)
	// TODO add description talking about assumption and limitations.
	// Requires row based replication is enabled.
	// Assumes tables have an id primary key column which is used
	// to deduplicate on data dump.

	var _ = fs.String("config", "", "config file (optional)")
	c.Dump = fs.Bool("force-dump", false, "Reset stored binlog position and start mysql data dump after replication has caught up to present")
	c.DumpImmediately = fs.Bool("force-immediate-dump", false, "Force full immediate data dump and reset stored binlog position")
	c.Rewind = fs.Bool("rewind", false, "Reset stored binlog position and start replication from earliest available binlog event")
	c.RunProfile = fs.Bool("profile", false, "Outputs pprof profile to cpu.pprof for performance analysis")
	c.StartFromGtid = fs.String("start-from-gtid", "", "Start from gtid set")
	c.MysqlDb = fs.String("mysql-db", "bigcartel", "mysql db to dump (also available via MYSQL_DB")
	c.MysqlAddr = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db (also via MYSQL_ADDR)")
	c.MysqlUser = fs.String("mysql-user", "metabase", "mysql user (also via MYSQL_USER)")
	c.MysqlPassword = fs.String("mysql-password", "", "mysql password (also via MYSQL_PASSWORD)")
	c.ClickhouseAddr = fs.String("clickhouse-addr", "10.100.0.56:9000", "ip/url and port for destination clickhouse db (also via CLICKHOUSE_ADDR)")
	c.ClickhouseDb = fs.String("clickhouse-db", "mysql_bigcartel_binlog", "db to write binlog data to (also available via CLICKHOUSE_DB)")
	c.ClickhouseUsername = fs.String("clickhouse-username", "default", "Clickhouse username (also via CLICKHOUSE_USERNAME)")
	c.ClickhousePassword = fs.String("clickhouse-password", "", "Clickhouse password (also available via CLICKHOUSE_PASSWORD)")
	c.BatchWriteInterval = fs.Duration("batch-write-interval", 10*time.Second, "Interval of batch writes (valid values - 1m, 10s, 500ms, etc...)")
	IgnoredColumnsForDeduplication := fs.String("ignored-columns-for-deuplication", "updated-at", "Comma separated list of columns to exclude from deduplication checks")

	c.IgnoredColumnsForDeduplication = strings.Split(*IgnoredColumnsForDeduplication, ",")

	// TODO make various global configs cli args for different workloads

	must(ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	))

	c.MysqlDbByte = []byte(*Config.MysqlDb)

	c.flagsParsed = true
}

var Config = GlobalConfig{}