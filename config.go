package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v3"
	"github.com/siddontang/go-log/log"
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

	BatchSize,
	ConcurrentBatchWrites,
	ConcurrentMysqlDumpSelects *int

	BatchWriteInterval *time.Duration

	DumpImmediately,
	Dump,
	Rewind,
	RunProfile *bool

	IgnoredColumnsForDeduplication,
	AnonymizeFields,
	YamlColumns []string
}

func (c *GlobalConfig) ParseFlags(args []string) {
	*c = GlobalConfig{}

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
	c.BatchSize = fs.Int("batch-size", 100000,
		"Threshold of records needed to trigger batch write. Batch write will happen when either batch-write-interval since last batch write is exceeded, or this threshold is hit.")
	c.ConcurrentBatchWrites = fs.Int("concurrent-batch-writes", 10, "Number of batch writes of different tables to clickhouse to run in parallel")
	c.ConcurrentMysqlDumpSelects = fs.Int("concurrent-mysql-dump-selects", 4, "Number of concurrent select queries to run when dumping mysql db")

	IgnoredColumnsForDeduplication := fs.String("ignored-columns-for-deuplication", "updated-at", "Comma separated list of columns to exclude from deduplication checks")
	YamlColumns := fs.String("yaml-columns", "theme_instances.settings,theme_instances.image_sort_order,order_transactions.params", "Comma separated list of columns to parse as yaml")
	AnonymizeFields := fs.String("anonymize-fields",
		"*address*,*street*,*secret*,*postal*,*line?,*password*,*salt*,*email*,*longitude*,*latitude*,payment_methods.properties*,payment_methods.*description*,*given_name*,*surname*,*exp_*,*receipt_*",
		"Comma separated list of field name patterns to anonymize. Supports * and ? syntax, * means 0 or more of any character, ? means exactly one of any character. cool*line? will match cool.something.line1,cool-whatever-line2, etc. The pattern for the field name being matched against is #{tableName}.#{fieldName}.#{jsonFieldName}*. ")

	c.IgnoredColumnsForDeduplication = strings.Split(*IgnoredColumnsForDeduplication, ",")
	c.YamlColumns = strings.Split(*YamlColumns, ",")
	c.AnonymizeFields = strings.Split(*AnonymizeFields, ",")

	err := ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)

	if err != nil {
		if err.Error() != "error parsing commandline arguments: flag: help requested" {
			log.Infoln(err)
		}

		os.Exit(1)
	}

	c.MysqlDbByte = []byte(*Config.MysqlDb)
}

var Config = GlobalConfig{}
