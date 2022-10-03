package main

import (
	"flag"
	"os"
	"regexp"
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

	DumpTables map[string]bool

	AnonymizeFields,
	IgnoredColumnsForDeduplication,
	YamlColumns []*regexp.Regexp
}

func csvToRegexps(csv string) []*regexp.Regexp {
	s := strings.Split(csv, ",")
	var fields = make([]*regexp.Regexp, len(s))
	for i := range fields {
		fields[i] = regexp.MustCompile(s[i])
	}
	return fields
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
	c.BatchSize = fs.Int("batch-size", 200000,
		"Threshold of records needed to trigger batch write. Batch write will happen when either batch-write-interval since last batch write is exceeded, or this threshold is hit.")
	c.ConcurrentBatchWrites = fs.Int("concurrent-batch-writes", 10, "Number of batch writes of different tables to clickhouse to run in parallel")
	c.ConcurrentMysqlDumpSelects = fs.Int("concurrent-mysql-dump-selects", 4, "Number of concurrent select queries to run when dumping mysql db")

	DumpTables := fs.String("force-dump-tables", "", "Comma separate list of tables to force dump")
	IgnoredColumnsForDeduplication := fs.String("ignored-columns-for-deduplication", "updated-at", "Comma separated list of columns to exclude from deduplication checks")
	YamlColumns := fs.String("yaml-columns", "theme_instances.settings,theme_instances.image_sort_order,order_transactions.params", "Comma separated list of columns to parse as yaml")
	AnonymizeFields := fs.String("anonymize-fields",
		".*(address|street|secret|postal|line.|password|salt|email|longitude|latitude|given_name|surname|\\.exp_|receipt_).*,payment_methods.properties.*,payment_methods.*description.*",
		"Comma separated list of field name regexps to anonymize. Uses golang regexp syntax. The pattern for the field name being matched against is #{tableName}.#{fieldName}.#{jsonFieldName}*. ")

	err := ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)

	c.DumpTables = make(map[string]bool)
	for _, t := range strings.Split(*DumpTables, ",") {
		c.DumpTables[t] = true
	}

	c.IgnoredColumnsForDeduplication = csvToRegexps(*IgnoredColumnsForDeduplication)
	c.YamlColumns = csvToRegexps(*YamlColumns)
	c.AnonymizeFields = csvToRegexps(*AnonymizeFields)

	if err != nil {
		if err.Error() != "error parsing commandline arguments: flag: help requested" {
			log.Infoln(err)
		}

		os.Exit(1)
	}

	c.MysqlDbByte = []byte(*Config.MysqlDb)
}

var Config = GlobalConfig{}
