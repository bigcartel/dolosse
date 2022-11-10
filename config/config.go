package config

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v3"
)

type Config struct {
	ClickhouseDb,
	ClickhouseUsername,
	ClickhouseAddr,
	ClickhousePassword,
	MysqlAddr,
	MysqlUser,
	MysqlPassword,
	MysqlDb,
	StartFromGtid *string

	BatchSize,
	ConcurrentBatchWrites,
	ConcurrentMysqlDumpQueries *int

	BatchWriteInterval *time.Duration

	DumpImmediately,
	Dump,
	Rewind,
	AssumeOnlyAppendedColumns,
	RunProfile *bool

	DumpTables map[string]bool

	AnonymizeFields,
	SkipAnonymizeFields,
	IgnoredColumnsForDeduplication,
	// TODO specialize these with type aliases to avoid bugs
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

func NewFromFlags(args []string) (Config, error) {
	c := Config{}

	fs := flag.NewFlagSet("Dolosse", flag.ContinueOnError)
	// TODO add description talking about assumption and limitations.
	// Requires row based replication is enabled.
	// Assumes tables have an id primary key column which is used
	// to deduplicate on data dump.
	fs.Usage = func() {
		fmt.Print(`
  ____   ___  _     ___  ____ ____  _____
 |  _ \ / _ \| |   / _ \/ ___/ ___|| ____|
 | | | | | | | |  | | | \___ \___ \|  _|
 | |_| | |_| | |__| |_| |___) |__) | |___
 |____/ \___/|_____\___/|____/____/|_____|

Simple and efficient mysql binlog to clickhouse replication.

Dolosse supports:
- Parsing columns containing YAML and converting them to JSON
- Stably hashing fields of sensitive user data to anonymize it (even nested within YAML columns) while still providing a means for querying that data.
- Only replicating the mysql columns that are also present in the destination clickhouse table and deduplicating row data. Any events which would result in a duplicate given the subset of columns in the destination table will be ignored.
- Pulling the maximum amount of binlog data available, and dumping only data not already present in the binlog.
- Deduplicating binlog data such that you can rewind the syncer or re-run a database dump at any time without fear of writing duplicate data.

You'll need to set binlog_format = 'ROW' for Dolosse to work.
To have the most seamless handling of changes in schema over time it's recommended to also set binlog_row_metadata = 'FULL'.
Dolosse will run fine with binlog_row_metadata = 'MINIMAL' but will fall back to assuming newly added columns are appended
to the table, or skipping historical events on schema mismatch with --asume-only-append-columns=false.

All flags can also be specified as environment variables.
--clickhouse-db=test becomes DOLOSSE_CLICKHOUSE_DB=test

Flags:
`)
		fs.PrintDefaults()
	}

	var _ = fs.String("config", "", "config file (optional)")
	c.Dump = fs.Bool("force-dump", false, "Reset stored binlog position and start mysql data dump after replication has caught up to present")
	c.DumpImmediately = fs.Bool("force-immediate-dump", false, "Force full immediate data dump and reset stored binlog position")
	c.Rewind = fs.Bool("rewind", false, "Reset stored binlog position and start replication from earliest available binlog event")
	c.AssumeOnlyAppendedColumns = fs.Bool("assume-only-appended-columns", true, `Assume that any ALTER TABLE ADD COLUMN statements are appended to the end of the table.
Meaning they don't use ADD COLUMN FIRST|BEFORE|AFTER. Only relevant if is not set to BINLOG_ROW_METADATA=FULL`)
	c.RunProfile = fs.Bool("profile", false, "Outputs pprof profile to cpu.pprof for performance analysis")
	c.StartFromGtid = fs.String("start-from-gtid", "", "Start from gtid set")
	c.MysqlDb = fs.String("mysql-db", "bigcartel", "mysql db to dump")
	c.MysqlAddr = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db")
	c.MysqlUser = fs.String("mysql-user", "metabase", "mysql user")
	c.MysqlPassword = fs.String("mysql-password", "", "mysql password")
	c.ClickhouseAddr = fs.String("clickhouse-addr", "10.100.0.56:9000", "ip/url and port for destination clickhouse db")
	c.ClickhouseDb = fs.String("clickhouse-db", "mysql_bigcartel_binlog", "db to write binlog data to")
	c.ClickhouseUsername = fs.String("clickhouse-username", "default", "Clickhouse username")
	c.ClickhousePassword = fs.String("clickhouse-password", "", "Clickhouse password")
	c.BatchWriteInterval = fs.Duration("batch-write-interval", 10*time.Minute, "Interval of batch writes (valid values - 1m, 10s, 500ms, etc...)")
	c.BatchSize = fs.Int("batch-size", 200000,
		"Threshold of records needed to trigger batch write. Batch write will happen when either batch-write-interval since last batch write is exceeded, or this threshold is hit.")
	c.ConcurrentBatchWrites = fs.Int("concurrent-batch-writes", 10, "Number of batch writes of different tables to clickhouse to run in parallel")
	c.ConcurrentMysqlDumpQueries = fs.Int("concurrent-mysql-dump-selects", 4, "Number of concurrent select queries to run when dumping mysql db")

	DumpTables := fs.String("force-dump-tables", "", "Comma separate list of tables to force dump")
	IgnoredColumnsForDeduplication := fs.String("ignored-columns-for-deduplication", "updated-at", "Comma separated list of columns to exclude from deduplication checks")
	YamlColumns := fs.String("yaml-columns", "theme_instances.settings,theme_instances.image_sort_order,order_transactions.params", "Comma separated list of columns to parse as yaml")
	AnonymizeFields := fs.String("anonymize-fields",
		".*(address|street|secret|postal|line.|password|salt|email|longitude|latitude|given_name|surname|\\.exp_|receipt_).*,payment_methods.properties.*,payment_methods.*description.*",
		"Comma separated list of field name regexps to anonymize. Uses golang regexp syntax. The pattern for the field name being matched against is '{tableName}.{fieldName}.{jsonFieldName}*'. ")

	SkipAnonymizeFields := fs.String("skip-anonymize-fields",
		".*\\..*_type$",
		"Comma separated list of field name regexps to explicitly not anonymize. Uses golang regexp syntax. The pattern for the field name being matched against is '{tableName}.{fieldName}.{jsonFieldName}*'. ")
	err := ff.Parse(fs, args,
		ff.WithEnvVarPrefix("DOLOSSE"),
	)

	c.DumpTables = make(map[string]bool)
	for _, t := range strings.Split(*DumpTables, ",") {
		c.DumpTables[t] = true
	}

	c.IgnoredColumnsForDeduplication = csvToRegexps(*IgnoredColumnsForDeduplication)
	c.YamlColumns = csvToRegexps(*YamlColumns)
	c.AnonymizeFields = csvToRegexps(*AnonymizeFields)
	c.SkipAnonymizeFields = csvToRegexps(*SkipAnonymizeFields)

	return c, err
}
