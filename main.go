package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/peterbourgon/ff/v3"
	"gopkg.in/yaml.v3"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/natefinch/atomic"
	"github.com/siddontang/go-log/log"
)

var yamlColumns = map[string]map[string]bool{
	"theme_instances": {
		"settings":          true,
		"images_sort_order": true,
	},
	"order_transactions": {
		"params": true,
	},
}

var ignoreColumns = map[string]map[string]bool{
	"theme_instances": {
		"images_marked_for_destruction": true,
	},
}

type ProcessRow struct {
	canal.DummyEventHandler
}

type RowData map[string]interface{}

type RowInsertData struct {
	Id             int64     `json:"id"`
	Table          string    `json:"table"`
	EventCreatedAt time.Time `json:"event_created_at"`
	Action         string    `json:"action"`
	Event          RowData   `json:"event"`
}

var syncCanal *canal.Canal

func interfaceToInt64(v interface{}) int64 {
	switch i := v.(type) {
	case int64:
		return i
	case int32:
		return int64(i)
	case uint32:
		return int64(i)
	default:
		return 0
	}
}

func columnInMap(tableName string, columnName string, lookup map[string]map[string]bool) bool {
	if v, ok := lookup[tableName]; ok {
		return v[columnName]
	} else {
		return false
	}
}

func isYamlColumn(tableName string, columnName string) bool {
	return columnInMap(tableName, columnName, yamlColumns)
}

/* FIXME we might go a different approach here and instead
   infer what to insert based on clickhouse table column names */
func isIgnoredColumn(tableName string, columnName string) bool {
	return columnInMap(tableName, columnName, ignoreColumns)
}

var weirdYamlKeyMatcher = regexp.MustCompile("^:(.*)")

func handleValue(value interface{}, tableName string, columnName string) interface{} {
	switch v := value.(type) {
	case []uint8:
		value = string(v)
	case string:
		if isYamlColumn(tableName, columnName) {
			y := make(map[string]interface{})
			err := yaml.Unmarshal([]byte(v), y)

			if err != nil {
				y["rawYaml"] = v
				y["errorParsingYaml"] = err
				log.Errorln(v)
				log.Errorln(err)
			}

			for k, v := range y {
				delete(y, k)
				y[weirdYamlKeyMatcher.ReplaceAllString(k, "$1")] = v
			}

			value = y
		}
	}

	return value
}

func (h *ProcessRow) OnRow(e *canal.RowsEvent) error {
	gtidSet := syncCanal.SyncedGTIDSet()
	Data := make(RowData, len(e.Rows[0]))
	var id int64
	row := e.Rows[0]

	for i, c := range e.Table.Columns {
		if !isIgnoredColumn(e.Table.Name, c.Name) {
			if c.Name == "id" {
				id = interfaceToInt64(row[i])
			}

			Data[c.Name] = handleValue(row[i], e.Table.Name, c.Name)
		}
	}

	// TODO use the row's updated_at instead of this if present
	timestamp := time.Now()

	if e.Header != nil {
		timestamp = time.Unix(int64(e.Header.Timestamp), 0)
	}

	updated_at := Data["updated_at"]

	if v, ok := updated_at.(time.Time); ok && v.Before(timestamp) {
		log.Infoln("using updated at")
		timestamp = v
	}

	insertData := RowInsertData{
		Id:             id,
		EventCreatedAt: timestamp,
		Table:          e.Table.Name,
		Action:         e.Action,
		Event:          Data,
	}

	j, err := json.Marshal(insertData)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(j))

	setGTIDString(gtidSet.String())

	return nil
}

var gtidSetKey string = "./last_synced_gtid_set"

func getGTIDSet() mysql.GTIDSet {
	file, err := os.OpenFile(gtidSetKey, os.O_RDWR, 0644)
	gtidString := make([]byte, 0)

	if errors.Is(err, os.ErrNotExist) {
		file, err := os.Create(gtidSetKey)

		if err != nil {
			log.Fatal(err)
		}

		file.Close()
	} else if err != nil {
		log.Fatal(err)
	} else {
		_, err := file.Read(gtidString)

		if err != nil {
			log.Fatal(err)
		}
	}

	log.Infoln("read gtid set", string(gtidString))
	set, err := mysql.ParseMysqlGTIDSet(string(gtidString))

	if err != nil {
		log.Fatal(err)
	}

	return set
}

func setGTIDString(s string) {
	err := atomic.WriteFile(gtidSetKey, strings.NewReader(s))

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	rotatingLog, err := log.NewTimeRotatingFileHandler("log/mysql_binlog_sync.log", 3, 0)
	if err != nil {
		panic(err)
	}

	logger := log.NewDefault(rotatingLog)
	log.SetDefaultLogger(logger)

	fs := flag.NewFlagSet("MySQL -> Clickhouse binlog replicator", flag.ContinueOnError)
	var (
		mysqlAddr     = fs.String("mysql-addr", "10.100.0.104:3306", "ip/url and port for mysql db (also via MYSQL_ADDR)")
		mysqlUser     = fs.String("mysql-user", "metabase", "mysql user (also via MYSQL_USER)")
		mysqlPassword = fs.String("mysql-password", "password", "mysql password (also via MYSQL_PASSWORD)")
		_             = fs.String("config", "", "config file (optional)")
	)

	err = ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)

	if err != nil {
		log.Fatal(err)
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = *mysqlAddr
	cfg.User = *mysqlUser
	cfg.Password = *mysqlPassword
	cfg.Dump.TableDB = "bigcartel"
	cfg.Dump.Tables = []string{"plans"}
	cfg.IncludeTableRegex = []string{"products"}
	cfg.Logger = logger

	syncCanal, err = canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	syncCanal.SetEventHandler(&ProcessRow{})

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Infoln(syncCanal.SyncedGTIDSet())
		os.Exit(1)
	}()

	syncCanal.StartFromGTID(getGTIDSet())
}
