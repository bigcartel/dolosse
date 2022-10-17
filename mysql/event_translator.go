package mysql

import (
	"bigcartel/dolosse/cached_matchers"
	"bigcartel/dolosse/clickhouse/cached_columns"
	"bigcartel/dolosse/consts"
	"bigcartel/dolosse/err_utils"
	"bigcartel/dolosse/zeroalloc"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/city"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/goccy/go-yaml"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"
)

type EventTranslator struct {
	CachedMatchers cached_matchers.CachedMatchers
	Config         EventTranslatorConfig
}

type EventTranslatorConfig struct {
	AnonymizeFields                []*regexp.Regexp
	IgnoredColumnsForDeduplication []*regexp.Regexp
	YamlColumns                    []*regexp.Regexp
}

func NewEventTranslator(config EventTranslatorConfig) EventTranslator {
	return EventTranslator{
		CachedMatchers: cached_matchers.NewCachedMatchers(),
		Config:         config,
	}
}

// TODO test with all types we care about - yaml conversion, etc.
// dedupe for yaml columns according to filtered values?
func (t EventTranslator) PopulateInsertData(e *MysqlReplicationRowEvent, columns *cached_columns.ChColumnSet) (IsDuplicate bool) {
	insertData, isDuplicate := t.InsertDataFromRows(e, columns)

	if isDuplicate {
		return true
	}

	e.Pks = e.buildPk(insertData)

	insertData[consts.EventCreatedAtColumnName] = e.Timestamp
	insertData[consts.EventActionColumnName] = e.Action
	insertData[consts.EventServerIdColumnName] = e.ServerId
	insertData[consts.EventTransactionIdColumnName] = e.TransactionId
	insertData[consts.EventTransactionEventNumberColumnName] = e.TransactionEventNumber

	e.InsertData = insertData

	return false
}

func (t EventTranslator) InsertDataFromRows(e *MysqlReplicationRowEvent, columns *cached_columns.ChColumnSet) (data RowInsertData, isDuplicate bool) {
	data = make(RowInsertData, len(columns.Columns))

	var previousRow []interface{}
	tableName := e.Table.Name
	hasPreviousEvent := len(e.Rows) == 2

	newEventIdx := len(e.Rows) - 1

	if hasPreviousEvent {
		previousRow = e.Rows[0]
	}

	row := e.Rows[newEventIdx]

	isDuplicate = false
	if e.Action == "update" {
		isDuplicate = true
	}

	for i, c := range e.Table.Columns {
		columnName := c.Name
		if columns.ColumnLookup[columnName] {
			if isDuplicate &&
				hasPreviousEvent &&
				!t.memoizedRegexpsMatch(columnName, t.Config.IgnoredColumnsForDeduplication) &&
				!reflect.DeepEqual(row[i], previousRow[i]) {
				isDuplicate = false
			}

			convertedValue := t.ParseValue(row[i], c.Type, tableName, columnName)
			data[c.Name] = convertedValue
		}
	}

	return data, isDuplicate
}

func (t EventTranslator) ParseValue(value interface{}, columnType int, tableName string, columnName string) interface{} {
	value = t.convertMysqlColumnType(value, columnType)

	switch v := value.(type) {
	case []byte:
		return t.parseString(string(v), tableName, columnName)
	case string:
		return t.parseString(v, tableName, columnName)
	default:
		return value
	}
}

func (t EventTranslator) parseString(value string, tableName string, columnName string) interface{} {
	var out interface{}

	if t.isYamlColumn(tableName, columnName) {
		y := make(map[string]interface{})

		err := yaml.Unmarshal(zeroalloc.StringToByteSlice(value), &y)

		if err != nil {
			y["rawYaml"] = value
			y["errorParsingYaml"] = err
			log.Errorln(value)
			log.Errorln(err)
		}

		for k, v := range y {
			delete(y, k)
			v = t.anonymizeValue(v, tableName, t.fieldString(columnName, k))
			y[stripLeadingColon(k)] = v
		}

		out, err = json.Marshal(y)
		err_utils.Must(err)
	} else {
		out = t.anonymizeValue(value, tableName, columnName)
	}

	return out
}

// TODO should this be injected and be a convert function the translator is initiated with?
const MysqlDateFormat = "2006-01-02"

func (t EventTranslator) convertMysqlColumnType(value interface{}, columnType int) interface{} {
	if value == nil {
		return value
	}
	switch columnType {
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.ParseInLocation(mysql.TimeFormat, vs, time.UTC)
			err_utils.Must(err)
			return vt
		} else {
			return value
		}
	case schema.TYPE_DATE:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.Parse(MysqlDateFormat, vs)
			err_utils.Must(err)
			return vt
		} else {
			return value
		}
	case schema.TYPE_DECIMAL:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			val, err := decimal.NewFromString(vs)
			err_utils.Must(err)
			return val
		} else {
			return vs
		}
	default:
		return value
	}
}

func (t EventTranslator) isAnonymizedField(s string) bool {
	return t.memoizedRegexpsMatch(s, t.Config.AnonymizeFields)
}

func (t EventTranslator) isYamlColumn(tableName string, columnName string) bool {
	return t.memoizedRegexpsMatch(t.fieldString(tableName, columnName), t.Config.YamlColumns)
}

func (t EventTranslator) memoizedRegexpsMatch(s string, r []*regexp.Regexp) bool {
	return t.CachedMatchers.MemoizedRegexpsMatch(s, r)
}

func (t EventTranslator) fieldString(table string, columnPath string) string {
	b := strings.Builder{}
	b.Grow(len(table) + len(columnPath) + 1)
	b.WriteString(table)
	b.WriteString(".")
	b.WriteString(columnPath)
	return b.String()
}

func (t EventTranslator) hashString(s []byte) string {
	return strconv.FormatUint(city.CH64(s), 10)
}

// sanitize yaml keys that start with colon
func stripLeadingColon(s string) string {
	if s[0] == ':' {
		return s[1:]
	} else {
		return s
	}
}

// currently only supports strings
func (t EventTranslator) anonymizeValue(value interface{}, table string, columnPath string) interface{} {
	anonymize := t.isAnonymizedField(t.fieldString(table, columnPath))

	switch v := value.(type) {
	case map[string]interface{}:
		for k, subv := range v {
			delete(v, k)
			subv = t.anonymizeValue(subv, table, t.fieldString(columnPath, k))

			v[stripLeadingColon(k)] = subv
		}
	case []interface{}:
		for i := range v {
			v[i] = t.anonymizeValue(v[i], table, t.fieldString(columnPath, fmt.Sprint(i)))
		}
	case string:
		if anonymize {
			// not safe to use StringToByteSlice here
			return t.hashString([]byte(v))
		}
	case []byte:
		if anonymize {
			return t.hashString(v)
		}
	}

	return value
}
