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
	SkipAnonymizeFields            []*regexp.Regexp
	IgnoredColumnsForDeduplication []*regexp.Regexp
	YamlColumns                    []*regexp.Regexp
	AssumeOnlyAppendedColumns      bool
}

func NewEventTranslator(config EventTranslatorConfig) EventTranslator {
	return EventTranslator{
		CachedMatchers: cached_matchers.NewCachedMatchers(),
		Config:         config,
	}
}

// TODO test with all types we care about - yaml conversion, etc.
// dedupe for yaml columns according to filtered values?
func (t EventTranslator) PopulateInsertData(e *MysqlReplicationRowEvent, columns *cached_columns.ChColumnSet) (isDuplicate bool, isColumnMismatch bool) {
	insertData, isDuplicate, isColumnMismatch := t.InsertDataFromRows(e, columns)

	if isDuplicate {
		return
	}

	e.Pks = e.buildPk(insertData)

	insertData[consts.EventCreatedAtColumnName] = e.Timestamp
	insertData[consts.EventActionColumnName] = e.Action
	insertData[consts.EventServerIdColumnName] = e.ServerId
	insertData[consts.EventTransactionIdColumnName] = e.TransactionId
	insertData[consts.EventTransactionEventNumberColumnName] = e.TransactionEventNumber

	e.InsertData = insertData

	return
}

func (t EventTranslator) InsertDataFromRows(e *MysqlReplicationRowEvent, chColumns *cached_columns.ChColumnSet) (data RowInsertData, isDuplicate bool, isColumnMismatch bool) {
	data = make(RowInsertData, len(chColumns.Columns))

	var previousRow []interface{}
	tableName := e.Table.Name
	hasPreviousEvent := len(e.Rows) == 2

	newEventIdx := len(e.Rows) - 1

	if hasPreviousEvent {
		previousRow = e.Rows[0]
	}

	row := e.Rows[newEventIdx]

	if len(e.EventColumnNames) == 0 && !t.Config.AssumeOnlyAppendedColumns && len(row) != len(e.Table.Columns) {
		log.Errorln("Mismatch between event columns and schema table columns for table", e.Table.Name)
		log.Errorf("row: %+v", row)
		log.Errorf("table columns: %+v", e.Table.Columns)

		isColumnMismatch = true
		return
	}

	isDuplicate = false
	changedColumns := make([]string, 0, len(chColumns.Columns))
	if e.Action == "update" {
		isDuplicate = true
	}

	columnNames := e.ColumnNames()
	maxRowIdx := len(row) - 1

	for i, columnName := range columnNames {
		if i > maxRowIdx {
			log.Infof("More columns than row fields for row: %+v, column names: %+v", row, columnNames)
			break
		}

		if chColumnType, ok := chColumns.ColumnLookup[columnName]; ok {
			if hasPreviousEvent &&
				!t.memoizedRegexpsMatch(t.fieldString(columnName, tableName), t.Config.IgnoredColumnsForDeduplication) &&
				!reflect.DeepEqual(row[i], previousRow[i]) {
				changedColumns = append(changedColumns, columnName)
				isDuplicate = false
			}

			convertedValue := t.ParseValue(row[i], e.EventColumnTypes[i], tableName, columnName, chColumnType)
			data[columnName] = convertedValue
		}
	}

	if _, ok := chColumns.ColumnLookup[consts.EventUpdatedColumnsColumnName]; ok {
		data[consts.EventUpdatedColumnsColumnName] = changedColumns
	}

	return
}

func (t EventTranslator) ParseValue(value interface{}, columnType byte, tableName string, columnName string, chColumnType cached_columns.ClickhouseQueryColumn) interface{} {
	value = t.convertMysqlColumnType(value, columnType, chColumnType)

	switch v := value.(type) {
	case []byte:
		return t.parseString(string(v), tableName, columnName, chColumnType)
	case string:
		return t.parseString(v, tableName, columnName, chColumnType)
	default:
		return value
	}
}

func (t EventTranslator) parseString(value string, tableName string, columnName string, chColumnType cached_columns.ClickhouseQueryColumn) interface{} {
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
			v = t.anonymizeValue(v, tableName, t.fieldString(columnName, k), chColumnType)
			y[stripLeadingColon(k)] = v
		}

		out, err = json.Marshal(y)
		err_utils.Must(err)
	} else {
		out = t.anonymizeValue(value, tableName, columnName, chColumnType)
	}

	return out
}

// TODO should this be injected and be a convert function the translator is initiated with?
const MysqlDateFormat = "2006-01-02"

func (t EventTranslator) convertMysqlColumnType(value interface{}, columnType byte, chColumnType cached_columns.ClickhouseQueryColumn) interface{} {
	if value == nil {
		return value
	}
	switch columnType {
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME,
		mysql.MYSQL_TYPE_TIMESTAMP2, mysql.MYSQL_TYPE_DATETIME2:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.ParseInLocation(mysql.TimeFormat, vs, time.UTC)
			err_utils.Must(err)
			return vt
		} else {
			return value
		}
	case mysql.MYSQL_TYPE_DATE:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.Parse(MysqlDateFormat, vs)
			err_utils.Must(err)
			return vt
		} else {
			return value
		}
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			if chColumnType.IsInt() {
				// reflect_utils.ReflectAppend takes care of converting to
				// the nearest compatible clickhouse int column type
				return err_utils.Unwrap(strconv.ParseInt(strings.Replace(vs, ".", "", 1), 10, 64))
			} else if chColumnType.IsFloat() {
				// reflect_utils.ReflectAppend takes care of converting to
				// the nearest compatible clickhouse float column type
				return err_utils.Unwrap(strconv.ParseFloat(vs, 64))
			} else {
				val, err := decimal.NewFromString(vs)
				err_utils.Must(err)
				return val
			}
		} else {
			return vs
		}
	default:
		return value
	}
}

func (t EventTranslator) isSkippedAnonymizedField(s string) bool {
	return t.memoizedRegexpsMatch(s, t.Config.SkipAnonymizeFields)
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

func (t EventTranslator) uintHashString(s []byte) uint64 {
	return city.CH64(s)
}

func (t EventTranslator) hashString(s []byte, asUint bool) interface{} {
	uintHash := t.uintHashString(s)

	if asUint {
		return uintHash
	} else {
		return strconv.FormatUint(uintHash, 10)
	}
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
func (t EventTranslator) anonymizeValue(value interface{}, table string, columnPath string, chColumnType cached_columns.ClickhouseQueryColumn) interface{} {
	fieldString := t.fieldString(table, columnPath)
	anonymize := !t.isSkippedAnonymizedField(fieldString) && t.isAnonymizedField(fieldString)
	destColumnIsUint64 := chColumnType.IsUInt64()
	destIsSubfield := strings.Count(fieldString, ".") > 1
	writeHashAsUInt64 := destColumnIsUint64 || destIsSubfield

	switch v := value.(type) {
	case map[string]interface{}:
		for k, subv := range v {
			delete(v, k)
			subv = t.anonymizeValue(subv, table, t.fieldString(columnPath, k), chColumnType)

			v[stripLeadingColon(k)] = subv
		}
	case []interface{}:
		for i := range v {
			v[i] = t.anonymizeValue(v[i], table, t.fieldString(columnPath, fmt.Sprint(i)), chColumnType)
		}
	case string:
		if anonymize || destColumnIsUint64 {
			// not safe to use StringToByteSlice here
			return t.hashString([]byte(v), writeHashAsUInt64)
		}
	case []byte:
		if anonymize || destColumnIsUint64 {
			return t.hashString(v, writeHashAsUInt64)
		}
	}

	return value
}
