package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
)

// TODO replace all these events with enum types that are uint8 underneath
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
	DumpAction   = "dump"
)

type RowInsertData map[string]interface{}

type MysqlReplicationRowEvent struct {
	TransactionEventNumber uint32
	TransactionId          uint64
	Action                 string
	ServerId               string
	Table                  *schema.Table
	InsertData             RowInsertData
	Pks                    []Pk
	Timestamp              time.Time
	Rows                   [][]interface{}
}

func (e *MysqlReplicationRowEvent) IsDumpEvent() bool {
	return e.Action == DumpAction
}

type DuplicateBinlogEventKey = struct {
	TransactionEventNumber uint32 `ch:"changelog_gtid_transaction_event_number"`
	TransactionId          uint64 `ch:"changelog_gtid_transaction_id"`
	ServerId               string `ch:"changelog_gtid_server_id"`
}

func (e *MysqlReplicationRowEvent) DedupeKey() DuplicateBinlogEventKey {
	return DuplicateBinlogEventKey{
		ServerId:               e.ServerId,
		TransactionId:          e.TransactionId,
		TransactionEventNumber: e.TransactionEventNumber,
	}
}

func (e *MysqlReplicationRowEvent) GtidRangeString() string {
	if !e.IsDumpEvent() {
		return e.ServerId + ":1-" + strconv.FormatUint(e.TransactionId, 10)
	} else {
		return ""
	}
}

func (e *MysqlReplicationRowEvent) InsertDataFromRows(columns *ChColumnSet) (data RowInsertData, isDuplicate bool) {
	data = make(RowInsertData, len(columns.columns))

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
		if columns.columnLookup[columnName] {
			if isDuplicate &&
				hasPreviousEvent &&
				!State.cachedMatchers.MemoizedRegexpsMatch(columnName, Config.IgnoredColumnsForDeduplication) &&
				!reflect.DeepEqual(row[i], previousRow[i]) {
				isDuplicate = false
			}

			convertedValue := parseValue(row[i], c.Type, tableName, columnName)
			data[c.Name] = convertedValue
		}
	}

	return data, isDuplicate
}

func (e *MysqlReplicationRowEvent) PkString() string {
	pksb := strings.Builder{}
	pksb.Grow(len(e.Pks) * 8)

	for i, pk := range e.Pks {
		pksb.WriteString(fmt.Sprintf("%v", pk.Value))

		if i < len(e.Pks)-1 {
			pksb.WriteRune('-')
		}
	}

	return pksb.String()
}

func (e *MysqlReplicationRowEvent) buildPk(insertData RowInsertData) (PKValues Pks) {
	PKValues = make(Pks, 0, len(e.Table.PKColumns))

	for _, idx := range e.Table.PKColumns {
		column := e.Table.Columns[idx]
		value := insertData[column.Name]

		if value != nil {
			PKValues = append(PKValues, Pk{
				ColumnName: column.Name,
				Value:      value,
			})
		}
	}

	return PKValues
}

// TODO test with all types we care about - yaml conversion, etc.
// dedupe for yaml columns according to filtered values?
func (e *MysqlReplicationRowEvent) ParseInsertData(columns *ChColumnSet) (IsDuplicate bool) {
	insertData, isDuplicate := e.InsertDataFromRows(columns)

	if isDuplicate {
		return true
	}

	e.Pks = e.buildPk(insertData)

	insertData[eventCreatedAtColumnName] = e.Timestamp
	insertData[eventActionColumnName] = e.Action
	insertData[eventServerIdColumnName] = e.ServerId
	insertData[eventTransactionIdColumnName] = e.TransactionId
	insertData[eventTransactionEventNumberColumnName] = e.TransactionEventNumber

	e.InsertData = insertData

	return false
}
