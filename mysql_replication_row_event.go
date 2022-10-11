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
	EventId                string
	Table                  *schema.Table
	InsertData             RowInsertData
	Pks                    []Pk
	Timestamp              time.Time
	Rows                   [][]interface{}
}

// TODO replace this with storing event data in 3 columns
func EventIdString(serverId string, gtidNum uint64, gtidEventCount uint32) string {
	gtid := strconv.FormatUint(gtidNum, 10)
	gtidCount := strconv.FormatUint(uint64(gtidEventCount), 10)

	// optimized to reduce allocations vs sprintf or string concatenation.
	eid := strings.Builder{}
	eid.Grow(len(serverId) + len(gtid) + len(gtidCount) + 2)
	eid.WriteString(serverId)
	eid.WriteRune(':')
	eid.WriteString(gtid)
	eid.WriteRune('#')
	eid.WriteString(gtidCount)

	return eid.String()
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

	var serverId, eventId string
	var transactionId uint64

	// TODO re-evaluate the use of this - it's only needed now for the local dedupe..
	if e.Action != "dump" {
		serverId = e.ServerId
		transactionId = e.TransactionId
		eventId = EventIdString(e.ServerId, e.TransactionId, e.TransactionEventNumber)
	} else {
		serverId = "dump"
		eventId = fmt.Sprintf("dump:%s#0", e.PkString())
	}

	insertData[eventCreatedAtColumnName] = e.Timestamp
	insertData[eventActionColumnName] = e.Action
	insertData[eventServerIdColumnName] = serverId
	insertData[eventTransactionIdColumnName] = transactionId
	insertData[eventTransactionEventNumberColumnName] = e.TransactionEventNumber

	e.EventId = eventId
	e.InsertData = insertData

	return false
}
