package main

import (
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
)

type RowData map[string]interface{}

type MysqlReplicationRowEvent struct {
	TransactionEventNumber uint32
	TransactionId          uint64
	RecordId               int64
	Action                 string
	ServerId               string
	EventId                string
	Table                  *schema.Table
	InsertData             RowData
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
	return e.Action == "dump"
}

type DuplicateBinlogEventKey = struct {
	ServerId               string `ch:"changelog_gtid_server_id"`
	TransactionId          uint64 `ch:"changelog_gtid_transaction_id"`
	TransactionEventNumber uint32 `ch:"changelog_gtid_transaction_event_number"`
}

func (e *MysqlReplicationRowEvent) DedupeKey() DuplicateBinlogEventKey {
	return DuplicateBinlogEventKey{
		ServerId:               e.ServerId,
		TransactionId:          e.TransactionId,
		TransactionEventNumber: e.TransactionEventNumber,
	}
}

// TODO hoist this up into RowInsertData
func (e *MysqlReplicationRowEvent) GtidRangeString() string {
	if !e.IsDumpEvent() {
		return e.ServerId + ":1-" + strconv.FormatUint(e.TransactionId, 10)
	} else {
		return ""
	}
}

// TODO test with all types we care about - yaml conversion, etc.
// dedupe for yaml columns according to filtered values?
func (e *MysqlReplicationRowEvent) ParseInsertData(columns *ChColumnSet) (IsDuplicate bool) {
	Event := make(RowData, len(columns.columns))

	var previousRow []interface{}
	tableName := e.Table.Name
	hasPreviousEvent := len(e.Rows) == 2

	newEventIdx := len(e.Rows) - 1

	if hasPreviousEvent {
		previousRow = e.Rows[0]
	}

	row := e.Rows[newEventIdx]

	isDuplicate := false
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
			Event[c.Name] = convertedValue
		}
	}

	if isDuplicate {
		return true
	}

	id := toInt64(Event["id"])
	var serverId, eventId string
	var transactionId uint64
	// TODO re-evaluate the use of this - it's only needed now for the local dedupe..
	if e.Action != "dump" {
		serverId = e.ServerId
		transactionId = e.TransactionId
		eventId = EventIdString(e.ServerId, e.TransactionId, e.TransactionEventNumber)
	} else {
		serverId = "dump"
		transactionId = uint64(id)
		eventId = EventIdString("dump", uint64(id), 0)
	}

	Event[eventCreatedAtColumnName] = e.Timestamp
	Event[eventActionColumnName] = e.Action
	Event[eventServerIdColumnName] = serverId
	Event[eventTransactionIdColumnName] = transactionId
	Event[eventTransactionEventNumberColumnName] = e.TransactionEventNumber

	e.RecordId = id
	e.EventId = eventId
	e.InsertData = Event

	return false
}
