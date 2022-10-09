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

type MysqlReplicationRowEvent struct {
	Table                  *schema.Table
	Rows                   [][]interface{}
	Action                 string
	Timestamp              time.Time
	ServerId               string
	TransactionEventNumber uint32
	TransactionId          uint64
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
func (e *MysqlReplicationRowEvent) ToClickhouseRowData(columns *ChColumnSet) (ClickhouseRowData *RowInsertData, IsDuplicate bool) {
	insertData := RowInsertDataPool.Get().(*RowInsertData)
	insertData.Reset()
	insertData.RawEvent = e

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
			insertData.Event[c.Name] = convertedValue
		}
	}

	var timestamp time.Time
	if !e.Timestamp.IsZero() {
		timestamp = e.Timestamp
	} else {
		timestamp = time.Now()
	}

	insertData.Event[eventCreatedAtColumnName] = timestamp

	if isDuplicate {
		return insertData, true
	}

	insertData.Event[eventActionColumnName] = e.Action

	id := toInt64(insertData.Event["id"])
	insertData.Id = toInt64(id)
	var serverId, eventId string
	// TODO re-evaluate the use of this - it's only needed now for the local dedupe..
	if e.Action != "dump" {
		serverId = e.ServerId
		eventId = EventIdString(e.ServerId, e.TransactionId, e.TransactionEventNumber)
	} else {
		serverId = "dump"
		eventId = EventIdString("dump", uint64(id), 0)
	}

	insertData.Event[eventIdColumnName] = eventId
	insertData.ServerId = serverId
	insertData.EventId = eventId
	insertData.EventTable = tableName
	insertData.EventCreatedAt = timestamp
	insertData.EventAction = e.Action

	return insertData, false
}
