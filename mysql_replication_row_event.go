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
)

type MysqlReplicationRowEvent struct {
	Table          *schema.Table
	Rows           [][]interface{}
	Action         string
	Timestamp      time.Time
	ServerId       string
	GtidEventCount uint32
	Gtid           int64
}

func (e *MysqlReplicationRowEvent) EventId() string {
	gtid := strconv.FormatInt(e.Gtid, 10)
	gtidCount := strconv.FormatUint(uint64(e.GtidEventCount), 10)

	// optimized to reduce allocations vs sprintf or string concatenation.
	eid := strings.Builder{}
	eid.Grow(len(e.ServerId) + len(gtid) + len(gtidCount) + 2)
	eid.WriteString(e.ServerId)
	eid.WriteRune(':')
	eid.WriteString(gtid)
	eid.WriteRune('#')
	eid.WriteString(gtidCount)

	return eid.String()
}

func (e *MysqlReplicationRowEvent) IsDumpEvent() bool {
	return e.Action == "dump"
}

func (e *MysqlReplicationRowEvent) GtidRangeString() string {
	if !e.IsDumpEvent() {
		return e.ServerId + ":1-" + strconv.FormatInt(e.Gtid, 10)
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

	insertData.Event[actionColumnName] = e.Action

	id := toInt64(insertData.Event["id"])
	insertData.Id = toInt64(id)
	var eventId string
	if e.Action != "dump" {
		eventId = e.EventId()
	} else {
		eventId = fmt.Sprintf("dump:%d#0", id)
	}

	insertData.Event[eventIdColumnName] = eventId
	insertData.EventId = eventId
	insertData.EventTable = tableName
	insertData.EventCreatedAt = timestamp
	insertData.EventAction = e.Action

	return insertData, false
}
