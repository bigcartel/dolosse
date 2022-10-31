package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	// TODO dependency inject these

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
	Timestamp              time.Time
	Table                  *schema.Table
	// maps to MYSQL_TYPE_* types defined in
	// go-mysql/mysql/const.go
	EventColumnTypes []byte
	// This will only be populated if using at least mysql8 and the setting binlog_row_metadata=FULL
	// https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
	// It will be populated with names of the columns in correct order at the time event was created.
	EventColumnNames []string
	InsertData       RowInsertData
	Pks              []Pk
	Rows             [][]interface{}
}

func (e MysqlReplicationRowEvent) IsDumpEvent() bool {
	return e.Action == DumpAction
}

type DuplicateBinlogEventKey = struct {
	TransactionEventNumber uint32 `ch:"changelog_gtid_transaction_event_number"`
	TransactionId          uint64 `ch:"changelog_gtid_transaction_id"`
	ServerId               string `ch:"changelog_gtid_server_id"`
}

func (e MysqlReplicationRowEvent) DedupeKey() DuplicateBinlogEventKey {
	return DuplicateBinlogEventKey{
		ServerId:               e.ServerId,
		TransactionId:          e.TransactionId,
		TransactionEventNumber: e.TransactionEventNumber,
	}
}

func (e MysqlReplicationRowEvent) GtidRangeString() string {
	if !e.IsDumpEvent() {
		return e.ServerId + ":1-" + strconv.FormatUint(e.TransactionId, 10)
	} else {
		return ""
	}
}

func (e MysqlReplicationRowEvent) PkString() string {
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

func (e MysqlReplicationRowEvent) buildPk(insertData RowInsertData) (PKValues Pks) {
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

// Returns either the column names as they were at the time of the event
// if binlog_row_metadata=FULL, or the column names as they are at present
func (e MysqlReplicationRowEvent) ColumnNames() []string {
	columnNames := e.EventColumnNames

	if len(columnNames) == 0 {
		columnNames = make([]string, len(e.Table.Columns))
		for i := range e.Table.Columns {
			columnNames[i] = e.Table.Columns[i].Name
		}
	}

	return columnNames
}
