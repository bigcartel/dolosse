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
