package main

import (
	"sync"
	"time"

	"golang.org/x/exp/maps"
)

type RowData map[string]interface{}

type RowInsertData struct {
	Id                     int64
	EventTable             string
	EventCreatedAt         time.Time
	EventAction            string
	EventId                string
	ServerId               string
	TransactionEventNumber uint32
	TransactionId          uint64
	RawEvent               *MysqlReplicationRowEvent
	Event                  RowData
}

type DuplicateBinlogEventKey = struct {
	ServerId               string `ch:"changelog_gtid_server_id"`
	TransactionId          uint64 `ch:"changelog_gtid_transaction_id"`
	TransactionEventNumber uint32 `ch:"changelog_gtid_transaction_event_number"`
}

func (d *RowInsertData) Reset() {
	maps.Clear(d.Event)
}

func (d *RowInsertData) DedupeKey() DuplicateBinlogEventKey {
	return DuplicateBinlogEventKey{
		ServerId:               d.ServerId,
		TransactionId:          d.TransactionId,
		TransactionEventNumber: d.TransactionEventNumber,
	}
}

var RowInsertDataPool = sync.Pool{
	New: func() any {
		insertData := new(RowInsertData)
		insertData.Event = make(RowData, 20)
		return insertData
	},
}
