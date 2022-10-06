package main

import (
	"sync"
	"time"

	"golang.org/x/exp/maps"
)

type RowInsertData struct {
	Id             int64
	EventTable     string
	EventCreatedAt time.Time
	EventAction    string
	EventId        string
	RawEvent       *MysqlReplicationRowEvent
	Event          RowData
}

func (d *RowInsertData) Reset() {
	maps.Clear(d.Event)
}

var RowInsertDataPool = sync.Pool{
	New: func() any {
		insertData := new(RowInsertData)
		insertData.Event = make(RowData, 20)
		return insertData
	},
}
