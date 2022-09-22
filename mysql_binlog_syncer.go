package main

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
)

type MysqlReplicationRowEvent struct {
	Table             *schema.Table
	Rows              [][]interface{}
	Action            string
	Timestamp         uint32
	SubsecondSequence uint32
	Gtid              string
}

// TODO replace all these events with enum types that are uint8 underneath
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

var replicationDelay = atomic.Uint32{}

// TODO - if I have the event time in events do I really need to store this globally?
func updateReplicationDelay(eventTime uint32) {
	var newDelay uint32
	now := uint32(time.Now().Unix())

	if now >= eventTime {
		newDelay = now - eventTime
	}
	replicationDelay.Store(newDelay)
}

type PerSecondEventCountMap = map[uint64]uint32

const eventCountMemory = 60

func rotateEventCounts(counts *[]PerSecondEventCountMap, currentTimestamp, newestTimestamp uint32) {
	rotateWindowBy := int(currentTimestamp - newestTimestamp)

	if rotateWindowBy > eventCountMemory {
		rotateWindowBy = eventCountMemory
	}

	recycle, truncated := (*counts)[0:rotateWindowBy], (*counts)[rotateWindowBy:]
	*counts = truncated
	for _, m := range recycle {
		if m != nil {
			maps.Clear(m)
		} else {
			m = make(PerSecondEventCountMap, 100)
		}

		*counts = append(*counts, m)
	}
}

func startReplication(gtidSet mysql.GTIDSet) {
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001,
		HeartbeatPeriod: 60 * time.Second,
		Flavor:          "mysql",
		Host:            *mysqlAddr,
		User:            *mysqlUser,
		Password:        *mysqlPassword,
		UseDecimal:      false,
		ParseTime:       false,
	}

	// parser := replication.NewBinlogParser()
	// DONT PARSE TIME OR DECIMAL here since it's single threaded
	// push that out to the process event workers

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(gtidSet)
	checkErr(err)

	checkErr(withMysqlConnection(func(conn *client.Conn) error {
		var action string
		var lastGTIDString string
		perSecondEventCounts := make([]PerSecondEventCountMap, eventCountMemory)
		var newestTimestampEncountered uint32 = 0

		for {
			ev, err := streamer.GetEvent(context.Background())
			checkErr(err)

			// TODO pass context into here and select on it - if done print last gtid string
			// This is a partial copy of
			// https://github.com/go-mysql-org/go-mysql/blob/master/canal/sync.go#L133
			// but simplified so we're not paying the performance overhead for events
			// we don't care about
			// the basic idea is to handle rows event, gtid, and xid events and ignore the rest.
			// maybe if there's a way to detect alter table events it could use that and refresh the given table if it gets one.
			switch e := ev.Event.(type) {
			// case *replication.MariadbGTIDEvent:
			// 	lastGTIDString = e.GTID.String()
			case *replication.GTIDEvent:
				u, _ := uuid.FromBytes(e.SID)
				lastGTIDString = u.String() + ":1-" + strconv.FormatInt(e.GNO, 10)
			case *replication.QueryEvent:
				// TODO do a naive check for alter table here
				// - if an alter table is detected clear the mysql table cache used to decode events
				// log.Infoln(string(e.Query))
			case *replication.RowsEvent:
				if !bytes.Equal(e.Table.Schema, mysqlDbByte) {
					continue
				}

				// TODO make cachedChColumnsForTable support []byte so it doesn't need extra allocations
				// or is there a faster way to check this to make skipping less expensive?
				// is this even expensive?
				_, hasColumns := cachedChColumnsForTable(string(e.Table.Table))
				if !hasColumns {
					continue
				}

				if ev.Header.Timestamp > newestTimestampEncountered {
					rotateEventCounts(&perSecondEventCounts, ev.Header.Timestamp, newestTimestampEncountered)
					newestTimestampEncountered = ev.Header.Timestamp
				}

				counterLookupOffset := eventCountMemory - (newestTimestampEncountered - ev.Header.Timestamp) - 1

				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					action = InsertAction
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					action = DeleteAction
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					action = UpdateAction
				}

				table := getMysqlTable(string(e.Table.Schema), string(e.Table.Table))

				rowE := MysqlReplicationRowEvent{
					Table:             table,
					Rows:              e.Rows,
					Action:            action,
					Timestamp:         ev.Header.Timestamp,
					SubsecondSequence: perSecondEventCounts[counterLookupOffset][e.TableID],
					Gtid:              lastGTIDString,
				}

				OnRow(&rowE)

				perSecondEventCounts[counterLookupOffset][e.TableID]++
			}

			updateReplicationDelay(ev.Header.Timestamp)
		}
	}))
}
