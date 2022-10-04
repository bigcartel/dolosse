package main

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
	"github.com/pkg/errors"
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

	eid := strings.Builder{}
	eid.Grow(len(e.ServerId) + len(gtid) + len(gtidCount) + 2)
	eid.WriteString(e.ServerId)
	eid.WriteRune(':')
	eid.WriteString(gtid)
	eid.WriteRune('#')
	eid.WriteString(gtidCount)

	return eid.String()
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

func startReplication(gtidSet mysql.GTIDSet) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001,
		HeartbeatPeriod: 60 * time.Second,
		Host:            *Config.MysqlAddr,
		User:            *Config.MysqlUser,
		Password:        *Config.MysqlPassword,
		Flavor:          "mysql",
		RawModeEnabled:  true,
	}

	parser := replication.NewBinlogParser()
	parser.SetFlavor("mysql")
	parser.SetUseDecimal(false)
	parser.SetParseTime(false)

	// TODO experiment with raw mode and instantiating parser to only parse events of interest
	// parser := replication.NewBinlogParser()

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(gtidSet)
	must(err)

	return withMysqlConnection(func(conn *client.Conn) error {
		var action string
		var serverUuid string
		var gtidEventCount uint32
		var eventGtid int64
		var rawEventSid []byte
		var txnCommitTime time.Time

		for {
			rawEv, err := streamer.GetEvent(State.ctx)
			if err != nil {
				return err
			}

			var ev *replication.BinlogEvent
			switch rawEv.Header.EventType {
			case replication.TABLE_MAP_EVENT,
				replication.FORMAT_DESCRIPTION_EVENT,
				replication.GTID_EVENT,
				replication.XID_EVENT,
				replication.WRITE_ROWS_EVENTv0,
				replication.UPDATE_ROWS_EVENTv0,
				replication.DELETE_ROWS_EVENTv0,
				replication.WRITE_ROWS_EVENTv1,
				replication.DELETE_ROWS_EVENTv1,
				replication.UPDATE_ROWS_EVENTv1,
				replication.WRITE_ROWS_EVENTv2,
				replication.UPDATE_ROWS_EVENTv2,
				replication.DELETE_ROWS_EVENTv2:

				ev, err = parser.Parse(rawEv.RawData)

				if err != nil {
					return err
				}
			default:
				continue
			}

			// This is a partial copy of
			// https://github.com/go-mysql-org/go-mysql/blob/master/canal/sync.go#L133
			// but simplified so we're not paying the performance overhead for events
			// we don't care about
			// the basic idea is to handle rows event, gtid, and xid events and ignore the rest.
			// maybe if there's a way to detect alter table events it could use that and refresh the given table if it gets one.
			switch e := ev.Event.(type) {
			// TODO add testing and support for mariadb
			// case *replication.MariadbGTIDEvent:
			// 	lastGTIDString = e.GTID.String()
			case *replication.GTIDEvent:
				txnCommitTime = e.OriginalCommitTime()
				eventGtid = e.GNO
				gtidEventCount = 0

				if !bytes.Equal(e.SID, rawEventSid) {
					rawEventSid = e.SID
					sid, err := uuid.FromBytes(rawEventSid)
					if err != nil {
						return errors.Wrap(err, "failed parsing GTID event server UUID")
					}

					serverUuid = sid.String()
				}
			case *replication.RowsEvent:
				if !bytes.Equal(e.Table.Schema, Config.MysqlDbByte) {
					continue
				}

				// TODO make cachedChColumnsForTable support []byte so it doesn't need extra allocations
				// or is there a faster way to check this to make skipping less expensive?
				// is this even expensive?
				_, hasColumns := State.chColumns.ColumnsForTable(string(e.Table.Table))
				if !hasColumns {
					continue
				}

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
					Table:          table,
					Rows:           e.Rows,
					Action:         action,
					Timestamp:      txnCommitTime,
					ServerId:       serverUuid,
					GtidEventCount: gtidEventCount,
					Gtid:           eventGtid,
				}

				OnRow(&rowE)
				gtidEventCount++
			}

			updateReplicationDelay(ev.Header.Timestamp)
		}
	})
}
