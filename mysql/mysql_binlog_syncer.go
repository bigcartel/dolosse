package mysql

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	"bigcartel/dolosse/err_utils"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var ReplicationDelay = atomic.Uint32{}

// TODO - if I have the event time in events do I really need to store this globally?
func updateReplicationDelay(eventTime uint32) {
	var newDelay uint32
	now := uint32(time.Now().Unix())

	if now >= eventTime {
		newDelay = now - eventTime
	}
	ReplicationDelay.Store(newDelay)
}

type PerSecondEventCountMap = map[uint64]uint32

func (my *Mysql) StartReplication(gtidSet mysql.GTIDSet, OnRow RowHandler) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:         uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001,
		HeartbeatPeriod:  60 * time.Second,
		Host:             my.cfg.Address,
		User:             my.cfg.User,
		Password:         my.cfg.Password,
		Flavor:           "mysql",
		DisableRetrySync: true,
		RawModeEnabled:   true,
	}

	parser := replication.NewBinlogParser()
	parser.SetFlavor("mysql")
	parser.SetUseDecimal(false)
	parser.SetParseTime(false)

	// TODO experiment with raw mode and instantiating parser to only parse events of interest
	// parser := replication.NewBinlogParser()

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(gtidSet)
	err_utils.Must(err)

	return withMysqlConnection(my, func(conn *client.Conn) error {
		var action string
		var serverUuid string
		var EventTransactionEventNumber uint32
		var eventTransactionId uint64
		var rawEventSid []byte
		var txnCommitTime time.Time

		for {
			rawEv, err := streamer.GetEvent(my.Ctx)
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
				eventTransactionId = uint64(e.GNO)
				EventTransactionEventNumber = 0

				if !bytes.Equal(e.SID, rawEventSid) {
					rawEventSid = e.SID
					sid, err := uuid.FromBytes(rawEventSid)
					if err != nil {
						return errors.Wrap(err, "failed parsing GTID event server UUID")
					}

					serverUuid = sid.String()
				}
			case *replication.RowsEvent:
				if !bytes.Equal(e.Table.Schema, my.dbNameByte) {
					continue
				}

				// TODO make cachedChColumnsForTable support []byte so it doesn't need extra allocations
				// or is there a faster way to check this to make skipping less expensive?
				// is this even expensive?
				_, hasColumns := my.ChColumns.ColumnsForTable(string(e.Table.Table))
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

				table := my.GetMysqlTable(string(e.Table.Schema), string(e.Table.Table))

				rowE := MysqlReplicationRowEvent{
					Table:                  table,
					Rows:                   e.Rows,
					Action:                 action,
					Timestamp:              txnCommitTime,
					ServerId:               serverUuid,
					TransactionEventNumber: EventTransactionEventNumber,
					TransactionId:          eventTransactionId,
				}

				OnRow(&rowE)
				EventTransactionEventNumber++
			}

			updateReplicationDelay(ev.Header.Timestamp)
		}
	})
}
