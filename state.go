package main

import (
	"context"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
	"go.uber.org/atomic"
)

var State GlobalState

type GlobalState struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	processRows          chan *MysqlReplicationRowEvent
	batchWrite           chan *MysqlReplicationRowEvent
	latestProcessingGtid chan string
	initiatedDump        atomic.Bool
	mysqlPoolInitialized atomic.Bool
	mysqlPool            *client.Pool
	chColumns            ChColumns
	mysqlColumns         ConcurrentMap[*schema.Table]
	dumpingTables        ConcurrentMap[struct{}]
	cachedMatchers       CachedMatchers
}

func NewGlobalState() *GlobalState {
	s := GlobalState{}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	s.chColumns = ChColumns{
		m: NewConcurrentMap[ChColumnSet](),
	}
	s.processRows = make(chan *MysqlReplicationRowEvent, *Config.BatchSize*2)
	s.batchWrite = make(chan *MysqlReplicationRowEvent, *Config.BatchSize*2)
	s.latestProcessingGtid = make(chan string)
	s.dumpingTables = NewConcurrentMap[struct{}]()
	s.mysqlColumns = NewConcurrentMap[*schema.Table]()
	s.cachedMatchers = NewCachedMatchers()

	return &s
}

func (s *GlobalState) Init() {
	*s = *NewGlobalState()

	s.InitMysqlPool()
	clickhouseDb := unwrap(establishClickhouseConnection())
	clickhouseDb.Setup()

	// TODO validate all clickhouse table columns are compatible with mysql table columns
	// and that clickhouse tables have event_created_at DateTime, id same_as_mysql, action string
	// use the clickhouse mysql table function with the credentials provided to this command
	// to do it using clickhouse translated types for max compat
	clickhouseDb.CheckSchema()

	s.chColumns.Sync(clickhouseDb)
}

func (s *GlobalState) InitMysqlPool() {
	if !s.mysqlPoolInitialized.Load() {
		s.mysqlPool = client.NewPool(log.Debugf, 10, 20, 5, *Config.MysqlAddr, *Config.MysqlUser, *Config.MysqlPassword, *Config.MysqlDb)
		s.mysqlPoolInitialized.Store(true)
	}
}

type ChColumnSet struct {
	columns      []ClickhouseQueryColumn
	columnLookup LookupMap
}

type ChColumns struct {
	m ConcurrentMap[ChColumnSet]
}

func (c *ChColumns) UpdateTable(table string, columns []ClickhouseQueryColumn, columnLookup LookupMap) {
	c.m.Set(table, &ChColumnSet{
		columns:      columns,
		columnLookup: columnLookup,
	})
}

func (c *ChColumns) ColumnsForTable(table string) (*ChColumnSet, bool) {
	columns := c.m.Get(table)
	return columns, (columns != nil && len(columns.columns) > 0)
}

func (c *ChColumns) Sync(clickhouseDb ClickhouseDb) {
	existingClickhouseTables := clickhouseDb.ColumnsForMysqlTables()

	for table := range existingClickhouseTables {
		cols, lookup := clickhouseDb.Columns(table)

		c.m.Set(table, &ChColumnSet{
			columns:      cols,
			columnLookup: lookup,
		})
	}
}
