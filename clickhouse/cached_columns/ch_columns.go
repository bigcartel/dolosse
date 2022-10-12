package cached_columns

import (
	"bigcartel/dolosse/concurrent_map"
	"reflect"
)

type ClickhouseQueryColumn struct {
	Name string
	Type reflect.Type
}

type ClickhouseQueryColumns []ClickhouseQueryColumn

// unfortunately we can't get reflect types when querying all tables at once
// so this is a separate type from ClickhouseQueryColumn
type ChColumnInfo struct {
	Name  string `ch:"name"`
	Table string `ch:"table"`
	Type  string `ch:"type"`
}

type ChColumnMap map[string][]ChColumnInfo

type ChColumnSet struct {
	Columns      ClickhouseQueryColumns
	ColumnLookup map[string]bool
}

type ChColumns struct {
	M concurrent_map.ConcurrentMap[ChColumnSet]
}

func NewChColumns() *ChColumns {
	return &ChColumns{
		M: concurrent_map.NewConcurrentMap[ChColumnSet](),
	}
}

func (c *ChColumns) ColumnsForTable(table string) (*ChColumnSet, bool) {
	columns := c.M.Get(table)
	return columns, (columns != nil && len(columns.Columns) > 0)
}

type chColumnQueryable interface {
	ColumnsForMysqlTables(MyColumnQueryable) ChColumnMap
	Columns(tableName string) (ClickhouseQueryColumns, map[string]bool)
}

// this being in here and imported elsewhere feels smelly to me
type MyColumnQueryable interface {
	GetMysqlTableNames() []string
}

// TODO make this a cache method
func (c *ChColumns) Sync(ch chColumnQueryable, my MyColumnQueryable) {
	existingClickhouseTables := ch.ColumnsForMysqlTables(my)

	for table := range existingClickhouseTables {
		cols, lookup := ch.Columns(table)

		c.M.Set(table, &ChColumnSet{
			Columns:      cols,
			ColumnLookup: lookup,
		})
	}
}
