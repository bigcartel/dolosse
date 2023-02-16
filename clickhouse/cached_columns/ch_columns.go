package cached_columns

import (
	"bigcartel/dolosse/concurrent_map"
	"reflect"
	"strings"
)

type ChInsertColumn struct {
	Name             string
	DatabaseTypeName string
	Type             reflect.Type
}

func (c ChInsertColumn) IsUInt64() bool {
	return c.DatabaseTypeName == "UInt64"
}

func (c ChInsertColumn) IsInt() bool {
	return strings.HasPrefix(c.DatabaseTypeName, "Int") ||
		strings.HasPrefix(c.DatabaseTypeName, "UInt")
}

func (c ChInsertColumn) IsFloat() bool {
	return strings.HasPrefix(c.DatabaseTypeName, "Float")
}

type ChInsertColumns []ChInsertColumn
type ChInsertColumnMap = map[string]ChInsertColumn

// unfortunately we can't get reflect types when querying all tables at once
// so this is a separate type from ClickhouseQueryColumn
type ChTableColumn struct {
	Name  string `ch:"name"`
	Table string `ch:"table"`
	Type  string `ch:"type"`
}

type ChTableColumnMap map[string][]ChTableColumn

type ChTableColumnSet struct {
	Columns      ChInsertColumns
	ColumnLookup ChInsertColumnMap
}

type ChDatabaseColumns struct {
	M concurrent_map.ConcurrentMap[ChTableColumnSet]
}

func NewChDatabaseColumns() *ChDatabaseColumns {
	return &ChDatabaseColumns{
		M: concurrent_map.NewConcurrentMap[ChTableColumnSet](),
	}
}

func (c *ChDatabaseColumns) ColumnsForTable(table string) (*ChTableColumnSet, bool) {
	columns := c.M.Get(table)
	return columns, (columns != nil && len(columns.Columns) > 0)
}

type ChColumnQueryable interface {
	ColumnsForMysqlTables(MyColumnQueryable) ChTableColumnMap
	ColumnsForInsert(tableName string) (ChInsertColumns, ChInsertColumnMap)
}

// this being in here and imported elsewhere feels smelly to me
type MyColumnQueryable interface {
	GetMysqlTableNames() []string
}

// TODO make this a cache method
func (c *ChDatabaseColumns) Sync(ch ChColumnQueryable, my MyColumnQueryable) {
	existingClickhouseTables := ch.ColumnsForMysqlTables(my)

	for table := range existingClickhouseTables {
		cols, lookup := ch.ColumnsForInsert(table)

		c.M.Set(table, &ChTableColumnSet{
			Columns:      cols,
			ColumnLookup: lookup,
		})
	}
}
