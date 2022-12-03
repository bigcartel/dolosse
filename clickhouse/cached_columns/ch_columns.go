package cached_columns

import (
	"bigcartel/dolosse/concurrent_map"
	"reflect"
	"strings"
)

type ClickhouseQueryColumn struct {
	Name             string
	DatabaseTypeName string
	Type             reflect.Type
}

func (c ClickhouseQueryColumn) IsUInt64() bool {
	return c.DatabaseTypeName == "UInt64"
}

func (c ClickhouseQueryColumn) IsInt() bool {
	return strings.HasPrefix(c.DatabaseTypeName, "Int") ||
		strings.HasPrefix(c.DatabaseTypeName, "UInt")
}

func (c ClickhouseQueryColumn) IsFloat() bool {
	return strings.HasPrefix(c.DatabaseTypeName, "Float")
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
	ColumnLookup map[string]ClickhouseQueryColumn
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
	Columns(tableName string) (ClickhouseQueryColumns, map[string]ClickhouseQueryColumn)
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
