package main

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/siddontang/go-log/log"
)

// TODO do I even need this? Should I instead refresh clickhouse columns in the dump routine?
func getMysqlTableNames(mysqlCanal *canal.Canal) []string {
	rr, err := mysqlCanal.Execute(fmt.Sprintf("SHOW TABLES FROM %s", *mysqlDb))
	if err != nil {
		log.Fatal(err)
	}

	tables := make([]string, len(rr.Values))
	for i, v := range rr.Values {
		tables[i] = string(v[0].AsString())
	}

	return tables
}
