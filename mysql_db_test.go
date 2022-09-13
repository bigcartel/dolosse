package main

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shopspring/decimal"
)

type testField struct {
	val string
}

func (f *testField) AsString() []byte {
	return []byte(f.val)
}

func (f *testField) Value() interface{} {
	return f.val
}

func mysqlFormatTime(t time.Time) string {
	return t.Format(mysql.TimeFormat)
}

func TestParseMysqlDumpTimeField(t *testing.T) {
	testTypes := []uint8{mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP}

	ts := time.Now()

	for _, tt := range testTypes {
		var timeValue DumpFieldVal = &testField{val: mysqlFormatTime(ts)}
		r := parseMysqlDumpField(timeValue, tt)
		parsedTs := r.(time.Time)

		if mysqlFormatTime(ts) != mysqlFormatTime(parsedTs) {
			t.Errorf("Expected parsed timestamp %s to equal timestamp %s", mysqlFormatTime(ts), mysqlFormatTime(parsedTs))
		}
	}
}

func TestParseMysqlDumpDecimalField(t *testing.T) {
	testTypes := []uint8{mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL}

	for _, dt := range testTypes {
		ds := "11.43"
		d, _ := decimal.NewFromString(ds)
		var dVal DumpFieldVal = &testField{val: ds}
		r := parseMysqlDumpField(dVal, dt)
		if !r.(decimal.Decimal).Equal(d) {
			t.Errorf("expected parsed decimal %s to equal %s", d, r)
		}
	}
}

func TestParseMysqlDumpStringField(t *testing.T) {
	s := "asdf"
	var v DumpFieldVal = &testField{val: s}
	r := parseMysqlDumpField(v, mysql.MYSQL_TYPE_STRING)
	if r.(string) != s {
		t.Errorf("expected parsed string %s to equal %s", s, r)
	}
}
