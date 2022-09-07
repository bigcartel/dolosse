package main

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-faster/city"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

const mysqlDateFormat = "2006-01-02"

// TODO either move to config or infer from string text if a config is set to true?
// could this also pick what fields I want to extract?
// Could this also work with json columns/ruby serialized columns in a generic way?
var yamlColumns = map[string]map[string]bool{
	"theme_instances": {
		"settings":          true,
		"images_sort_order": true,
	},
	"order_transactions": {
		"params": true,
	},
}

// TODO flesh out this regexp/make configurable
var anonymizeFields = regexp.MustCompile(".*(address|street|password|salt|email|longitude|latitude).*|payment_methods.properties|products.description")

func convertMysqlValue(col *schema.TableColumn, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch col.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return value
	case schema.TYPE_FLOAT:
		return value
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				// log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		default:
			return cast.ToString(value)
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		default:
			cast.ToString(value)
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return time.Time{}
			}
			return vt
		default:
			return cast.ToTime(v)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return time.Time{}
			}
			return vt
		}
	case schema.TYPE_DECIMAL:
		val, _ := decimal.NewFromString(cast.ToString(value))
		return val
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int8(1)
			}

			return int8(0)
		default:
			return cast.ToInt8(value)
		}
	}

	return cast.ToString(value)
}

var weirdYamlKeyMatcher = regexp.MustCompile("^:(.*)")

func isYamlColumn(tableName string, columnName string) bool {
	if v, ok := yamlColumns[tableName]; ok {
		return v[columnName]
	} else {
		return false
	}
}

func parseString(value string, tableName string, columnName string) interface{} {
	var out interface{}

	if isYamlColumn(tableName, columnName) {
		y := make(map[string]interface{})
		err := yaml.Unmarshal([]byte(value), y)

		if err != nil {
			y["rawYaml"] = value
			y["errorParsingYaml"] = err
			log.Errorln(value)
			log.Errorln(err)
		}

		for k, v := range y {
			delete(y, k)
			v = anonymizeValue(v, tableName, fmt.Sprintf("%s.%s", columnName, k))
			y[weirdYamlKeyMatcher.ReplaceAllString(k, "$1")] = v
		}

		out = y
	} else {
		out = anonymizeValue(value, tableName, columnName)
	}

	return out
}

func parseValue(value interface{}, tableName string, columnName string) interface{} {
	switch v := value.(type) {
	case []byte:
		return parseString(string(v), tableName, columnName)
	case string:
		return parseString(v, tableName, columnName)
	default:
		return value
	}
}

// currently only supports strings
func anonymizeValue(value interface{}, table string, columnPath string) interface{} {
	fieldString := fmt.Sprintf("%s.%s", table, columnPath)

	if anonymizeFields.Match([]byte(fieldString)) {
		switch v := value.(type) {
		case string:
			return fmt.Sprint(city.CH64([]byte(v)))
		}
	}

	return value
}

func pointerToValue(value reflect.Value) reflect.Value {
	pt := reflect.PtrTo(value.Type())
	pointerValue := reflect.New(pt.Elem())
	pointerValue.Elem().Set(value)
	return pointerValue
}

func reflectAppend(chColumnType reflect.Type, ary any, val any) (any, error) {
	var v reflect.Value
	var reflectAry reflect.Value

	if ary == nil {
		reflectAry = reflect.MakeSlice(reflect.SliceOf(chColumnType), 0, 0)
	} else {
		reflectAry = reflect.ValueOf(ary)
	}

	v, err := convertValue(val, chColumnType)
	if err != nil {
		return reflectAry.Interface(), err
	}

	return reflect.Append(reflectAry, v).Interface(), nil
}

func convertValue(val interface{}, convertType reflect.Type) (reflect.Value, error) {
	var v reflect.Value
	var err error

	if val == nil {
		// special case time because go's zero value for time is year zero
		if (convertType == reflect.TypeOf(time.Time{})) {
			v = reflect.ValueOf(time.Unix(0, 0))
		} else {
			v = reflect.New(convertType).Elem()
		}
	} else {
		v = reflect.ValueOf(val)

		if convertType.Kind() == reflect.Pointer && (val == nil || v.CanConvert(convertType.Elem())) {
			if v.CanConvert(convertType.Elem()) {
				v = v.Convert(convertType.Elem())
			}

			v = pointerToValue(v)
		} else if v.CanConvert(convertType) {
			v = v.Convert(convertType)
		} else if convertType.Kind() != v.Kind() {
			err = fmt.Errorf("failed converting %s to %s", val, convertType.String())
		}
	}

	return v, err
}
