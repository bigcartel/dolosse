package main

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/city"
	"github.com/siddontang/go-log/log"
	"gopkg.in/yaml.v3"
)

// could instead be comma separate list of table.column paths provided via CLI.
// Could also infer and attempt to parse if the destination column is JSON type.
// TODO either move to config or infer from string text if a config is set to true?
// Could this also work with json columns/ruby serialized columns in a generic way?
// TODO make this the same as anonymizeFields - just a path string
var yamlColumns = []string{
	"theme_instances.settings",
	"theme_instances.image_sort_order",
	"order_transactions.params",
}

// TODO make this a configurable arg of comma separated strings
var anonymizeFields = []string{
	"address",
	"street",
	"password",
	"salt",
	"email",
	"longitude",
	"latitude",
	"payment_methods.properties",
}

var weirdYamlKeyMatcher = regexp.MustCompile("^:(.*)")

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

func stringInSlice(s string, slice []string) bool {
	for i := range slice {
		if strings.Contains(s, slice[i]) {
			return true
		}
	}

	return false
}

func isAnonymizedField(s string) bool {
	return stringInSlice(s, anonymizeFields)
}

func isYamlColumn(tableName string, columnName string) bool {
	return stringInSlice(fmt.Sprintf("%s.%s", tableName, columnName), yamlColumns)
}

func fieldString(table string, columnPath string) string {
	b := strings.Builder{}
	b.WriteString(table)
	b.WriteString(columnPath)
	return b.String()
}

func hashString(s *[]byte) string {
	return strconv.FormatUint(city.CH64(*s), 10)
}

// currently only supports strings
func anonymizeValue(value interface{}, table string, columnPath string) interface{} {
	if isAnonymizedField(fieldString(table, columnPath)) {
		switch v := value.(type) {
		case string:
			vp := []byte(v)
			return hashString(&vp)
		case []byte:
			return hashString(&v)
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
		// TODO find a way to re-use these slices between runs.
		// could this use a sync.Pool? Only issue is that different slices have different types,
		// so I might need to make something custom.
		// Also - set this slice to be set to the size of this batch instead of max batch size
		reflectAry = reflect.MakeSlice(reflect.SliceOf(chColumnType), 0, batchSize)
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
