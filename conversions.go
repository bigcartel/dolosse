package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/city"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/goccy/go-yaml"
	"github.com/shopspring/decimal"
	"github.com/siddontang/go-log/log"
)

func parseString(value string, tableName string, columnName string) interface{} {
	var out interface{}

	if isYamlColumn(tableName, columnName) {
		y := make(map[string]interface{})

		err := yaml.Unmarshal([]byte(value), &y)

		if err != nil {
			y["rawYaml"] = value
			y["errorParsingYaml"] = err
			log.Errorln(value)
			log.Errorln(err)
		}

		for k, v := range y {
			delete(y, k)
			v = anonymizeValue(v, tableName, fmt.Sprintf("%s.%s", columnName, k))
			y[stripLeadingColon(k)] = v
		}

		out, err = json.Marshal(y)
		must(err)
	} else {
		out = anonymizeValue(value, tableName, columnName)
	}

	return out
}

func convertMysqlColumnType(value interface{}, columnType int) interface{} {
	if value == nil {
		return value
	}
	switch columnType {
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.ParseInLocation(mysql.TimeFormat, vs, time.UTC)
			must(err)
			return vt
		} else {
			return value
		}
	case schema.TYPE_DATE:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			vt, err := time.Parse("2006-01-02", vs)
			must(err)
			return vt
		} else {
			return value
		}
	case schema.TYPE_DECIMAL:
		vs := fmt.Sprint(value)
		if len(vs) > 0 {
			val, err := decimal.NewFromString(vs)
			must(err)
			return val
		} else {
			return vs
		}
	default:
		return value
	}
}

func parseValue(value interface{}, columnType int, tableName string, columnName string) interface{} {
	value = convertMysqlColumnType(value, columnType)

	switch v := value.(type) {
	case []byte:
		return parseString(string(v), tableName, columnName)
	case string:
		return parseString(v, tableName, columnName)
	default:
		return value
	}
}

var regexpMatchCache = NewConcurrentMap[ConcurrentMap[bool]]()

func memoizedStringMatch(v, matcher string, callback func() bool) bool {
	var cachedMatch *bool
	matcherCache := regexpMatchCache.Get(matcher)
	if matcherCache != nil {
		cachedMatch = matcherCache.Get(v)

		if cachedMatch != nil {
			return *cachedMatch
		}
	} else {
		newCache := NewConcurrentMap[bool]()
		matcherCache = &newCache
		regexpMatchCache.Set(matcher, &newCache)
	}

	matched := callback()
	matcherCache.Set(v, &matched)

	return matched
}

func memoizedRegexpsMatch(s string, regexps []*regexp.Regexp) bool {
	for _, r := range regexps {
		match := memoizedStringMatch(s, r.String(), func() bool {
			return r.MatchString(s)
		})
		if match {
			return true
		}
	}

	return false
}

func isAnonymizedField(s string) bool {
	return memoizedRegexpsMatch(s, Config.AnonymizeFields)
}

func isYamlColumn(tableName string, columnName string) bool {
	return memoizedRegexpsMatch(fieldString(tableName, columnName), Config.YamlColumns)
}

func fieldString(table string, columnPath string) string {
	b := strings.Builder{}
	b.WriteString(table)
	b.WriteString(".")
	b.WriteString(columnPath)
	return b.String()
}

func hashString(s *[]byte) string {
	return strconv.FormatUint(city.CH64(*s), 10)
}

// sanitize yaml keys that start with colon
func stripLeadingColon(s string) string {
	if s[0] == ':' {
		return s[1:]
	} else {
		return s
	}
}

// currently only supports strings
func anonymizeValue(value interface{}, table string, columnPath string) interface{} {
	anonymize := isAnonymizedField(fieldString(table, columnPath))

	switch v := value.(type) {
	case map[string]interface{}:
		for k, subv := range v {
			delete(v, k)
			subv = anonymizeValue(subv, table, fmt.Sprintf("%s.%s", columnPath, k))

			v[stripLeadingColon(k)] = subv
		}
	case []interface{}:
		for i := range v {
			v[i] = anonymizeValue(v[i], table, fmt.Sprintf("%s.%s", columnPath, fmt.Sprint(i)))
		}
	case string:
		if anonymize {
			vp := []byte(v)
			return hashString(&vp)
		}
	case []byte:
		if anonymize {
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
	// treat json columns as strings because clickhouse-go's json type conversion isn't perfect
	if chColumnType.Kind() == reflect.Map {
		chColumnType = reflect.TypeOf("")
	}

	var v reflect.Value
	var reflectAry reflect.Value

	if ary == nil {
		// TODO find a way to re-use these slices between runs.
		// could this use a sync.Pool? Only issue is that different slices have different types,
		// so I might need to make something custom.
		// Also - set this slice to be set to the size of this batch instead of max batch size
		reflectAry = reflect.MakeSlice(reflect.SliceOf(chColumnType), 0, *Config.BatchSize)
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
