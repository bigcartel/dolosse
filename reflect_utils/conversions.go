package reflect_utils

import (
	"fmt"
	"reflect"
	"time"
)

func PointerToValue(value reflect.Value) reflect.Value {
	pt := reflect.PtrTo(value.Type())
	pointerValue := reflect.New(pt.Elem())
	pointerValue.Elem().Set(value)
	return pointerValue
}

func ReflectAppend(chColumnType reflect.Type, ary any, val any, newSliceLen int) (any, error) {
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
		reflectAry = reflect.MakeSlice(reflect.SliceOf(chColumnType), 0, newSliceLen)
	} else {
		reflectAry = reflect.ValueOf(ary)
	}

	v, err := ConvertValue(val, chColumnType)
	if err != nil {
		return reflectAry.Interface(), err
	}

	return reflect.Append(reflectAry, v).Interface(), nil
}

func ConvertValue(val interface{}, convertType reflect.Type) (reflect.Value, error) {
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

			v = PointerToValue(v)
		} else if v.CanConvert(convertType) {
			v = v.Convert(convertType)
		} else if convertType.Kind() != v.Kind() {
			err = fmt.Errorf("failed converting %s to %s", val, convertType.String())
		}
	}

	return v, err
}
