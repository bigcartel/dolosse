package main

import (
	"reflect"
	"testing"
	"time"
)

func TestReflectAppendTypeConversion(t *testing.T) {
	var val int64 = 309
	a, err := reflectAppend(reflect.TypeOf(int32(0)), nil, val)
	if err != nil {
		t.Fatal(err)
	}
	storedVal := a.([]int32)[0]
	if storedVal != int32(val) {
		t.Fatalf("expected %d to be stored but got %d", val, storedVal)
	}
}

func TestReflectAppendNewArrayConcrete(t *testing.T) {
	var val int32 = 309
	a, _ := reflectAppend(reflect.TypeOf(val), nil, val)
	storedVal := a.([]int32)[0]
	if storedVal != val {
		t.Fatalf("expected %d to be stored but got %d", val, storedVal)
	}
}

func TestReflectAppendNewArrayPointer(t *testing.T) {
	val := "test string"
	a, _ := reflectAppend(reflect.TypeOf(&val), nil, val)
	storedVal := *a.([]*string)[0]
	if storedVal != val {
		t.Fatalf("expected %s to be stored but got %s", val, storedVal)
	}
}

func TestReflectAppendExistingArrayConcrete(t *testing.T) {
	var val int32 = 309
	ary := make([]int32, 0)
	a, _ := reflectAppend(reflect.TypeOf(val), ary, val)
	storedVal := a.([]int32)[0]
	if storedVal != val {
		t.Fatalf("expected %d to be stored but got %d", val, storedVal)
	}
}

func TestReflectAppendExistingArrayPointer(t *testing.T) {
	val := "test string"
	ary := make([]*string, 0)
	a, _ := reflectAppend(reflect.TypeOf(&val), ary, val)
	storedVal := *a.([]*string)[0]
	if storedVal != val {
		t.Fatalf("expected %s to be stored but got %s", val, storedVal)
	}
}

func TestReflectAppendNilPointer(t *testing.T) {
	val := "test string"
	a, _ := reflectAppend(reflect.TypeOf(&val), nil, nil)
	storedVal := a.([]*string)[0]
	if storedVal != nil {
		t.Fatalf("expected nil to be stored but got %s", *storedVal)
	}
}

func TestReflectAppendZeroTime(t *testing.T) {
	var val time.Time
	a, _ := reflectAppend(reflect.TypeOf(val), nil, nil)
	storedVal := a.([]time.Time)[0]
	if storedVal != time.Unix(0, 0) {
		t.Fatalf("expected beginning of unix time to be stored but got %s", storedVal)
	}
}
