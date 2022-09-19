package main

import (
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"gopkg.in/yaml.v3"
)

func makeRowEvent() *MysqlReplicationRowEvent {
	replicationRowEvent := &MysqlReplicationRowEvent{}
	replicationRowEvent.Action = "create"
	table := schema.Table{Name: "products"}
	table.Columns = append(table.Columns, schema.TableColumn{Name: "id"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "name"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "description"})
	replicationRowEvent.Table = &table
	rows := make([][]interface{}, 0)
	rows = append(rows, []interface{}{12, "asdfe", "cool"})
	rows = append(rows, []interface{}{12, "asdf", "pretty cool"})
	replicationRowEvent.Rows = rows
	return replicationRowEvent
}

func makeColumnSet() *ChColumnSet {
	return &ChColumnSet{
		columns: []ClickhouseQueryColumn{
			{
				Name: "id",
				Type: reflect.TypeOf(12),
			},
			{
				Name: "name",
				Type: reflect.TypeOf(""),
			},
			{
				Name: "description",
				Type: reflect.TypeOf(""),
			},
		},
		columnLookup: map[string]bool{
			"id":          true,
			"name":        true,
			"description": true,
		},
	}
}

func TestEventToClickhouseRowData(t *testing.T) {
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	insertData, isDuplicate := eventToClickhouseRowData(rowEvent, columns)

	if isDuplicate {
		t.Error("Expected row not to be flagged as duplicate")
	}

	if insertData.Id != 12 {
		t.Errorf("Expected id to be 12, got %d", insertData.Id)
	}

	if insertData.Event["id"] != 12 {
		t.Errorf("Expected Event['id'] to be 12, got %d", insertData.Id)
	}

	if insertData.Event["name"] != "asdf" {
		t.Errorf("Expected Event['name'] to be asdf, got %s", insertData.Event["name"])
	}

	if insertData.Event["description"] != "pretty cool" {
		t.Errorf("Expected Event['description'] to be 'pretty cool', got %s", insertData.Event["description"])
	}
}

func BenchmarkEventToClickhouseRowData(b *testing.B) {
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	for n := 0; n < b.N; n++ {
		eventToClickhouseRowData(rowEvent, columns)
	}
}

func TestParseValueUint8Array(t *testing.T) {
	outValue := "test string"
	inValue := []uint8(outValue)
	parsedValue := parseValue(inValue, "table", "column")
	if outValue != parsedValue.(string) {
		t.Fatalf("expected '%s' to be converted to string, got '%s'", inValue, parsedValue)
	}
}

func TestParseConvertAndAnonymizeYaml(t *testing.T) {
	password := "test"
	email := "max@test.com"
	firstName := "max"

	input := struct {
		Password  string
		Email     string
		Firstname string `yaml:":firstname"`
	}{password, email, firstName}

	yamlString, err := yaml.Marshal(input)
	if err != nil {
		t.Error(err)
	}

	out := parseValue(string(yamlString), "order_transactions", "params").(map[string]interface{})

	anonymizedEmail := out["email"].(string)
	anonymizedPassword := out["password"].(string)
	parsedFirstName := out["firstname"].(string)

	if anonymizedPassword == password {
		t.Fatalf("Expected password '%s' to be anonymized, got %s", password, anonymizedPassword)
	}

	if anonymizedEmail == email {
		t.Fatalf("Expected email '%s' to be anonymized, got %s", email, anonymizedEmail)
	}

	if parsedFirstName != firstName {
		t.Fatalf("Expected email '%s' not to be anonymized, got %s", firstName, parsedFirstName)
	}
}

func TestAnonymizeStringValue(t *testing.T) {
	password := "test"
	out := parseValue(password, "some_table", "password").(string)
	if out == password {
		t.Fatalf("Expected password '%s' to be anonymized, got %s", password, out)
	}
}

func TestAnonymizeByteSlice(t *testing.T) {
	password := []byte("test")
	out := parseValue(password, "some_table", "password").(string)

	if out == string(password) {
		t.Fatalf("Expected byte slice of password '%s' to be anonymized, got %s", password, out)
	}
}
