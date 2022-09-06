package main

import (
	"testing"

	"gopkg.in/yaml.v3"
)

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
