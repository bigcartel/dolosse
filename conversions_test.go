package main

import (
	"encoding/json"
	"reflect"
	"regexp"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
)

func makeRowEvent() *MysqlReplicationRowEvent {
	replicationRowEvent := &MysqlReplicationRowEvent{}
	replicationRowEvent.Action = "create"
	table := schema.Table{Name: "test_table"}
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

func TestParseBadYaml(t *testing.T) {
	Config.ParseFlags([]string{})

	yaml := `---
:primary_font: Abril Fatface
:background_color: "#FEFEFE"
:text_color: "#F126F1"
:link_hover: "#056FFA"
:border: "#000000"
:button_text_color: "#FFFFFF"
:button_background_color: "#000000"
:button_hover_text_color: "#FFFFFF"
:button_hover_background_color: "#056FFA"
:badge_text_color: "#000000"
:badge_background_color: "#9AFF00"
:announcement_text_color: "#FFFFFF"
:announcement_background_color: "#000000"
:error_text_color: "#FFFFFF"
:error_background_color: "#EB0000"
:announcement_message_text: 'USE CODE â€œ15AJADAâ€\u009d FOR $$$ OF YOUR FIRST ORDER, YES I
  STILL SELL BRACES  '
:maintenance_message: We're working on our shop right now.<br /><br />Please check
  back soon.
:theme_layout: below_header
:layout_style: fixed-width
:show_layout_gutters: true
:sidebar_position: left
:header_position: left
:uppercase_text: true
:background_image_layout: fixed
:background_image_style: transparent_color_overlay
:show_product_grid_gutters: true
:product_list_layout: rows
:product_grid_image_size: small
:grid_image_style: cover
:product_badge_style: circle
:show_overlay: under_image left-align
:mobile_product_grid_style: small
:secondary_product_images: thumbs
:featured_items: 12
:featured_order: position
:products_per_page: 24
:number_related_products: 3
:show_quickview: true
:show_search: true
:show_inventory_bars: false
:show_sold_out_product_options: true
:money_format: sign
:bandcamp_url:
:facebook_url:
:instagram_url: https://www.instagram.com/bracedbyajada/?hl=en
:pinterest_url:
:tumblr_url:
:twitter_url:
:youtube_url: https://youtube.com/channel/UCVyYq3cVHDkysN2xsa4Z3og
`

	out := struct {
		PrimaryFont string `json:"primary_font"`
	}{}

	err := json.Unmarshal(parseValue(yaml, schema.TYPE_STRING, "order_transactions", "params").([]byte), &out)

	if err != nil {
		t.Fatal(err)
	} else if out.PrimaryFont != "Abril Fatface" {
		t.Fatalf("expected primary font to be Abril Fatface, got %s", out.PrimaryFont)
	}
}

func yamlColumnSetup() {
	Config.YamlColumns = []*regexp.Regexp{regexp.MustCompile("test_table.yaml_column")}
	State = *NewGlobalState()
}

func TestParseValueUint8Array(t *testing.T) {
	outValue := "test string"
	inValue := []uint8(outValue)
	parsedValue := parseValue(inValue, schema.TYPE_STRING, "table", "column")
	if outValue != parsedValue.(string) {
		t.Fatalf("expected '%s' to be converted to string, got '%s'", inValue, parsedValue)
	}
}

func TestParseConvertAndAnonymizeYaml(t *testing.T) {
	yamlColumnSetup()
	Config.AnonymizeFields = []*regexp.Regexp{
		regexp.MustCompile(".*email.*"),
		regexp.MustCompile(".*password.*"),
	}
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

	out := struct {
		Password  string `json:"password"`
		Email     string `json:"email"`
		Firstname string `json:"firstname"`
	}{}

	err = json.Unmarshal(parseValue(string(yamlString), schema.TYPE_STRING, "test_table", "yaml_column").([]byte), &out)
	if err != nil {
		t.Fatal(err)
	}

	anonymizedEmail := out.Email
	anonymizedPassword := out.Password
	parsedFirstName := out.Firstname

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
	yamlColumnSetup()
	password := "test"
	out := parseValue(password, schema.TYPE_STRING, "some_table", "password").(string)
	assert.NotEqual(t, out, password, "expected password string to be anonymized")
}

func TestAnonymizeByteSlice(t *testing.T) {
	yamlColumnSetup()
	password := []byte("test")
	out := parseValue(password, schema.TYPE_STRING, "some_table", "password").(string)

	assert.NotEqual(t, out, string(password), "expected password byte slice to be anonymized")
}

func BenchmarkParseConvertAndAnonymizeYaml(b *testing.B) {
	Config.AnonymizeFields = []*regexp.Regexp{
		regexp.MustCompile(".*email.*"),
		regexp.MustCompile(".*password.*"),
	}
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
		b.Error(err)
	}

	for n := 0; n < b.N; n++ {
		parseValue(string(yamlString), schema.TYPE_STRING, "test_table", "yaml_column")
	}
}

func BenchmarkIsAnonymizedField(b *testing.B) {
	Config.AnonymizeFields = []*regexp.Regexp{
		regexp.MustCompile(".*email.*"),
	}

	for n := 0; n < b.N; n++ {
		isAnonymizedField("email.yes")
	}
}
