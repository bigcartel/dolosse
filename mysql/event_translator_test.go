package mysql

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"bigcartel/dolosse/clickhouse/cached_columns"
	"bigcartel/dolosse/consts"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
)

func makeRowEvent() *MysqlReplicationRowEvent {
	replicationRowEvent := &MysqlReplicationRowEvent{}
	replicationRowEvent.Action = "create"
	replicationRowEvent.EventColumnNames = []string{"id", "name", "description", "updated_at"}
	replicationRowEvent.EventColumnTypes = []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_VARCHAR}
	table := schema.Table{Name: "test_table"}
	table.Columns = append(table.Columns, schema.TableColumn{Name: "id"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "name"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "description"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "updated_at"})
	table.PKColumns = []int{0}
	replicationRowEvent.Table = &table
	rows := [][]interface{}{
		{12, "asdfe", "cool", "12-01-01"},
		{12, "asdf", "pretty cool", "12-01-01"},
	}
	replicationRowEvent.Rows = rows
	return replicationRowEvent
}

func makeColumnSet() *cached_columns.ChTableColumnSet {
	idColumn := cached_columns.ChInsertColumn{
		Name: "id",
		Type: reflect.TypeOf(12),
	}
	nameColumn := cached_columns.ChInsertColumn{
		Name: "name",
		Type: reflect.TypeOf(""),
	}
	descriptionColumn := cached_columns.ChInsertColumn{
		Name: "description",
		Type: reflect.TypeOf(""),
	}
	updatedAtColumn := cached_columns.ChInsertColumn{
		Name: "updated_at",
		Type: reflect.TypeOf(""),
	}
	eventUpdatedColumnsColumn := cached_columns.ChInsertColumn{
		Name: consts.EventUpdatedColumnsColumnName,
		Type: reflect.TypeOf([]string{}),
	}

	return &cached_columns.ChTableColumnSet{
		Columns: []cached_columns.ChInsertColumn{
			idColumn,
			nameColumn,
			descriptionColumn,
			updatedAtColumn,
			eventUpdatedColumnsColumn,
		},
		ColumnLookup: map[string]cached_columns.ChInsertColumn{
			"id":                                 idColumn,
			"name":                               nameColumn,
			"description":                        descriptionColumn,
			"updated_at":                         updatedAtColumn,
			consts.EventUpdatedColumnsColumnName: eventUpdatedColumnsColumn,
		},
	}
}

func TestParseBadYaml(t *testing.T) {
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

	cfg := EventTranslatorConfig{
		YamlColumns: regexpSlice("order_transactions"),
	}

	tr := NewEventTranslator(cfg)

	chCol := cached_columns.ChInsertColumn{
		Name: "params",
		Type: reflect.TypeOf(""),
	}
	err := json.Unmarshal(tr.ParseValue(yaml, mysql.MYSQL_TYPE_VARCHAR, "order_transactions", "params", chCol).([]byte), &out)

	if err != nil {
		t.Fatal(err)
	} else if out.PrimaryFont != "Abril Fatface" {
		t.Fatalf("expected primary font to be Abril Fatface, got %s", out.PrimaryFont)
	}
}

func translator() EventTranslator {
	cfg := EventTranslatorConfig{
		AnonymizeFields:     regexpSlice(".*(password|email|address).*"),
		SkipAnonymizeFields: regexpSlice(".*(password)_type$"),
		YamlColumns:         regexpSlice("test_table.yaml_column"),
	}

	return NewEventTranslator(cfg)
}

func TestParseValueUint8Array(t *testing.T) {
	tr := translator()
	outValue := "test string"
	inValue := []uint8(outValue)
	chCol := cached_columns.ChInsertColumn{
		Name: "column",
		Type: reflect.TypeOf(""),
	}
	parsedValue := tr.ParseValue(inValue, mysql.MYSQL_TYPE_VARCHAR, "table", "column", chCol)
	if outValue != parsedValue.(string) {
		t.Fatalf("expected '%s' to be converted to string, got '%s'", inValue, parsedValue)
	}
}

func TestTruncateDateTimeOverflow(t *testing.T) {
	tr := translator()

	outValue := time.Date(2261, 1, 2, 15, 4, 5, 0, time.UTC)
	inValue := time.Date(2400, 1, 2, 15, 4, 5, 0, time.UTC).Format("2006-01-02 15:04:05")

	chCol := cached_columns.ChInsertColumn{
		Name:                 "column",
		DatabaseTypeName:     "DateTime64(3)",
		DatabaseTypeBaseName: "DateTime64",
		Type:                 reflect.TypeOf(""),
	}

	parsedValue := tr.ParseValue(inValue, mysql.MYSQL_TYPE_DATETIME, "table", "column", chCol)

	if outValue != parsedValue.(time.Time) {
		t.Fatalf("expected '%s' to be converted to time.Time and truncated to %s, got '%s'", inValue, outValue, parsedValue)
	}
}

func TestParseDateTime(t *testing.T) {
	tr := translator()

	outValue := time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC)
	inValue := outValue.Format("2006-01-02 15:04:05")

	chCol := cached_columns.ChInsertColumn{
		Name:             "column",
		DatabaseTypeName: "DateTime64",
		Type:             reflect.TypeOf(""),
	}

	parsedValue := tr.ParseValue(inValue, mysql.MYSQL_TYPE_DATETIME, "table", "column", chCol)

	if outValue != parsedValue.(time.Time) {
		t.Fatalf("expected '%s' to be converted to time.Time and truncated to %s, got '%s'", inValue, outValue, parsedValue)
	}
}

func regexpSlice(vs ...string) []*regexp.Regexp {
	sl := make([]*regexp.Regexp, len(vs))

	for i := range vs {
		sl[i] = regexp.MustCompile(vs[i])
	}

	return sl
}

func TestParseConvertAndAnonymizeYaml(t *testing.T) {
	tr := translator()

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
		Password  uint64 `json:"password"`
		Email     uint64 `json:"email"`
		Firstname string `json:"firstname"`
	}{}

	chCol := cached_columns.ChInsertColumn{
		Name: "yaml_column",
		Type: reflect.TypeOf(""),
	}
	err = json.Unmarshal(tr.ParseValue(string(yamlString), mysql.MYSQL_TYPE_VARCHAR, "test_table", "yaml_column", chCol).([]byte), &out)
	if err != nil {
		t.Fatal(err)
	}

	anonymizedEmail := out.Email
	anonymizedPassword := out.Password
	parsedFirstName := out.Firstname

	assert.NotEqual(t, anonymizedPassword, password)
	assert.NotEqual(t, anonymizedEmail, email)
	assert.Equal(t, parsedFirstName, firstName)
}

func TestParseRubyHashYaml(t *testing.T) {
	tr := translator()

	yamlString := []byte(`--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
:firstname: "Mr"
:last: "Person"
`)

	out := struct {
		Firstname string `json:"firstname"`
		Last      string `json:"last"`
	}{}

	chCol := cached_columns.ChInsertColumn{
		Name: "yaml_column",
		Type: reflect.TypeOf(""),
	}

	err := json.Unmarshal(tr.ParseValue(string(yamlString), mysql.MYSQL_TYPE_VARCHAR, "test_table", "yaml_column", chCol).([]byte), &out)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "Mr", out.Firstname)
	assert.Equal(t, "Person", out.Last)
}

func TestAnonymizeStringValue(t *testing.T) {
	tr := translator()
	password := "test"
	chCol := cached_columns.ChInsertColumn{
		Name: "password",
		Type: reflect.TypeOf(""),
	}
	out := tr.ParseValue(password, mysql.MYSQL_TYPE_VARCHAR, "some_table", "password", chCol).(string)
	assert.NotEqual(t, out, password, "expected password string to be anonymized")
}

func TestAnonymizeStringToUint64(t *testing.T) {
	tr := translator()
	password := "test"
	chCol := cached_columns.ChInsertColumn{
		Name:             "password",
		DatabaseTypeName: "UInt64",
		Type:             reflect.TypeOf(uint64(0)),
	}
	out := tr.ParseValue(password, mysql.MYSQL_TYPE_VARCHAR, "some_table", "pswd", chCol).(uint64)
	assert.NotEqual(t, out, password, "expected password string to be anonymized")
}

func TestConvertDecimalToInt(t *testing.T) {
	tr := translator()
	v := "10.21"
	chCol := cached_columns.ChInsertColumn{
		Name:             "num",
		DatabaseTypeName: "Int64",
		Type:             reflect.TypeOf(uint64(0)),
	}
	out := tr.ParseValue(v, mysql.MYSQL_TYPE_DECIMAL, "some_table", "num", chCol).(int64)
	assert.Equal(t, out, int64(1021), "Expected decimal to be converted to integer")
}

func TestConvertDecimalToFloat(t *testing.T) {
	tr := translator()
	v := "10.21"
	chCol := cached_columns.ChInsertColumn{
		Name:             "num",
		DatabaseTypeName: "Float64",
		Type:             reflect.TypeOf(float64(0)),
	}
	out := tr.ParseValue(v, mysql.MYSQL_TYPE_DECIMAL, "some_table", "num", chCol).(float64)
	assert.Equal(t, out, 10.21, "Expected decimal to be converted to integer")
}

func TestSkipAnonymizeStringValue(t *testing.T) {
	tr := translator()
	password := "test"
	chCol := cached_columns.ChInsertColumn{
		Name: "password",
		Type: reflect.TypeOf(""),
	}
	out := tr.ParseValue(password, mysql.MYSQL_TYPE_VARCHAR, "some_table", "password_type", chCol).(string)
	assert.Equal(t, out, password, "expected password string not to be anonymized")
}

func TestAnonymizeByteSlice(t *testing.T) {
	tr := translator()
	password := []byte("test")
	chCol := cached_columns.ChInsertColumn{
		Name: "password",
		Type: reflect.TypeOf(""),
	}
	out := tr.ParseValue(password, mysql.MYSQL_TYPE_VARCHAR, "some_table", "password", chCol).(string)

	assert.NotEqual(t, out, string(password), "expected password byte slice to be anonymized")
}

func BenchmarkParseConvertAndAnonymizeYaml(b *testing.B) {
	tr := translator()
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

	chCol := cached_columns.ChInsertColumn{
		Name: "yaml_column",
		Type: reflect.TypeOf(""),
	}
	for n := 0; n < b.N; n++ {
		tr.ParseValue(string(yamlString), mysql.MYSQL_TYPE_VARCHAR, "test_table", "yaml_column", chCol)
	}
}

func BenchmarkIsAnonymizedField(b *testing.B) {
	tr := translator()

	for n := 0; n < b.N; n++ {
		tr.isAnonymizedField("email.yes")
	}
}

func TestSkipIgnoredDeduplicationColumns(t *testing.T) {
	tr := translator()
	tr.Config.IgnoredColumnsForDeduplication = []*regexp.Regexp{regexp.MustCompile("updated_at")}
	columns := makeColumnSet()
	rowEvent := makeRowEvent()
	rowEvent.Action = "update"
	rowEvent.Rows = [][]interface{}{
		{12, "asdf", "asdf", "12-01-01"},
		{12, "asdf", "asdf", "12-01-02"},
	}

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(rowEvent, columns)

	assert.True(t, isDuplicate)
	assert.False(t, isColumnMismatch)
}

func TestPopulateInsertData(t *testing.T) {
	tr := translator()
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(rowEvent, columns)

	assert.False(t, isDuplicate)
	assert.False(t, isColumnMismatch)
	assert.Equal(t, "12", rowEvent.PkString())
	assert.Equal(t, 12, rowEvent.InsertData["id"])
	assert.Equal(t, "asdf", rowEvent.InsertData["name"])
	assert.Equal(t, "pretty cool", rowEvent.InsertData["description"])
	assert.Equal(t, []string{"name", "description"}, rowEvent.InsertData[consts.EventUpdatedColumnsColumnName])
}

func TestPopulateInsertDataWithNoColumnNamesForOldEvents(t *testing.T) {
	tr := translator()
	ev := makeRowEvent()
	ev.EventColumnNames = []string{}
	columns := makeColumnSet()

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(ev, columns)
	assert.False(t, isDuplicate)
	assert.False(t, isColumnMismatch)
	assert.Equal(t, "12", ev.PkString())
	assert.Equal(t, 12, ev.InsertData["id"])
	assert.Equal(t, "asdf", ev.InsertData["name"])
	assert.Equal(t, "pretty cool", ev.InsertData["description"])
}

func TestSkipIfAssumeOnlyAppendColumnsIsFalse(t *testing.T) {
	tr := translator()
	ev := makeRowEvent()
	ev.Table.Columns = append(ev.Table.Columns,
		schema.TableColumn{Name: "description2"})
	ev.EventColumnNames = []string{}
	columns := makeColumnSet()

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(ev, columns)
	assert.False(t, isDuplicate)
	assert.True(t, isColumnMismatch)
	assert.Equal(t, nil, ev.InsertData["id"])
}

func TestPopulateInsertDataWithNewColumnsAndNoColumnNamesForOldEvents(t *testing.T) {
	tr := translator()
	tr.Config.AssumeOnlyAppendedColumns = true
	ev := makeRowEvent()
	ev.Table.Columns = append(ev.Table.Columns,
		schema.TableColumn{Name: "description2"})
	ev.EventColumnNames = []string{}
	columns := makeColumnSet()

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(ev, columns)
	assert.False(t, isDuplicate)
	assert.False(t, isColumnMismatch)
	assert.Equal(t, "12", ev.PkString())
	assert.Equal(t, 12, ev.InsertData["id"])
	assert.Equal(t, "asdf", ev.InsertData["name"])
	assert.Equal(t, "pretty cool", ev.InsertData["description"])
}

func TestEventWithYaml(t *testing.T) {
	tr := translator()

	yaml := `
---
id: pi_3LhGn1BxWYA6NAEc1iTUZmn1
object: payment_intent
amount_details:
  tip: {}
amount_received: 3800
capture_method: automatic
charges:
  object: list
  data:
  - id: ch_3LhGn1BxWYA6NAEc1m0S5sWZ
    object: charge
    amount: 3800
    amount_captured: 3800
    amount_refunded: 0
    application: ca_216f6MqztUrV3ApE7UcCKi0PnUNukl8U
    application_fee:
    application_fee_amount:
    balance_transaction:
      id: txn_3LhGn1BxWYA6NAEc1k7r56vG
      object: balance_transaction
      amount: 3800
      available_on: 1663200000
      created: 1663005083
      currency: gbp
      description: 'Payment for Big Cartel order #QFYO-589401 from laurenamywhitehouse@hotmail.co.uk'
      exchange_rate:
      fee: 73
      fee_details:
      - amount: 73
        application:
        currency: gbp
        description: Stripe processing fees
        type: stripe_fee
      net: 3727
      reporting_category: charge
      source: ch_3LhGn1BxWYA6NAEc1m0S5sWZ
      status: pending
      type: charge
    billing_details:
      address:
        city: Newport
        country: GB
        line1: 2 Rose Cottages
        line2: Loverstone Lane
        postal_code: PO30 3EL
        state: 'Hampshire '
      email:
      name: Lauren Whitehouse
      phone:
    calculated_statement_descriptor: SASHANICOLETATTOO
    captured: true
    created: 1663005083
    currency: gbp
    customer:
    description: 'Payment for Big Cartel order #QFYO-589401 from laurenamywhitehouse@hotmail.co.uk'
    destination:
    dispute:
    disputed: false
    failure_balance_transaction:
    failure_code:
    failure_message:
    fraud_details: {}
    invoice:
    livemode: true
    metadata:
      in_person: 'false'
      plan: Platinum
    on_behalf_of:
    order:
    outcome:
      network_status: approved_by_network
      reason:
      risk_level: normal
      seller_message: Payment complete.
      type: authorized
    paid: true
    payment_intent: pi_3LhGn1BxWYA6NAEc1iTUZmn1
    payment_method: pm_1LhGmbBxWYA6NAEcR4jBWrJn
    payment_method_details:
      card:
        brand: visa
        checks:
          address_line1_check: pass
          address_postal_code_check: pass
          cvc_check: pass
        country: GB
        exp_month: 1
        exp_year: 2023
        fingerprint: lILyioImKWpQdy55
        funding: debit
        installments:
        last4: '4026'
        mandate:
        moto: false
        network: visa
        three_d_secure:
        wallet:
      type: card
    receipt_email:
    receipt_number:
    receipt_url: https://pay.stripe.com/receipts/payment/CAcQARoXChVhY2N0XzFJSFVvTUJ4V1lBNk5BRWMoneP9mAYyBmtsPVm4aTosFnBY99XDm6wv3JjqiFFAA8AvDEFZCW5Xm3wYkuZ5qELGNZQvZBYACDbSIGg
    refunded: false
    refunds:
      object: list
      data: []
      has_more: false
      total_count: 0
      url: "/v1/charges/ch_3LhGn1BxWYA6NAEc1m0S5sWZ/refunds"
    review:
    shipping:
      address:
        city: Newport
        country: GB
        line1: 2 Rose Cottages
        line2: Loverstone Lane
        postal_code: PO30 3EL
        state: 'Hampshire '
      carrier:
      name: Lauren Whitehouse
      phone: "+447903946758"
      tracking_number:
    source:
    source_transfer:
    statement_descriptor:
    statement_descriptor_suffix:
    status: succeeded
    transfer_data:
    transfer_group:
  has_more: false
  total_count: 1
  url: "/v1/charges?payment_intent=pi_3LhGn1BxWYA6NAEc1iTUZmn1"
client_secret: pi_3LhGn1BxWYA6NAEc1iTUZmn1_secret_2EuPy1ChTXyPI8DQ6qQyGFiDL
confirmation_method: manual
created: 1663005083
currency: gbp
customer:
description: 'Payment for Big Cartel order #QFYO-589401 from laurenamywhitehouse@hotmail.co.uk'
invoice:
last_payment_error:
livemode: true
metadata:
  in_person: 'false'
  plan: Platinum
next_action:
on_behalf_of:
payment_method: pm_1LhGmbBxWYA6NAEcR4jBWrJn
payment_method_options:
  card:
    installments:
    mandate_options:
    network:
    request_three_d_secure: automatic
payment_method_types:
- card
processing:
receipt_email:
review:
setup_future_usage:
shipping:
  address:
    city: Newport
    country: GB
    line1: 2 Car House
    line2: Cool house Lane
    postal_code: P110 3EL
    state: 'Hampshire '
  carrier:
  name: Lauren Whitehouse
  phone: "+447903946758"
  tracking_number:
`

	idType := cached_columns.ChInsertColumn{
		Name:             "id",
		DatabaseTypeName: "Int",
		Type:             reflect.TypeOf(12),
	}
	yamlType := cached_columns.ChInsertColumn{
		Name:             "yaml_column",
		DatabaseTypeName: "String",
		Type:             reflect.TypeOf(""),
	}

	columns := &cached_columns.ChTableColumnSet{
		Columns: []cached_columns.ChInsertColumn{
			idType,
			yamlType,
		},
		ColumnLookup: map[string]cached_columns.ChInsertColumn{
			"id":          idType,
			"yaml_column": yamlType,
		},
	}

	replicationRowEvent := &MysqlReplicationRowEvent{}
	replicationRowEvent.Action = "create"
	replicationRowEvent.EventColumnNames = []string{"id", "yaml_column"}
	replicationRowEvent.EventColumnTypes = []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_BLOB}
	table := schema.Table{Name: "test_table"}
	table.Columns = append(table.Columns, schema.TableColumn{Name: "id"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "yaml_column"})
	table.PKColumns = []int{0}
	replicationRowEvent.Table = &table
	rows := make([][]interface{}, 0)
	rows = append(rows, []interface{}{12, "old_yaml"})
	rows = append(rows, []interface{}{12, yaml})
	replicationRowEvent.Rows = rows

	isDuplicate, isColumnMismatch := tr.PopulateInsertData(replicationRowEvent, columns)

	if isDuplicate {
		t.Error("Expected row not to be flagged as duplicate")
	}

	if isColumnMismatch {
		t.Error("Expected row not to be flagged as column mismatch")
	}

	if replicationRowEvent.PkString() != "12" {
		t.Errorf("Expected id to be 12, got %s", replicationRowEvent.PkString())
	}

	if strings.Contains(string(replicationRowEvent.InsertData["yaml_column"].([]byte)), "2 Rose Cottages") {
		t.Errorf("Expected Event['yaml'] to anonymize address, got %s", replicationRowEvent.InsertData["yaml_column"])
	}
}

func BenchmarkEventToClickhouseRowData(b *testing.B) {
	tr := translator()
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	for n := 0; n < b.N; n++ {
		tr.PopulateInsertData(rowEvent, columns)
	}
}
