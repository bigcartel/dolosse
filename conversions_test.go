package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/goccy/go-yaml"
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

	err := json.Unmarshal(parseValue(yaml, schema.TYPE_STRING, "order_transactions", "params").([]byte), &out)

	if err != nil {
		t.Fatal(err)
	} else if out.PrimaryFont != "Abril Fatface" {
		t.Fatalf("expected primary font to be Abril Fatface, got %s", out.PrimaryFont)
	}
}

func TestEventWithYaml(t *testing.T) {
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

	yamlColumns = append(yamlColumns, "test_table.yaml_column")
	columns := &ChColumnSet{
		columns: []ClickhouseQueryColumn{
			{
				Name: "id",
				Type: reflect.TypeOf(12),
			},
			{
				Name: "yaml_column",
				Type: reflect.TypeOf(""),
			},
		},
		columnLookup: map[string]bool{
			"id":          true,
			"yaml_column": true,
		},
	}

	replicationRowEvent := &MysqlReplicationRowEvent{}
	replicationRowEvent.Action = "create"
	table := schema.Table{Name: "test_table"}
	table.Columns = append(table.Columns, schema.TableColumn{Name: "id"})
	table.Columns = append(table.Columns, schema.TableColumn{Name: "yaml_column"})
	replicationRowEvent.Table = &table
	rows := make([][]interface{}, 0)
	rows = append(rows, []interface{}{12, "old_yaml"})
	rows = append(rows, []interface{}{12, yaml})
	replicationRowEvent.Rows = rows

	insertData, isDuplicate := eventToClickhouseRowData(replicationRowEvent, columns)

	if isDuplicate {
		t.Error("Expected row not to be flagged as duplicate")
	}

	if insertData.Id != 12 {
		t.Errorf("Expected id to be 12, got %d", insertData.Id)
	}

	if strings.Contains(string(insertData.Event["yaml_column"].([]byte)), "2 Rose Cottages") {
		t.Errorf("Expected Event['yaml'] to anonymize address, got %s", insertData.Event["yaml_column"])
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
	parsedValue := parseValue(inValue, schema.TYPE_STRING, "table", "column")
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

	out := struct {
		Password  string `json:"password"`
		Email     string `json:"email"`
		Firstname string `json:"firstname"`
	}{}

	err = json.Unmarshal(parseValue(string(yamlString), schema.TYPE_STRING, "order_transactions", "params").([]byte), &out)
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
	password := "test"
	out := parseValue(password, schema.TYPE_STRING, "some_table", "password").(string)
	if out == password {
		t.Fatalf("Expected password '%s' to be anonymized, got %s", password, out)
	}
}

func TestAnonymizeByteSlice(t *testing.T) {
	password := []byte("test")
	out := parseValue(password, schema.TYPE_STRING, "some_table", "password").(string)

	if out == string(password) {
		t.Fatalf("Expected byte slice of password '%s' to be anonymized, got %s", password, out)
	}
}
