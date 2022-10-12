package main

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
)

func TestEventToClickhouseRowData(t *testing.T) {
	yamlColumnSetup()
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	isDuplicate := rowEvent.ParseInsertData(columns)

	if isDuplicate {
		t.Error("Expected row not to be flagged as duplicate")
	}

	if rowEvent.PkString() != "12" {
		t.Errorf("Expected id to be 12, got %s", rowEvent.PkString())
	}

	if rowEvent.InsertData["id"] != 12 {
		t.Errorf("Expected Event['id'] to be 12, got %d", rowEvent.InsertData["id"])
	}

	if rowEvent.InsertData["name"] != "asdf" {
		t.Errorf("Expected Event['name'] to be asdf, got %s", rowEvent.InsertData["name"])
	}

	if rowEvent.InsertData["description"] != "pretty cool" {
		t.Errorf("Expected Event['description'] to be 'pretty cool', got %s", rowEvent.InsertData["description"])
	}
}

func TestEventWithYaml(t *testing.T) {
	yamlColumnSetup()
	Config.AnonymizeFields = []*regexp.Regexp{
		regexp.MustCompile(".*address.*"),
	}
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
	table.PKColumns = []int{0}
	replicationRowEvent.Table = &table
	rows := make([][]interface{}, 0)
	rows = append(rows, []interface{}{12, "old_yaml"})
	rows = append(rows, []interface{}{12, yaml})
	replicationRowEvent.Rows = rows

	isDuplicate := replicationRowEvent.ParseInsertData(columns)

	if isDuplicate {
		t.Error("Expected row not to be flagged as duplicate")
	}

	if replicationRowEvent.PkString() != "12" {
		t.Errorf("Expected id to be 12, got %s", replicationRowEvent.PkString())
	}

	if strings.Contains(string(replicationRowEvent.InsertData["yaml_column"].([]byte)), "2 Rose Cottages") {
		t.Errorf("Expected Event['yaml'] to anonymize address, got %s", replicationRowEvent.InsertData["yaml_column"])
	}
}

func BenchmarkEventToClickhouseRowData(b *testing.B) {
	yamlColumnSetup()
	columns := makeColumnSet()
	rowEvent := makeRowEvent()

	for n := 0; n < b.N; n++ {
		rowEvent.ParseInsertData(columns)
	}
}
