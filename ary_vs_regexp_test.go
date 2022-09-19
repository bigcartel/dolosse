package main

import (
	"regexp"
	"testing"
)

func BenchmarkMatchFieldsArray(b *testing.B) {
	for n := 0; n < b.N; n++ {
		testString := "my_cool_latitude"
		isAnonymizedField(testString)
	}
}

func BenchmarkMatchFieldsRegex(b *testing.B) {
	matcher := regexp.MustCompile(".*(address|street|password|salt|email|longitude|latitude).*|payment_methods.properties|products.description")

	for n := 0; n < b.N; n++ {
		testString := "my_cool_latitude"
		if matcher.MatchString(testString) {
			continue
		}
	}
}

func BenchmarkMatchFieldsRegexConvert(b *testing.B) {
	matcher := regexp.MustCompile(".*(address|street|password|salt|email|longitude|latitude).*|payment_methods.properties|products.description")

	for n := 0; n < b.N; n++ {
		testString := "my_cool_latitude"
		if matcher.Match([]byte(testString)) {
			continue
		}
	}
}
