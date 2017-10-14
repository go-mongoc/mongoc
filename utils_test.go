package mongoc

import (
	"testing"
)

func TestParseSort(t *testing.T) {
	sort := ParseSort("-a", "b")
	if len(sort) != 2 {
		t.Error("parse error")
		return
	}
	if sort[0].Name != "a" || sort[1].Name != "b" {
		t.Error("parse error")
		return
	}
}
