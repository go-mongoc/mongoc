package mongoc

import (
	"testing"

	bson "gopkg.in/bson.v2"
)

func TestSorted(t *testing.T) {
	doc := ParseSorted("-a", "b")
	if len(doc) != 2 {
		t.Error("parse error")
		return
	}
	if doc[0].Name != "a" || doc[1].Name != "b" {
		t.Error("parse error")
		return
	}
	sorted := ParseDoc(bson.D{
		{
			Name:  "a",
			Value: -1,
		},
		{
			Name:  "b",
			Value: 1,
		},
	})
	if len(sorted) != 2 || sorted[0] != "-a" || sorted[1] != "b" {
		t.Error("parse error")
		return
	}
}
