package mongoc

import bson "gopkg.in/bson.v2"
import "strings"

//ParseSort will parse sort string to bson.D, following by:
//-xx to xx:-1; xx to xx:1
func ParseSort(sort ...string) (doc bson.D) {
	for _, s := range sort {
		if strings.HasPrefix(s, "-") {
			doc = append(doc, bson.DocElem{
				Name:  strings.TrimPrefix(s, "-"),
				Value: -1,
			})
		} else {
			doc = append(doc, bson.DocElem{
				Name:  s,
				Value: 1,
			})
		}
	}
	return
}
