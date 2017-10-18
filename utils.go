package mongoc

import bson "gopkg.in/bson.v2"
import "strings"

//ParseSorted will parse sorted string to bson.D, following by:
//-xx to xx:-1; xx to xx:1
func ParseSorted(sort ...string) (doc bson.D) {
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

//ParseDoc will parse doc to sort string, following by:
//doc.Value<0 to -xx; doc.Value>0 to xx:1
func ParseDoc(doc bson.D) (keys []string) {
	for _, d := range doc {
		val := d.Value.(int)
		if val > 0 {
			keys = append(keys, d.Name)
		} else {
			keys = append(keys, "-"+d.Name)
		}
	}
	return
}
