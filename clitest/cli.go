package main

import mongoc "gopkg.in/mongoc.v1"
import "fmt"
import "time"

func main() {
	pool := mongoc.NewPool("mongodb://loc.m:27017", 1, 1)
	col := pool.C("test", "abc")
	for {
		err := col.Insert(map[string]interface{}{
			"x": 1,
		})
		fmt.Println("insert->", err)
		time.Sleep(time.Second)
	}
}
