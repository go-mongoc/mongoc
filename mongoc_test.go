package mongoc

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	bson "gopkg.in/bson.v2"
)

func init() {
	LogTraceEnable()
}

func TestMongoc(t *testing.T) {
	pool := NewPool("mongodb://loc.m:27017", 100, 10)
	col := pool.C("test", "mongoc")
	//clear
	_, err := col.RemoveAll(nil)
	if err != nil {
		t.Error(err)
		return
	}
	count, err := col.Count(nil, 0, 0)
	if err != nil || count != 0 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//insert
	for i := 0; i < 10; i++ {
		rid := bson.NewObjectId().Hex()
		err = col.Insert(map[string]interface{}{
			"_id": rid,
			"a":   i,
			"b":   i % 3,
		})
		if err != nil {
			t.Error(err)
			return
		}
		err = col.FindID(rid, nil, &bson.M{})
		if err != nil {
			t.Error(err)
			return
		}
	}
	count, err = col.Count(nil, 0, 0)
	if err != nil || count != 10 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	var bvals = []int{}
	err = col.Distinct("b", nil, &bvals)
	if err != nil || len(bvals) != 3 {
		t.Errorf("count fail %v err:%v", len(bvals), err)
		return
	}
	fmt.Println(bvals)
	//find
	//
	var res = []map[string]interface{}{}
	err = col.Find(
		bson.M{
			"a": 1,
		}, bson.M{
			"a": 1,
		}, 0, 0, &res)
	if err != nil || len(res) != 1 {
		t.Errorf("find fail %v err:%v", len(res), err)
		return
	}
	var one = map[string]interface{}{}
	err = col.FindOne(
		bson.M{
			"a": 1,
		}, bson.M{
			"a": 1,
		}, &one)
	if err != nil {
		t.Errorf("find fail with err:%v", err)
		return
	}
	//
	one = map[string]interface{}{}
	err = col.FindOne(
		bson.M{
			"a": 199223,
		}, bson.M{
			"a": 1,
		}, &one)
	if err != ErrNotFound {
		t.Errorf("find fail with err:%v", err)
		return
	}
	//
	res = []map[string]interface{}{}
	err = col.Find(bson.M{
		"b": 1,
	}, bson.M{
		"a": 1,
		"b": 1,
	}, 0, 0, &res)
	if err != nil || len(res) != 3 {
		t.Errorf("find fail %v err:%v->%v", len(res), err, res)
		return
	}
	//update
	//
	err = col.UpdateMany(bson.M{
		"b": 1,
	}, bson.M{
		"$set": bson.M{
			"b": 100,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	count, err = col.Count(bson.M{
		"b": 100,
	}, 0, 0)
	if err != nil || count != 3 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//find and modify
	//
	one = map[string]interface{}{}
	changed, err := col.FindAndModify(
		bson.M{
			"b": 2,
		},
		bson.M{
			"$set": bson.M{
				"b": 300,
			},
		}, bson.M{
			"a": 1,
			"b": 1,
		}, false, true, &one)
	if err != nil {
		t.Error(err)
		return
	}
	if !changed.Matched || changed.Updated != 1 {
		t.Errorf("%v", changed)
		return
	}
	fmt.Println(one)
	//
	one = map[string]interface{}{}
	changed, err = col.FindAndModify(
		bson.M{
			"b": 122,
		},
		bson.M{
			"$set": bson.M{
				"b": 300,
			},
		}, bson.M{
			"a": 1,
			"b": 1,
		}, false, true, &one)
	if err != nil {
		t.Error(err)
		return
	}
	if changed.Matched || changed.Updated != 0 {
		t.Errorf("%v", changed)
		return
	}
	fmt.Println(one)
	//
	one = map[string]interface{}{}
	changed, err = col.Upsert(
		bson.M{
			"b": 1000,
		},
		bson.M{
			"$set": bson.M{
				"b": 1300,
			},
		})
	if err != nil {
		t.Error(err)
		return
	}
	if changed.Matched || changed.Updated != 1 {
		t.Errorf("%v", changed)
		return
	}
	fmt.Println(one)
	//
	count, err = col.Count(bson.M{
		"b": 300,
	}, 0, 0)
	if err != nil || count != 1 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//pipe
	//
	res = []map[string]interface{}{}
	err = col.Pipe([]bson.M{
		bson.M{
			"$match": bson.M{
				"b": 300,
			},
		},
		bson.M{
			"$project": bson.M{
				"_id": 1,
				"b":   1,
			},
		},
	}, &res)
	if err != nil || len(res) != 1 {
		t.Errorf("find fail %v err:%v->%v", len(res), err, res)
		return
	}
	//remove
	//
	removed, err := col.Remove(nil, true)
	if err != nil || removed != 1 {
		t.Error(err)
		return
	}
	count, err = col.Count(nil, 0, 0)
	if err != nil || count != 10 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//stats
	//
	one = map[string]interface{}{}
	err = col.Stats(nil, &one)
	if err != nil {
		t.Errorf("get stats fail with err:%v->%v", err, one)
		return
	}
	//rename
	//
	err = col.Rename("test", "mongoc2", true)
	if err != nil {
		t.Error(err)
		return
	}
	col2 := pool.C("test", "mongoc2")
	count, err = col2.Count(nil, 0, 0)
	if err != nil || count != 10 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//drop
	//
	err = col2.Drop()
	if err != nil {
		t.Error(err)
		return
	}
	count, err = col2.Count(nil, 0, 0)
	if err != nil || count != 0 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//execute client command
	//
	err = pool.Ping("test")
	if err != nil {
		t.Error(err)
	}
	// fmt.Printf("%v\n", one)

	//
	pool.Close()
}

func TestErrorFilter(t *testing.T) {
	ef := &DefaultErrorFilter{}
	if !ef.IsNormalError(nil) || ef.IsNormalError(fmt.Errorf("error")) || ef.IsNormalError(&BSONError{}) ||
		!ef.IsNormalError(&BSONError{Code: ErrCollectionNotExist}) {
		t.Error("error")
		return
	}
	if !ef.IsTempError(nil) || ef.IsTempError(fmt.Errorf("error")) || ef.IsTempError(&BSONError{}) ||
		!ef.IsTempError(&BSONError{Code: ErrServerSelectionFailure}) {
		t.Error("error")
		return
	}
}

func TestPool(t *testing.T) {
	//test normal pool
	{
		pool := NewPool("mongodb://loc.m:27017", 1, 10)
		//
		//check pop
		client := pool.Pop()
		pool.Push(client)
		client2 := pool.Pop()
		if client != client2 {
			t.Error("the client error")
			return
		}
		pool.Push(client2)
		fmt.Println("check pop...")
		//
		//check ping
		client = pool.Pop()
		client.LastError = &BSONError{Message: "other error"}
		pool.Push(client)
		client2 = pool.Pop()
		if client != client2 {
			t.Error("the client error")
			return
		}
		pool.Push(client2)
		fmt.Println("check ping...")
	}
	//test not reach, new timeout
	{
		fmt.Println("test new timeout is started...")
		pool := NewPool("mongodb://127.0.0.1:17017", 1, 10)
		pool.Timeout = 100 * time.Millisecond
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				} else {
					fmt.Println("test new timeout passed")
				}
			}()
			pool.Pop()
		}()
		fmt.Println("test new timeout done...")
	}
	//test not reach, ping timeout
	{
		fmt.Println("test ping timeout is started...")
		pool := NewPool("mongodb://127.0.0.1:17017", 1, 10)
		pool.Timeout = 100 * time.Millisecond
		pool.Err = &errFilter{Temp: true}
		//manual create client.
		<-pool.max
		client, _ := newClient(pool.URI)
		client.LastError = &BSONError{Message: "other error"}
		pool.Push(client)
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				} else {
					fmt.Println("test ping timeout passed")
				}
			}()
			pool.Pop()
		}()
		fmt.Println("test ping timeout done...")
	}
	//test ping error
	{
		fmt.Println("test ping error is started...")
		pool := NewPool("mongodb://127.0.0.1:17017", 1, 10)
		pool.Timeout = 100 * time.Millisecond
		pool.Err = &errFilter{Temp: false}
		//manual create client.
		<-pool.max
		client, _ := newClient(pool.URI)
		client.LastError = &BSONError{Message: "other error"}
		pool.Push(client)
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				} else {
					fmt.Println("test ping error passed")
				}
			}()
			pool.Pop()
		}()
		fmt.Println("test ping error done...")
	}
}

type errFilter struct {
	DefaultErrorFilter
	Temp bool
}

func (e *errFilter) IsTempError(err error) bool {
	return e.Temp
}

func TestCommand(t *testing.T) {
	InitShared("mongodb://loc.m:27017", "test")
	col := SharedC("mongoc")
	//clear
	_, err := col.RemoveAll(nil)
	if err != nil {
		t.Error(err)
		return
	}
	count, err := col.Count(nil, 0, 0)
	if err != nil || count != 0 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	//insert
	for i := 0; i < 10; i++ {
		err = col.Insert(map[string]interface{}{
			"a": i,
			"b": i % 3,
		})
		if err != nil {
			t.Error(err)
			return
		}
	}
	count, err = col.Count(nil, 0, 0)
	if err != nil || count != 10 {
		t.Errorf("count fail %v err:%v", count, err)
		return
	}
	reply := []bson.M{}
	// one := bson.M{}
	err = SharedExecute(
		bson.D{
			{
				Name:  "find",
				Value: "mongoc",
			},
		}, nil, &reply)
	if err != nil || len(reply) != 10 {
		t.Errorf("err %v,%v, %v reply", err, reply, len(reply))
		return
	}
	fmt.Println("-->", reply)
}
func TestErrCase(t *testing.T) {
	//test uri invalid
	func() {
		defer func() {
			err := recover()
			if err == nil {
				t.Error("not panic")
			} else {
				fmt.Println("test uri empty passed")
			}
		}()
		pool := NewPool("", 100, 10)
		col := pool.C("test", "mongoc")
		col.Remove(nil, false)
	}()
	//test max size error
	func() {
		defer func() {
			err := recover()
			if err == nil {
				t.Error("not panic")
			} else {
				fmt.Println("test max size error passed")
			}
		}()
		NewPool("", 0, 10)
	}()
	//test closed error
	{
		pool := NewPool("mongodb://loc.m:27017", 100, 10)
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			pool.Push(nil)
		}()
		pool.Close()
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			pool.Pop()
		}()
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			pool.Push(nil)
		}()
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			pool.C("test", "mongoc")
		}()
	}
	//test manual create client
	{
		client := Client{}
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			client.rawCollection("test", "mongoc")
		}()
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			client.Execute("test", nil, nil, nil)
		}()
		func() {
			defer func() {
				err := recover()
				if err == nil {
					t.Error("not error")
				}
			}()
			client.SetErrVer(2)
		}()
	}
	//test parse bson error
	{
		pool := NewPool("mongodb://loc.m:27017", 100, 10)
		col := pool.C("test", "mongoc")
		_, err := col.Remove(TestErrCase, false)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.Stats(TestErrCase, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.Insert(TestErrCase)
		if err == nil {
			t.Error("not error")
			return
		}
		_, err = col.Count(TestErrCase, 0, 0)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.Find(nil, TestErrCase, 0, 0, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.Find(TestErrCase, nil, 0, 0, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.PipeWithFlags(QueryNone, map[string]interface{}{}, TestErrCase, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.PipeWithFlags(QueryNone, TestErrCase, nil, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.Update(nil, TestErrCase, true, true)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.Update(TestErrCase, nil, true, true)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.PipeWithFlags(QueryNone, TestErrCase, nil, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		_, err = col.FindAndModify(TestErrCase, bson.M{"c": 100}, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		_, err = col.FindAndModify(nil, TestErrCase, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		_, err = col.FindAndModify(nil, bson.M{"c": 100}, TestErrCase, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		_, err = col.FindAndModify(nil, nil, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = pool.Execute("test", TestErrCase, nil, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = pool.Execute("test", map[string]interface{}{}, TestErrCase, nil)
		if err == nil {
			t.Error("not error")
			return
		}
	}
	{ //test bson error
		_, err := parseBSON([]byte(""))
		if err == nil {
			t.Error("not error")
			return
		}
	}
	// { //test umashal error
	// 	pool := NewPool("mongodb://loc.m:27017", 100, 10)
	// 	col := pool.C("test", "mongoc")
	// 	err := col.Insert(bson.M{
	// 		"xx": "xkd",
	// 		"b":  2,
	// 	})
	// 	if err != nil {
	// 		t.Error("not error")
	// 		return
	// 	}
	// 	val := []*errorItem{}
	// 	err = col.Find(nil, nil, 0, 0, &val)
	// 	if err == nil {
	// 		t.Errorf("-->%v", val)
	// 		return
	// 	}
	// }
	{ //test check index error
		pool := NewPool("mongodb://loc.m:27017", 100, 10)
		err := pool.CheckIndex("test", map[string][]*Index{
			"xkkd": []*Index{&Index{}},
		}, true)
		if err == nil {
			t.Error("not error")
			return
		}
	}
}

type serverErrPool struct {
	client *Client
}

func (s *serverErrPool) Pop() *Client {
	return s.client
}

func (s *serverErrPool) Push(client *Client) {

}
func TestServerErrCase(t *testing.T) {
	//test server error
	{
		var err error
		pool := &serverErrPool{}
		pool.client, err = newClient("mongodb://127.0.0.1:17017")
		if err != nil {
			t.Error("err")
			return
		}
		pool.client.SetErrVer(2)
		pool.client.Pool = pool
		//
		col := &Collection{
			DbName: "test",
			Name:   "mongoc",
			Pool:   pool,
		}
		err = col.Insert(map[string]interface{}{
			"a": 100,
			"b": 3,
		})
		if err == nil {
			t.Error("not error")
			return
		}
		fmt.Println(err)
		//
		one := map[string]interface{}{}
		err = col.Stats(nil, &one)
		if err == nil {
			t.Error("not error")
			return
		}
		res := []map[string]interface{}{}
		err = col.Find(nil, nil, 0, 0, &res)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		_, err = col.Count(nil, 0, 0)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.Update(nil, bson.M{"c": 100}, true, true)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		_, err = col.FindAndModify(nil, bson.M{"c": 100}, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = pool.client.Ping("test")
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.Rename("test", "nmongoc", false)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		_, err = col.Remove(nil, false)
		if err == nil {
			t.Error("not error")
			return
		}
		//
		err = col.Drop()
		if err == nil {
			t.Error("not error")
			return
		}
		//
		//test index.
		col2 := &Collection{
			DbName: "test",
			Name:   "xxxk",
			Pool:   pool,
		}
		err = col2.CheckIndex(true,
			&Index{
				Name: "xa",
				Key:  []string{"xa"},
			})
		if err == nil {
			t.Error(err)
			return
		}
		err = col2.CheckIndex(false,
			&Index{
				Name: "xa",
				Key:  []string{"xa"},
			},
			&Index{
				Name: "xb",
				Key:  []string{"xb"},
			})
		if err == nil {
			t.Error(err)
			return
		}
		//
		bulk := col2.NewBulk(false)
		bulk.Insert(bson.M{"a": 1})
		_, err = bulk.Execute()
		if err == nil {
			t.Error(err)
			return
		}
	}
}

func TestVersion(t *testing.T) {
	//for call well
	fmt.Printf("%v.%v.%v\n", MajorVersion(), MinorVersion(), MicroVersion())
	fmt.Println(Version())
	//
	if !CheckVersion(1, 8, 1) {
		t.Error("error")
		return
	}
	if CheckVersion(1, 9, 1) {
		t.Error("error")
		return
	}
}

func TestLog(t *testing.T) {
	//
	//for call well
	LogTraceEnable()
	LogTraceDisable()
	//
	//for call well
	LogHandler(LogLevelCritical, "testing", "1")
	LogHandler(LogLevelDebug, "testing", "2")
	LogHandler(LogLevelError, "testing", "3")
	LogHandler(LogLevelInfo, "testing", "4")
	LogHandler(LogLevelMessage, "testing", "5")
	LogHandler(LogLevelTrace, "testing", "6")
	LogHandler(LogLevelWarning, "testing", "7")
	LogHandler(1000, "testing", "7")
}

func runCreateFind(col *Collection, rid int64) {
	err := col.Insert(map[string]interface{}{
		"bench":   rid,
		"testing": "testing",
	})
	if err != nil {
		panic(err)
	}
	res := []map[string]interface{}{}
	err = col.Find(bson.M{
		"bench":   rid,
		"testing": "testing",
	}, nil, 0, 1, &res)
	if err != nil {
		panic(err)
	}
	if len(res) < 1 {
		panic("not found")
	}
	if res[0]["bench"].(int64) != rid {
		panic("data error")
	}
}

func TestIndexes(t *testing.T) {
	InitShared("loc.m:27017", "test")
	col := SharedC("testindex")
	err := col.Drop()
	if err != nil {
		t.Error(err)
		return
	}
	err = SharedCheckIndex(
		map[string][]*Index{
			"testindex": []*Index{
				{
					Name: "xa",
					Key:  []string{"xa"},
				},
			},
		}, true)
	if err != nil {
		t.Error(err)
		return
	}
	err = SharedCheckIndex(
		map[string][]*Index{
			"testindex": []*Index{
				{
					Name: "xa",
					Key:  []string{"xa"},
				},
				{
					Name: "xb",
					Key:  []string{"-xb"},
				},
			},
		}, false)
	if err != nil {
		t.Error(err)
		return
	}
	indexes, err := col.ListIndexes()
	if err != nil {
		t.Error(err)
		return
	}
	var xa, xb bool
	for _, index := range indexes {
		switch index.Name {
		case "xa":
			if len(index.Key) < 1 || index.Key[0] != "xa" {
				t.Error("index error")
				return
			}
			xa = true
		case "xb":
			if len(index.Key) < 1 || index.Key[0] != "-xb" {
				t.Error("index error")
				return
			}
			xb = true
		}
	}
	if !(xa && xb) {
		t.Error("error")
		return
	}
	//
	//test collection not exist
	err = col.Drop()
	if err != nil {
		t.Error(err)
		return
	}
	err = SharedCheckIndex( //drop first
		map[string][]*Index{
			"testindex": []*Index{
				{
					Name: "xa",
					Key:  []string{"xa"},
				},
			},
		}, true)
	if err != nil {
		t.Error(err)
		return
	}
	err = col.Drop()
	if err != nil {
		t.Error(err)
		return
	}
	err = SharedCheckIndex( //not drop first
		map[string][]*Index{
			"testindex": []*Index{
				{
					Name: "xa",
					Key:  []string{"xa"},
				},
			},
		}, false)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestBulk(t *testing.T) {
	pool := NewPool("mongodb://loc.m:27017", 100, 1)
	col := pool.C("test", "mongoc")
	col.Remove(nil, false)
	docs := []interface{}{}
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.M{
			"a": 1,
			"b": i % 3,
		})
	}
	bulk := col.NewBulk(true)
	bulk.Insert(docs...)
	bulk.Remove(bson.M{ //remove a:2,a:5,a:8
		"b": 2,
	})
	bulk.RemoveOne(bson.M{ //remove a:9
		"a": 9,
	})
	bulk.Replace(bson.M{ //replace a:0
		"a": 0,
	}, bson.M{
		"a": 200,
		"b": 0,
	}, false)
	bulk.Update( //update a:1,a:4,a:7
		bson.M{
			"b": 1,
		}, bson.M{
			"$set": bson.M{
				"b": 100,
			},
		}, false)
	bulk.UpdateOne( //update a:6
		bson.M{
			"a": 6,
		}, bson.M{
			"$set": bson.M{
				"a": 100,
			},
		}, false)
	reply, err := bulk.Execute()
	if err != nil {
		t.Error(err)
		return
	}
	if reply.Inserted != 10 || reply.Matched != 3 ||
		reply.Modified != 3 || reply.Removed != 3 ||
		reply.Upserted != 0 || len(reply.Errors) > 0 {
		fmt.Println(reply)
		t.Error(reply)
		return
	}
	//
	//test bulk error
	//
	bulk = col.NewBulk(true)
	bulk.Insert(nil)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	//
	bulk = col.NewBulk(true)
	bulk.Remove(nil)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	//
	bulk = col.NewBulk(true)
	bulk.RemoveOne(nil)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	//
	bulk = col.NewBulk(true)
	bulk.Replace(bson.M{
		"a": 0,
	}, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	bulk = col.NewBulk(true)
	bulk.Replace(nil, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	//
	bulk = col.NewBulk(true)
	bulk.Update(
		bson.M{
			"b": 1,
		}, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	bulk = col.NewBulk(true)
	bulk.Update(nil, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	//
	bulk = col.NewBulk(true)
	bulk.UpdateOne(
		bson.M{
			"b": 1,
		}, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
	bulk = col.NewBulk(true)
	bulk.UpdateOne(nil, nil, false)
	_, err = bulk.Execute()
	if err == nil {
		t.Error(err)
		return
	}
}

func TestCreateFind(t *testing.T) {
	pool := NewPool("mongodb://loc.m:27017", 100, 1)
	col := pool.C("test", "mongoc")
	col.Remove(nil, false)
	err := pool.CheckIndex("test", map[string][]*Index{
		"mongoc": []*Index{
			{
				Name: "bench",
				Key:  []string{"bench"},
			},
		},
	}, true)
	if err != nil {
		t.Error(err)
		return
	}
	runCreateFind(col, 0)
}

func BenchmarkMongoc(b *testing.B) {
	InitShared("loc.m:27017", "test")
	col := SharedC("mongoc")
	col.Remove(nil, false)
	err := SharedCheckIndex(
		map[string][]*Index{
			"mongoc": []*Index{
				{
					Name: "bench",
					Key:  []string{"bench"},
				},
			},
		}, false)
	if err != nil {
		b.Error(err)
		return
	}
	ridx := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rid := atomic.AddInt64(&ridx, 1)
			runCreateFind(col, rid)
		}
	})
}
