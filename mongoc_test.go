package mongoc

import (
	"fmt"
	"testing"

	bson "gopkg.in/bson.v2"
)

func init() {
	LogTraceEnable()
}

func TestMongoc(t *testing.T) {
	pool := NewPool("mongodb://loc.m:27017", 100, 10)
	col := pool.C("test", "mongoc")
	//clear
	err := col.Remove(nil, false)
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
	//find
	//
	var res = []map[string]interface{}{}
	err = col.Find(bson.M{
		"a": 1,
	}, bson.M{
		"a": 1,
	}, 0, 0, &res)
	if err != nil || len(res) != 1 {
		t.Errorf("find fail %v err:%v", len(res), err)
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
	var one = map[string]interface{}{}
	err = col.FindAndModify(
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
	err = col.Remove(nil, true)
	if err != nil {
		t.Error(err)
		return
	}
	count, err = col.Count(nil, 0, 0)
	if err != nil || count != 9 {
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
	if err != nil || count != 9 {
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
		err := col.Remove(TestErrCase, false)
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
		err = col.FindAndModify(TestErrCase, bson.M{"c": 100}, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.FindAndModify(nil, TestErrCase, nil, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.FindAndModify(nil, bson.M{"c": 100}, TestErrCase, true, true, nil)
		if err == nil {
			t.Error("not error")
			return
		}
		err = col.FindAndModify(nil, nil, nil, true, true, nil)
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
		err = col.FindAndModify(nil, bson.M{"c": 100}, nil, true, true, nil)
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
		err = col.Remove(nil, false)
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
