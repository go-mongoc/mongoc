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
	pool := NewPool("mongodb://loc.m:27017", 100, 10, 2)
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
		nil,
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
	//execute command
	//
	err = pool.Ping("test")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v\n", one)

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
		pool := NewPool("", 100, 10, 2)
		col := pool.C("test", "mongoc")
		col.Remove(nil, false)
	}()
	//test host error
	{
		pool := NewPool("mongodb://192.168.1.1:27017", 100, 10, 2)
		col := pool.C("test", "mongoc")
		err := col.Remove(nil, false)
		if err == nil {
			t.Error("not error")
			return
		}
		fmt.Println("test host err passed")
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
}
