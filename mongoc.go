// Package mongoc is the go binding for libmongoc
//
package mongoc

/*
#include <mongoc.h>
#cgo darwin CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo darwin LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
#cgo linux CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo linux LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
bool cmgo_ping(mongoc_client_t* client,char** reply, bson_error_t* error);
bson_t* cmgo_new_bson_from_json(const char* json,bson_error_t* error);
*/
import "C"
import (
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"gopkg.in/bson.v2"
)

func init() {
	C.mongoc_init()
}

type QueryFlags C.mongoc_query_flags_t

//
var QueryNone = QueryFlags(C.MONGOC_QUERY_NONE)
var QueryTailableCursor = QueryFlags(C.MONGOC_QUERY_TAILABLE_CURSOR)
var QuerySlaveOk = QueryFlags(C.MONGOC_QUERY_SLAVE_OK)
var QueryOplogReplay = QueryFlags(C.MONGOC_QUERY_OPLOG_REPLAY)
var QueryNoCursorTimeout = QueryFlags(C.MONGOC_QUERY_NO_CURSOR_TIMEOUT)
var QueryAwaitData = QueryFlags(C.MONGOC_QUERY_AWAIT_DATA)
var QueryExhaust = QueryFlags(C.MONGOC_QUERY_EXHAUST)
var QueryPartial = QueryFlags(C.MONGOC_QUERY_PARTIAL)

//

/**** version ****/

//CheckVersion

//Version return the version of libmongoc.
func Version() string {
	return C.GoString(C.mongoc_get_version())
}

//CheckVersion return true if libmongoc’s version is greater than or equal to the required version.
func CheckVersion(requiredMajor, requiredMinor, requiredMicro int) bool {
	return bool(C.mongoc_check_version(C.int(requiredMajor), C.int(requiredMinor), C.int(requiredMicro)))
}

//MajorVersion return the value of MONGOC_MAJOR_VERSION when libmongoc was compiled.
func MajorVersion() int {
	return int(C.mongoc_get_major_version())
}

//MicroVersion return the value of MONGOC_MICRO_VERSION when libmongoc was compiled.
func MicroVersion() int {
	return int(C.mongoc_get_micro_version())
}

//MinorVersion return the value of MONGOC_MINOR_VERSION when libmongoc was compiled.
func MinorVersion() int {
	return int(C.mongoc_get_minor_version())
}

/**** bson error ****/

//BSONError is the wrapper of bson_error_t.
type BSONError struct {
	Domain  uint32
	Code    uint32
	Message string
}

//parse bson_error_t to BSONError
func parseBSONError(err *C.bson_error_t) (berr *BSONError) {
	berr = &BSONError{
		Domain: uint32(err.domain),
		Code:   uint32(err.code),
	}
	berr.Message = C.GoString(&err.message[0])
	return berr
}

//Error is the golang error impl.
func (b *BSONError) Error() string {
	return fmt.Sprintf("BSONError(domain:%v,code:%v,message:%v)", b.Domain, b.Code, b.Message)
}

/**** bson ****/

//BSON is the wrapper of C.bson_t
type BSON struct {
	raw *C.bson_t
}

//NewBSON will create the BSON wrapper, following:
//
//	if v is []byte, parsing by C.bson_new_from_data
//
//	if v is other type, parsing value by bson.Marshal to []byte, then create bson by C.bson_new_from_data
func NewBSON(v interface{}) (bson *BSON, err error) {
	bson = &BSON{}
	switch v.(type) {
	case []byte:
		bson.raw, err = newRawBSON(v.([]byte))
	default:
		bson.raw, err = marshalRawBSON(v)
	}
	return
}

func parseBSON(v interface{}) (bson *C.bson_t, err error) {
	switch v.(type) {
	case []byte:
		bson, err = newRawBSON(v.([]byte))
	default:
		bson, err = marshalRawBSON(v)
	}
	return
}

func newRawBSON(bys []byte) (bson *C.bson_t, err error) {
	cbys := (*C.uint8_t)(C.CBytes(bys))
	defer C.free(unsafe.Pointer(cbys))
	bson = C.bson_new_from_data(cbys, C.size_t(len(bys)))
	if bson == nil {
		err = fmt.Errorf("pasing bytes to C.bson_t fail")
	}
	return
}

func marshalRawBSON(val interface{}) (bval *C.bson_t, err error) {
	bys, err := bson.Marshal(val)
	if err != nil {
		return
	}
	bval, err = newRawBSON(bys)
	return
}

//Release will destory the C.bson_t
func (b *BSON) Release() {
	if b.raw != nil {
		C.bson_destroy(b.raw)
	}
}

/**** pool ****/

//Pool is the pool of client
type Pool struct {
	URI  string //the client uri.
	pool chan *Client
	max  chan int
	//
	errorAPIVer uint32
	maxSize     uint32
	minSize     uint32
}

//NewPool will create the pool by size.
func NewPool(uri string, maxSize, minSize, erroAPIVer uint32) (pool *Pool) {
	if maxSize < 1 {
		panic("the pool must greater zero")
	}
	pool = &Pool{
		pool:        make(chan *Client, maxSize),
		max:         make(chan int, maxSize),
		errorAPIVer: erroAPIVer,
		maxSize:     maxSize,
		minSize:     minSize,
		URI:         uri,
	}
	for i := uint32(0); i < maxSize; i++ {
		pool.max <- 1
	}
	return
}

//Pop will try pop on client from pool, following
//
//	if having idle, pop one from pool
//
//	if pool is not full, create one
func (p *Pool) Pop() *Client {
	for {
		select {
		case found := <-p.pool:
			return found
		case <-p.max:
			client, err := NewClient(p.URI)
			if err != nil {
				fmt.Printf("pop fail with:%v, will retry after 3s\n", err)
				time.Sleep(3 * time.Second)
				p.max <- 1 //push back to the max chan.
				break
			}
			client.Pool = p
			return client
		}
	}
}

//Push will push one client to pool
func (p *Pool) Push(client *Client) {
	p.pool <- client
}

func (p *Pool) C(dbname, colname string) *Collection {
	return &Collection{
		Name:   colname,
		DbName: dbname,
		Pool:   p,
	}
}

//Execute one command.
func (p *Pool) Execute(dbname string, cmds, v interface{}) (err error) {
	client := p.Pop()
	defer client.Close()
	return client.Execute(dbname, cmds, v)
}

/**** client ****/

//Client is the wrapper of C.mongoc_client_t.
type Client struct {
	URI    string
	Pool   *Pool
	raw    *C.mongoc_client_t
	cols   map[string]*rawCollection
	colLck sync.RWMutex
}

//NewClient will create client by C.mongoc_client_new.
func NewClient(uri string) (client *Client, err error) {
	curistr := C.CString(uri)
	defer C.free(unsafe.Pointer(curistr))
	raw := C.mongoc_client_new(curistr)
	if raw == nil {
		err = fmt.Errorf("create client fail by uri(%v)", uri)
	} else {
		client = &Client{
			URI:  uri,
			raw:  raw,
			cols: map[string]*rawCollection{},
		}
	}
	return
}

// func (c *Client) DB(dbname string) *Database {
// 	cdbname := C.CString(dbname)
// 	defer C.free(unsafe.Pointer(cdbname))
// 	return &Database{
// 		Name: dbname,
// 		db:   C.mongoc_client_get_database(c.client, cdbname),
// 	}
// }

// func (c *Client) Collection(dbname, colname string) *Collection {
// 	col := &Collection{
// 		Name:   colname,
// 		DbName: dbname,
// 		Pool
// 	}
// 	// c.cols[key] = col
// 	return col
// }

// func (c *Client) Ping() (reply string, err error) {
// var berr C.bson_error_t
// var creply *C.char
// if bool(C.cmgo_ping(c.client, &creply, &berr)) {
// 	reply = C.GoString(creply)
// 	C.bson_free(unsafe.Pointer(creply))
// } else {
// 	err = newBsonError(&berr)
// }
// return
// }

//Close will following
//
//	if c.Pool is nil, call Relase to destory raw client
//
//	if c.Pool is not nil, push client to pool
func (c *Client) Close() {
	if c.Pool == nil {
		c.Release()
	} else {
		c.Pool.Push(c)
	}
}

//Release will destory client
func (c *Client) Release() {
	if c.raw != nil {
		C.mongoc_client_destroy(c.raw)
	}
}

func (c *Client) rawCollection(dbname, colname string) *rawCollection {
	if c.raw == nil {
		panic("raw client is nil")
	}
	c.colLck.Lock()
	defer c.colLck.Unlock()
	key := fmt.Sprintf("%v-%v", dbname, colname)
	col, ok := c.cols[key]
	if ok {
		return col
	}
	cdbname := C.CString(dbname)
	ccolname := C.CString(colname)
	col = &rawCollection{
		raw: C.mongoc_client_get_collection(c.raw, cdbname, ccolname),
	}
	C.free(unsafe.Pointer(cdbname))
	C.free(unsafe.Pointer(ccolname))
	c.cols[key] = col
	return col
}

//Execute one command
func (c *Client) Execute(dbname string, cmds, v interface{}) (err error) {
	var rawCmds *C.bson_t
	rawCmds, err = parseBSON(cmds)
	if err != nil {
		return
	}
	cdbname := C.CString(dbname)
	var db = C.mongoc_client_get_database(c.raw, cdbname)
	var berr C.bson_error_t
	var doc C.bson_t
	if C.mongoc_database_write_command_with_opts(db, rawCmds, nil, &doc, &berr) {
		var str = C.bson_get_data(&doc)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
		err = bson.Unmarshal(mbys, v)
		C.bson_destroy(&doc)
	} else {
		err = parseBSONError(&berr)
	}
	C.mongoc_database_destroy(db)
	C.free(unsafe.Pointer(cdbname))
	C.bson_destroy(rawCmds)
	return
}

type rawCollection struct {
	raw *C.mongoc_collection_t
}

func (r *rawCollection) Release() {
	if r.raw != nil {
		C.mongoc_collection_destroy(r.raw)
	}
}

//Collection is the wrapper of C.mongoc_collection_t
type Collection struct {
	Name   string
	DbName string
	Pool   *Pool
}

//Insert many document to database.
func (c *Collection) Insert(docs ...interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var bdoc *C.bson_t
	var bdocs []*C.bson_t
	defer func() {
		client.Close()
		for _, bdoc = range bdocs {
			if bdoc != nil {
				C.bson_destroy(bdoc)
			}
		}
	}()
	for _, doc := range docs {
		bdoc, err = parseBSON(doc)
		if err != nil {
			return
		}
		bdocs = append(bdocs, bdoc)
	}
	var berr C.bson_error_t
	if !C.mongoc_collection_insert_bulk(col.raw, C.MONGOC_INSERT_NONE, (**C.bson_t)(&bdocs[0]), C.uint32_t(len(bdocs)), nil, &berr) {
		// if !C.mongoc_collection_insert(col, C.MONGOC_INSERT_NONE, bdocs[0], nil, &berr) {
		err = parseBSONError(&berr)
	}
	return
}

//Update document to database by upsert or manay
func (c *Collection) Update(selector, update interface{}, upsert, many bool) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawSelector, rawUpdate *C.bson_t
	defer func() {
		client.Close()
		if rawSelector != nil {
			C.bson_destroy(rawSelector)
		}
		if rawUpdate != nil {
			C.bson_destroy(rawUpdate)
		}
	}()
	if selector != nil {
		rawSelector, err = parseBSON(selector)
		if err != nil {
			return
		}
	}
	if update != nil {
		rawUpdate, err = parseBSON(update)
		if err != nil {
			return
		}
	}
	var flags = C.MONGOC_UPDATE_NONE
	if upsert {
		flags = flags | C.MONGOC_UPDATE_UPSERT
	}
	if many {
		flags = flags | C.MONGOC_UPDATE_MULTI_UPDATE
	}
	var berr C.bson_error_t
	if !C.mongoc_collection_update(col.raw, C.mongoc_update_flags_t(flags), rawSelector, rawUpdate, nil, &berr) {
		err = parseBSONError(&berr)
	}
	return
}

//UpdateMany document to database
func (c *Collection) UpdateMany(selector, update interface{}) (err error) {
	return c.Update(selector, update, false, true)
}

//Remove document to database by single
func (c *Collection) Remove(selector interface{}, single bool) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawSelector *C.bson_t
	defer func() {
		client.Close()
		if rawSelector != nil {
			C.bson_destroy(rawSelector)
		}
	}()
	if selector == nil {
		selector = map[string]interface{}{}
	}
	rawSelector, err = parseBSON(selector)
	if err != nil {
		return
	}
	var flags = C.MONGOC_REMOVE_NONE
	if single {
		flags = flags | C.MONGOC_REMOVE_SINGLE_REMOVE
	}
	var berr C.bson_error_t
	if !C.mongoc_collection_remove(col.raw, C.mongoc_remove_flags_t(flags), rawSelector, nil, &berr) {
		err = parseBSONError(&berr)
	}
	return
}

//FindAndModifyWithFlags will find and modify document on database.
func (c *Collection) FindAndModifyWithFlags(query, sort, update, fields interface{}, remove, upsert, retnew bool, v interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawQuery, rawSort, rawUpdate, rawFields *C.bson_t
	defer func() {
		client.Close()
		if rawQuery != nil {
			C.bson_destroy(rawQuery)
		}
		if rawSort != nil {
			C.bson_destroy(rawSort)
		}
		if rawUpdate != nil {
			C.bson_destroy(rawUpdate)
		}
		if rawFields != nil {
			C.bson_destroy(rawFields)
		}
	}()
	if query != nil {
		rawQuery, err = parseBSON(query)
		if err != nil {
			return
		}
	}
	if sort != nil {
		rawSort, err = parseBSON(sort)
		if err != nil {
			return
		}
	}
	if update != nil {
		rawUpdate, err = parseBSON(update)
		if err != nil {
			return
		}
	}
	if fields != nil {
		rawFields, err = parseBSON(fields)
		if err != nil {
			return
		}
	}
	var berr C.bson_error_t
	var doc C.bson_t
	if !C.mongoc_collection_find_and_modify(col.raw,
		rawQuery, rawSort, rawUpdate, rawFields,
		C.bool(remove), C.bool(upsert), C.bool(retnew),
		&doc, &berr) {
		err = parseBSONError(&berr)
		return
	}
	var str = C.bson_get_data(&doc)
	mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
	err = bson.Unmarshal(mbys, v)
	return
}

//FindAndModify document on database.
func (c *Collection) FindAndModify(query, sort, update, fields interface{}, upsert, retnew bool, v interface{}) (err error) {
	return c.FindAndModifyWithFlags(query, sort, update, fields, false, upsert, retnew, v)
}

//parse cursor to value.
func (c *Collection) parseCursor(cursor *C.mongoc_cursor_t, val interface{}) (err error) {
	var doc *C.bson_t
	targetVal := reflect.Indirect(reflect.ValueOf(val))
	elemType := targetVal.Type().Elem()
	newVal := targetVal
	for C.mongoc_cursor_next(cursor, &doc) {
		var str = C.bson_get_data(doc)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
		//
		elemVal := reflect.New(elemType)
		elem := elemVal.Interface()
		err = bson.Unmarshal(mbys, elem)
		if err != nil {
			return
		}
		newVal = reflect.Append(newVal, reflect.Indirect(elemVal))
	}
	targetVal.Set(newVal)
	var berr C.bson_error_t
	if C.mongoc_cursor_error(cursor, &berr) {
		err = parseBSONError(&berr)
		return
	}
	return
}

//FindWithFlags the document by flags.
func (c *Collection) FindWithFlags(flags QueryFlags, query, fields interface{}, skip, limit, batchSize int, val interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawQuery, rawFields *C.bson_t
	defer func() {
		client.Close()
		if rawQuery != nil {
			C.bson_destroy(rawQuery)
		}
		if rawFields != nil {
			C.bson_destroy(rawFields)
		}
	}()
	if query != nil {
		rawQuery, err = parseBSON(query)
		if err != nil {
			return
		}
	}
	if fields != nil {
		rawFields, err = parseBSON(fields)
		if err != nil {
			return
		}
	}
	var cursor = C.mongoc_collection_find(col.raw, C.mongoc_query_flags_t(flags),
		C.uint32_t(skip), C.uint32_t(limit), C.uint32_t(batchSize), rawQuery, rawFields, nil)
	defer C.mongoc_cursor_destroy(cursor)
	err = c.parseCursor(cursor, val)
	return
}

//Find the document by flags.
func (c *Collection) Find(query, fields interface{}, skip, limit int, val interface{}) (err error) {
	return c.FindWithFlags(QueryNone, query, fields, skip, limit, 100, val)
}

//PipeWithFlags will pipe the document by flags.
func (c *Collection) PipeWithFlags(flags QueryFlags, pipeline, opts interface{}, val interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawPipeline, rawOpts *C.bson_t
	defer func() {
		client.Close()
		if rawPipeline != nil {
			C.bson_destroy(rawPipeline)
		}
		if rawOpts != nil {
			C.bson_destroy(rawOpts)
		}
	}()
	if pipeline != nil {
		rawPipeline, err = parseBSON(pipeline)
		if err != nil {
			return
		}
	}
	if opts != nil {
		rawOpts, err = parseBSON(opts)
		if err != nil {
			return
		}
	}
	var cursor = C.mongoc_collection_aggregate(col.raw, C.mongoc_query_flags_t(flags), rawPipeline, rawOpts, nil)
	defer C.mongoc_cursor_destroy(cursor)
	err = c.parseCursor(cursor, val)
	return
}

//Pipe the document by flags.
func (c *Collection) Pipe(pipeline interface{}, val interface{}) (err error) {
	return c.PipeWithFlags(QueryNone, pipeline, nil, val)
}

//CountWithFlags will return the row count by flags.
func (c *Collection) CountWithFlags(flags QueryFlags, query interface{}, skip, limit int) (count int, err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawQuery *C.bson_t
	defer func() {
		client.Close()
		if rawQuery != nil {
			C.bson_destroy(rawQuery)
		}
	}()
	if query == nil {
		query = map[string]interface{}{}
	}
	rawQuery, err = parseBSON(query)
	if err != nil {
		return
	}
	var berr C.bson_error_t
	count = int(C.mongoc_collection_count(col.raw,
		C.mongoc_query_flags_t(flags), rawQuery, C.int64_t(skip), C.int64_t(limit), nil, &berr))
	if count < 0 {
		err = parseBSONError(&berr)
	}
	return
}

//Count return the row coun.
func (c *Collection) Count(query interface{}, skip, limit int) (count int, err error) {
	return c.CountWithFlags(QueryNone, query, skip, limit)
}

//Drop collection
func (c *Collection) Drop() (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var berr C.bson_error_t
	if !C.mongoc_collection_drop(col.raw, &berr) {
		err = parseBSONError(&berr)
	}
	return
}
