// Package mongoc is the go binding for libmongoc
//
// It was created as simple use api and the libmongoc is not used instead of golang high performance pool
// all api is wrapped by basic, not all, but enough。
// if having some api is needed but not wrapped, send case to https://github.com/go-mongoc/mongoc/issues
// the bulk is on plan.
package mongoc

/*
#include <mongoc.h>
#cgo darwin CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo darwin LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
#cgo linux CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo linux LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
void mongoc_cgo_init();
*/
import "C"
import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"gopkg.in/bson.v2"
)

func init() {
	C.mongoc_cgo_init()
}

/**** log level ****/

//LogLevel is the wrapper for C.mongoc_log_level_t
type LogLevel C.mongoc_log_level_t

//LogLevelError is the C.MONGOC_LOG_LEVEL_ERROR, log error level
var LogLevelError = LogLevel(C.MONGOC_LOG_LEVEL_ERROR)

//LogLevelCritical is the C.MONGOC_LOG_LEVEL_CRITICAL, log critical level
var LogLevelCritical = LogLevel(C.MONGOC_LOG_LEVEL_CRITICAL)

//LogLevelWarning is the C.MONGOC_LOG_LEVEL_WARNING, log warning level
var LogLevelWarning = LogLevel(C.MONGOC_LOG_LEVEL_WARNING)

//LogLevelMessage is the C.MONGOC_LOG_LEVEL_MESSAGE, log message level
var LogLevelMessage = LogLevel(C.MONGOC_LOG_LEVEL_MESSAGE)

//LogLevelInfo is the C.MONGOC_LOG_LEVEL_INFO, log info level
var LogLevelInfo = LogLevel(C.MONGOC_LOG_LEVEL_INFO)

//LogLevelDebug is the C.MONGOC_LOG_LEVEL_DEBUG, log debug level
var LogLevelDebug = LogLevel(C.MONGOC_LOG_LEVEL_DEBUG)

//LogLevelTrace is the C.MONGOC_LOG_LEVEL_TRACE, log trace level
var LogLevelTrace = LogLevel(C.MONGOC_LOG_LEVEL_TRACE)

/**** query flags ****/

//QueryFlags is the wrapper for C.mongoc_query_flags_t
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
type QueryFlags C.mongoc_query_flags_t

//QueryNone is the C.MONGOC_QUERY_NONE
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryNone = QueryFlags(C.MONGOC_QUERY_NONE)

//QueryTailableCursor is the C.MONGOC_QUERY_TAILABLE_CURSOR
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryTailableCursor = QueryFlags(C.MONGOC_QUERY_TAILABLE_CURSOR)

//QuerySlaveOk is the C.MONGOC_QUERY_SLAVE_OK
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QuerySlaveOk = QueryFlags(C.MONGOC_QUERY_SLAVE_OK)

//QueryOplogReplay is the C.MONGOC_QUERY_OPLOG_REPLAY
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryOplogReplay = QueryFlags(C.MONGOC_QUERY_OPLOG_REPLAY)

//QueryNoCursorTimeout is the C.MONGOC_QUERY_NO_CURSOR_TIMEOUT
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryNoCursorTimeout = QueryFlags(C.MONGOC_QUERY_NO_CURSOR_TIMEOUT)

//QueryAwaitData is the C.MONGOC_QUERY_AWAIT_DATA
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryAwaitData = QueryFlags(C.MONGOC_QUERY_AWAIT_DATA)

//QueryExhaust is the C.MONGOC_QUERY_EXHAUST
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryExhaust = QueryFlags(C.MONGOC_QUERY_EXHAUST)

//QueryPartial is the C.MONGOC_QUERY_PARTIAL
//for more: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
var QueryPartial = QueryFlags(C.MONGOC_QUERY_PARTIAL)

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

/**** log ****/

//export logHandler
func logHandler(logLevel C.mongoc_log_level_t, logDomain *C.char, message *C.char, userData *unsafe.Pointer) {
	LogHandler(LogLevel(logLevel), C.GoString(logDomain), C.GoString(message))
}

//LogHandler is customable callback func to handler the mongoc log.
//default is log.Printf("[level] domain:message")
var LogHandler = func(logLevel LogLevel, logDomain, message string) {
	switch logLevel {
	case LogLevelError:
		log.Printf("[E] %v:%v", logDomain, message)
	case LogLevelCritical:
		log.Printf("[C] %v:%v", logDomain, message)
	case LogLevelWarning:
		log.Printf("[W] %v:%v", logDomain, message)
	case LogLevelMessage:
		log.Printf("[M] %v:%v", logDomain, message)
	case LogLevelInfo:
		log.Printf("[I] %v:%v", logDomain, message)
	case LogLevelDebug:
		log.Printf("[D] %v:%v", logDomain, message)
	case LogLevelTrace:
		log.Printf("[T] %v:%v", logDomain, message)
	default:
		log.Printf("[U] %v:%v", logDomain, message)
	}
}

//LogTraceEnable will enable trace log
//for more http://mongoc.org/libmongoc/current/logging.html
func LogTraceEnable() {
	C.mongoc_log_trace_enable()
}

//LogTraceDisable will disable trace log
//for more http://mongoc.org/libmongoc/current/logging.html
func LogTraceDisable() {
	C.mongoc_log_trace_disable()
}

//BSONError is the wrapper of bson_error_t.
//for more http://mongoc.org/libmongoc/current/errors.html
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

//IsServerSelectFail check the error if is server select fail.
func (b *BSONError) IsServerSelectFail() bool {
	return b.Code == C.MONGOC_ERROR_SERVER_SELECTION_FAILURE
}

/**** bson ****/

//BSON is the wrapper of C.bson_t
// type BSON struct {
// 	raw *C.bson_t
// }

//NewBSON will create the BSON wrapper, following:
//
//	if v is []byte, parsing by C.bson_new_from_data
//
//	if v is other type, parsing value by bson.Marshal to []byte, then create bson by C.bson_new_from_data
// func NewBSON(v interface{}) (bson *BSON, err error) {
// 	bson = &BSON{}
// 	switch v.(type) {
// 	case []byte:
// 		bson.raw, err = newRawBSON(v.([]byte))
// 	default:
// 		bson.raw, err = marshalRawBSON(v)
// 	}
// 	return
// }

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
// func (b *BSON) Release() {
// 	if b.raw != nil {
// 		C.bson_destroy(b.raw)
// 	}
// }

/**** pool ****/

//Poolable is interface for pool
type Poolable interface {
	Pop() *Client
	Push(client *Client)
}

//Pool is the pool of client
type Pool struct {
	URI  string //the client uri.
	pool chan *Client
	ping chan *Client
	max  chan int
	//
	ErrVer  int
	maxSize uint32
	minSize uint32
	closed  bool
	//
	Timeout time.Duration
}

//NewPool will create the pool by size.
func NewPool(uri string, maxSize, minSize uint32) (pool *Pool) {
	if maxSize < 1 {
		panic("the pool must greater zero")
	}
	pool = &Pool{
		pool:    make(chan *Client, maxSize),
		ping:    make(chan *Client, maxSize),
		max:     make(chan int, maxSize),
		ErrVer:  2,
		maxSize: maxSize,
		minSize: minSize,
		URI:     uri,
		Timeout: 600 * time.Second,
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
	if p.closed {
		panic("pool is closed")
	}
	var tempDelay = 5 * time.Millisecond // how long to sleep on accept failure
	for {
		select {
		case found := <-p.pool:
			return found
		case ping := <-p.ping:
			err := ping.Ping("test")
			if err == nil {
				return ping
			}
			//check if error is server select fail.
			//if select fail push back to ping pool and wait for retry
			//if other fail close it and push back to max pool for create new one.
			if berr, ok := err.(*BSONError); ok && berr.IsServerSelectFail() {
				tempDelay *= 2
				if tempDelay > p.Timeout {
					panic("pool timeout")
				}
				log.Printf("waring: pool ping to server fail with %v, will retry after %v", err, tempDelay)
				time.Sleep(tempDelay)
				ping.LastError = nil
				p.ping <- ping //push back to ping pool for retry
			} else {
				log.Printf("warning: one client is closed by error:%v", err)
				ping.Release()
				p.max <- 1 //push back to max pool, will create new client.
			}
		case <-p.max:
			client, err := newClient(p.URI)
			if err != nil {
				log.Printf("panic: pool new clien fail with %v", err)
				panic(err)
			}
			client.Pool = p
			client.SetErrVer(p.ErrVer)
			err = client.Ping("test")
			if err == nil {
				return client
			}
			client.Release()
			tempDelay *= 2
			if tempDelay > p.Timeout {
				panic("pool timeout")
			}
			log.Printf("warning: new client fail with ping error:%v, will retry after %v", err, tempDelay)
			time.Sleep(tempDelay)
			p.max <- 1 //push back to max pool for retry
		}
	}
}

//Push will push one client to pool
func (p *Pool) Push(client *Client) {
	if p.closed {
		panic("pool is closed")
	}
	if client == nil {
		panic("the client is nil")
	}
	if client.LastError == nil { //client is used well
		p.pool <- client
		return
	}
	//check if last error is server select fail.
	//if select fail push back to ping pool and wait for retry
	//if other fail close it and push back to max pool for create new one.
	if berr, ok := client.LastError.(*BSONError); ok && berr.IsServerSelectFail() {
		log.Printf("warning: one client having server select fail, push to ping pool:%v", berr)
		client.LastError = nil
		p.ping <- client //push back to ping pool for retry
	} else {
		log.Printf("warning: one client is closed by error:%v", berr)
		client.Release()
		p.max <- 1 //push back to max pool for create new one
	}
}

//C will create collection by database name and collection name.
func (p *Pool) C(dbname, colname string) *Collection {
	if p.closed {
		panic("pool is closed")
	}
	return &Collection{
		Name:   colname,
		DbName: dbname,
		Pool:   p,
	}
}

//Execute one command.
func (p *Pool) Execute(dbname string, cmds, opts, v interface{}) (err error) {
	client := p.Pop()
	defer client.Close()
	return client.Execute(dbname, cmds, opts, v)
}

//Close the pool
func (p *Pool) Close() {
	p.closed = true
	having := true
	for having { //close all client
		select {
		case found := <-p.pool:
			found.Pool = nil
			found.Close()
		default:
			having = false
		}
	}
	close(p.pool)
	close(p.max)
}

//Ping to database.
func (p *Pool) Ping(dbname string) (err error) {
	reply := map[string]interface{}{}
	err = p.Execute(dbname, bson.M{
		"ping": 1,
	}, nil, &reply)
	return
}

/**** client ****/

//Client is the wrapper of C.mongoc_client_t.
//Warning: close needed after used.
type Client struct {
	URI       string
	Pool      Poolable
	raw       *C.mongoc_client_t
	cols      map[string]*rawCollection
	colLck    sync.RWMutex
	LastError error
}

//newClient will create client by C.mongoc_client_new.
func newClient(uri string) (client *Client, err error) {
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
	c.colLck.Lock()
	for _, col := range c.cols { //free all collection.
		col.Release()
	}
	c.cols = map[string]*rawCollection{}
	c.colLck.Unlock()
}

//create the raw collection by dbname/colname.
//it will try get one from the cache list, if found use cache, or create new one.
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
func (c *Client) Execute(dbname string, cmds, opts, v interface{}) (err error) {
	if c.raw == nil {
		panic("raw client is nil")
	}
	rawCmds, err := parseBSON(cmds)
	if err != nil {
		return
	}
	if opts == nil {
		opts = map[string]interface{}{}
	}
	rawOpts, err := parseBSON(opts)
	if err != nil {
		return
	}
	cdbname := C.CString(dbname)
	var berr C.bson_error_t
	var doc C.bson_t
	if C.mongoc_client_read_write_command_with_opts(c.raw, cdbname, rawCmds, nil, rawOpts, &doc, &berr) {
		var str = C.bson_get_data(&doc)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
		err = bson.Unmarshal(mbys, v)
	} else {
		err = parseBSONError(&berr)
		c.LastError = err
	}
	C.bson_destroy(&doc)
	C.free(unsafe.Pointer(cdbname))
	C.bson_destroy(rawOpts)
	C.bson_destroy(rawCmds)
	return
}

//Ping to database.
func (c *Client) Ping(dbname string) (err error) {
	reply := map[string]interface{}{}
	err = c.Execute(dbname, bson.M{
		"ping": 1,
	}, nil, &reply)
	return
}

//SetErrVer will set the error message version.
//for more http://mongoc.org/libmongoc/current/errors.html#errors-error-api-version
func (c *Client) SetErrVer(ver int) bool {
	if c.raw == nil {
		panic("raw client is nil")
	}
	return bool(C.mongoc_client_set_error_api(c.raw, C.int32_t(ver)))
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
	Pool   Poolable
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
		client.LastError = err
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
	if selector == nil {
		selector = map[string]interface{}{}
	}
	rawSelector, err = parseBSON(selector)
	if err != nil {
		return
	}
	rawUpdate, err = parseBSON(update)
	if err != nil {
		return
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
		client.LastError = err
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
		client.LastError = err
	}
	return
}

//FindAndModifyWithFlags will find and modify document on database.
func (c *Collection) FindAndModifyWithFlags(query, update, fields interface{}, remove, upsert, retnew bool, v interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawQuery, rawSort, rawUpdate, rawFields *C.bson_t
	defer func() {
		client.Close()
		if rawQuery != nil {
			C.bson_destroy(rawQuery)
		}
		// if rawSort != nil {
		// 	C.bson_destroy(rawSort)
		// }
		if rawUpdate != nil {
			C.bson_destroy(rawUpdate)
		}
		if rawFields != nil {
			C.bson_destroy(rawFields)
		}
	}()
	if query == nil {
		query = map[string]interface{}{}
	}
	rawQuery, err = parseBSON(query)
	if err != nil {
		return
	}
	// if sort != nil {
	// 	rawSort, err = parseBSON(sort)
	// 	if err != nil {
	// 		return
	// 	}
	// }
	rawUpdate, err = parseBSON(update)
	if err != nil {
		return
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	rawFields, err = parseBSON(fields)
	if err != nil {
		return
	}
	var berr C.bson_error_t
	var doc C.bson_t
	if C.mongoc_collection_find_and_modify(col.raw, rawQuery, rawSort, rawUpdate, rawFields,
		C.bool(remove), C.bool(upsert), C.bool(retnew), &doc, &berr) {
		var str = C.bson_get_data(&doc)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
		err = bson.Unmarshal(mbys, v)
	} else {
		err = parseBSONError(&berr)
		client.LastError = err
	}
	C.bson_destroy(&doc)
	return
}

//FindAndModify document on database.
func (c *Collection) FindAndModify(query, update, fields interface{}, upsert, retnew bool, v interface{}) (err error) {
	return c.FindAndModifyWithFlags(query, update, fields, false, upsert, retnew, v)
}

//parse cursor to value.
func (c *Collection) parseCursor(client *Client, cursor *C.mongoc_cursor_t, val interface{}) (err error) {
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
		client.LastError = err
	}
	return
}

//FindWithFlags the document by flags.
func (c *Collection) FindWithFlags(flags QueryFlags, query, fields interface{}, skip, limit, batchSize int, val interface{}) (err error) {
	var client = c.Pool.Pop() //apply client
	var col = client.rawCollection(c.DbName, c.Name)
	var rawQuery, rawFields *C.bson_t
	defer func() {
		client.Close() //push back clien to pool
		if rawQuery != nil {
			C.bson_destroy(rawQuery)
		}
		if rawFields != nil {
			C.bson_destroy(rawFields)
		}
	}()
	if query == nil {
		query = map[string]interface{}{}
	}
	rawQuery, err = parseBSON(query)
	if err != nil {
		return
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	rawFields, err = parseBSON(fields)
	if err != nil {
		return
	}
	{ //execute cursor
		var cursor = C.mongoc_collection_find(col.raw, C.mongoc_query_flags_t(flags),
			C.uint32_t(skip), C.uint32_t(limit), C.uint32_t(batchSize), rawQuery, rawFields, nil)
		err = c.parseCursor(client, cursor, val)
		C.mongoc_cursor_destroy(cursor)
	}
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
		client.Close() //push back clien to pool
		if rawPipeline != nil {
			C.bson_destroy(rawPipeline)
		}
		if rawOpts != nil {
			C.bson_destroy(rawOpts)
		}
	}()
	rawPipeline, err = parseBSON(pipeline)
	if err != nil {
		return
	}
	if opts == nil {
		opts = map[string]interface{}{}
	}
	rawOpts, err = parseBSON(opts)
	if err != nil {
		return
	}
	{ //execute cursor
		var cursor = C.mongoc_collection_aggregate(col.raw, C.mongoc_query_flags_t(flags), rawPipeline, rawOpts, nil)
		err = c.parseCursor(client, cursor, val)
		C.mongoc_cursor_destroy(cursor)
	}
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
		client.Close() //push back clien to pool
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
		client.LastError = err
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
		client.LastError = err
	}
	client.Close() //push back clien to pool
	return
}

// //ExecuteWithFlags will execute command by flags.
// func (c *Collection) ExecuteWithFlags(flags QueryFlags, command, fields interface{}, skip, limit, batchSize int, val interface{}) (err error) {
// 	var client = c.Pool.Pop()
// 	var col = client.rawCollection(c.DbName, c.Name)
// 	var rawCommand, rawFields *C.bson_t
// 	defer func() {
// 		client.Close()
// 		if rawCommand != nil {
// 			C.bson_destroy(rawCommand)
// 		}
// 		if rawFields != nil {
// 			C.bson_destroy(rawFields)
// 		}
// 	}()
// 	if command != nil {
// 		rawCommand, err = parseBSON(command)
// 		if err != nil {
// 			return
// 		}
// 	}
// 	if fields != nil {
// 		rawFields, err = parseBSON(fields)
// 		if err != nil {
// 			return
// 		}
// 	}
// 	var cursor = C.mongoc_collection_command(col.raw, C.mongoc_query_flags_t(flags),
// 		C.uint32_t(skip), C.uint32_t(limit), C.uint32_t(batchSize), rawCommand, rawFields, nil)
// 	err = c.parseCursor(cursor, val)
// 	C.mongoc_cursor_destroy(cursor)
// 	return
// }

// //Execute command
// func (c *Collection) Execute(command, fields interface{}, skip, limit int, val interface{}) (err error) {
// 	return c.ExecuteWithFlags(QueryNone, command, fields, skip, limit, 100, val)
// }

//Rename the collection.
func (c *Collection) Rename(dbName, newName string, dropTargeBeforeRename bool) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var berr C.bson_error_t
	cDbName := C.CString(dbName)
	cNewName := C.CString(newName)
	if !C.mongoc_collection_rename(col.raw, cDbName, cNewName, C.bool(dropTargeBeforeRename), &berr) {
		err = parseBSONError(&berr)
		client.LastError = err
	}
	C.free(unsafe.Pointer(cDbName))
	C.free(unsafe.Pointer(cNewName))
	client.Close() //push back clien to pool
	return
}

//Stats return the collection stats.
func (c *Collection) Stats(options, v interface{}) (err error) {
	var client = c.Pool.Pop()
	var col = client.rawCollection(c.DbName, c.Name)
	var rawOptions *C.bson_t
	defer func() {
		client.Close() //push back clien to pool
		if rawOptions != nil {
			C.bson_destroy(rawOptions)
		}
	}()
	if options == nil {
		options = map[string]interface{}{}
	}
	rawOptions, err = parseBSON(options)
	if err != nil {
		return
	}
	var berr C.bson_error_t
	var doc C.bson_t
	if C.mongoc_collection_stats(col.raw, rawOptions, &doc, &berr) {
		var str = C.bson_get_data(&doc)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
		err = bson.Unmarshal(mbys, v)
	} else {
		err = parseBSONError(&berr)
		client.LastError = err
	}
	C.bson_destroy(&doc)
	return
}