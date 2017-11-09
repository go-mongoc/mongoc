// Package mongoc is the go binding for libmongoc
//
// it was created as simple use api and the libmongoc is not used instead of golang high performance pool.
// all api is wrapped by basic, not all, but enough.
// if having some api is needed but not wrapped, send case to https://github.com/go-mongoc/mongoc/issues.
//
// the bulk is on plan.
package mongoc

/*
#include <mongoc.h>
#cgo darwin CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo darwin LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
#cgo linux CFLAGS: -I/usr/local/include/libmongoc-1.0/ -I/usr/local/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo linux LDFLAGS: -L/usr/local/lib -lmongoc-1.0 -lbson-1.0
#cgo windows CFLAGS: -IC:/mongo-c-driver/include/libmongoc-1.0/ -IC:/mongo-c-driver/include/libbson-1.0/ -Wno-deprecated-declarations
#cgo windows LDFLAGS: -LC:/mongo-c-driver/lib -lmongoc-1.0 -lbson-1.0
void mongoc_cgo_init();
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
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

/**** error ****/

//ErrServerSelectionFailure is wrapper of C.MONGOC_ERROR_SERVER_SELECTION_FAILURE
var ErrServerSelectionFailure = uint32(C.MONGOC_ERROR_SERVER_SELECTION_FAILURE)

//ErrCollectionNotExist is wrapper of C.MONGOC_ERROR_COLLECTION_DOES_NOT_EXIST
var ErrCollectionNotExist = uint32(C.MONGOC_ERROR_COLLECTION_DOES_NOT_EXIST)

//ErrDuplicateKey is wrapper of C.MONGOC_ERROR_DUPLICATE_KEY
var ErrDuplicateKey = uint32(C.MONGOC_ERROR_DUPLICATE_KEY)

//ErrCommandInvalidArg is wrapper of C.MONGOC_ERROR_COMMAND_INVALID_ARG
var ErrCommandInvalidArg = uint32(C.MONGOC_ERROR_COMMAND_INVALID_ARG)

//ErrNamespaceInvalid is wrapper of C.MONGOC_ERROR_NAMESPACE_INVALID
var ErrNamespaceInvalid = uint32(C.MONGOC_ERROR_NAMESPACE_INVALID)

//ErrNamespaceInvalidFilterType is wrapper of C.MONGOC_ERROR_NAMESPACE_INVALID_FILTER_TYPE
var ErrNamespaceInvalidFilterType = uint32(C.MONGOC_ERROR_NAMESPACE_INVALID_FILTER_TYPE)

//ErrNotFound is the defined error for document not found.
var ErrNotFound = fmt.Errorf("not found")

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
var LogHandler = DefaultLogHandler

//Log is the default Logger
var Log = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

//DefaultLogHandler default mongoc log handler. impl by log.Printf("[level] domain:message")
func DefaultLogHandler(logLevel LogLevel, logDomain, message string) {
	switch logLevel {
	case LogLevelError:
		Log.Output(3, fmt.Sprintf("E %v:%v", logDomain, message))
	case LogLevelCritical:
		Log.Output(3, fmt.Sprintf("C %v:%v", logDomain, message))
	case LogLevelWarning:
		Log.Output(3, fmt.Sprintf("W %v:%v", logDomain, message))
	case LogLevelMessage:
		Log.Output(3, fmt.Sprintf("M %v:%v", logDomain, message))
	case LogLevelInfo:
		Log.Output(3, fmt.Sprintf("I %v:%v", logDomain, message))
	case LogLevelDebug:
		Log.Output(3, fmt.Sprintf("D %v:%v", logDomain, message))
	case LogLevelTrace:
		Log.Output(3, fmt.Sprintf("T %v:%v", logDomain, message))
	default:
		Log.Output(3, fmt.Sprintf("U %v:%v", logDomain, message))
	}
}

func warnLog(format string, args ...interface{}) {
	LogHandler(LogLevelWarning, "MGO", fmt.Sprintf(format, args...))
}

func infoLog(format string, args ...interface{}) {
	LogHandler(LogLevelInfo, "MGO", fmt.Sprintf(format, args...))
}

func errorLog(format string, args ...interface{}) {
	LogHandler(LogLevelError, "MGO", fmt.Sprintf(format, args...))
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

// //IsServerSelectFail check the error if is server select fail.
// func (b *BSONError) IsServerSelectFail() bool {
// 	return b.Code == ErrServerSelectionFailure
// }

//IsCollectionNotExist check the error if is collection not exist
func (b *BSONError) IsCollectionNotExist() bool {
	return b.Code == ErrCollectionNotExist
}

//ErrorFilter is the interface for filter the error type.
type ErrorFilter interface {
	//IsNormalError check the error if it is normal error, meaning the connection is well
	IsNormalError(err error) bool
	//IsTempError check the error if it is temp error, meaning the connection is not available current.
	IsTempError(err error) bool
}

//DefaultErrorFilter is basic error filter。
type DefaultErrorFilter struct {
}

//IsNormalError check the error if it is normal error, meaning the connection is well
func (d *DefaultErrorFilter) IsNormalError(err error) bool {
	if err == nil {
		return true
	}
	berr, ok := err.(*BSONError)
	if !ok {
		return false
	}
	switch berr.Code {
	case ErrServerSelectionFailure:
		return false
	case ErrCollectionNotExist:
		return true
	case ErrDuplicateKey:
		return true
	case ErrCommandInvalidArg:
		return true
	case ErrNamespaceInvalid:
		return true
	case ErrNamespaceInvalidFilterType:
		return true
	default:
		return false
	}
}

//IsTempError check the error if it is temp error, meaning the connection is not available current.
func (d *DefaultErrorFilter) IsTempError(err error) bool {
	if err == nil {
		return true
	}
	berr, ok := err.(*BSONError)
	return ok && berr.Code == ErrServerSelectionFailure
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

//parse cursor to value.
func parseCursor(client *Client, cursor *C.mongoc_cursor_t, val interface{}) (err error) {
	var doc *C.bson_t
	targetVal := reflect.Indirect(reflect.ValueOf(val))
	if targetVal.Kind() == reflect.Slice { //for multi element.
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
	} else {
		elemType := targetVal.Type()
		var elemVal reflect.Value
		for C.mongoc_cursor_next(cursor, &doc) { //for one element.
			var str = C.bson_get_data(doc)
			mbys := C.GoBytes(unsafe.Pointer(str), C.int(doc.len))
			//
			elemVal = reflect.New(elemType)
			elem := elemVal.Interface()
			err = bson.Unmarshal(mbys, elem)
			if err != nil {
				return
			}
			break
		}
		if elemVal.IsValid() {
			targetVal.Set(reflect.Indirect(elemVal))
		} else {
			err = ErrNotFound
		}
	}
	var berr C.bson_error_t
	if C.mongoc_cursor_error(cursor, &berr) {
		err = parseBSONError(&berr)
		client.LastError = err
	}
	return
}

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
	Err     ErrorFilter
}

//NewPool will create the pool by size.
func NewPool(uri string, maxSize, minSize uint32) (pool *Pool) {
	if maxSize < 1 {
		panic("the pool must greater zero")
	}
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
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
		Err:     &DefaultErrorFilter{},
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
			if p.Err.IsTempError(err) {
				tempDelay *= 2
				if tempDelay > p.Timeout {
					panic("pool timeout")
				}
				warnLog("pool ping to server fail with %v, will retry after %v", err, tempDelay)
				time.Sleep(tempDelay)
				ping.LastError = nil
				p.ping <- ping //push back to ping pool for retry
			} else {
				warnLog("one client is closed by error:%v", err)
				ping.Release()
				p.max <- 1 //push back to max pool, will create new client.
			}
		case <-p.max:
			infoLog("pool is not full, will try create new client")
			client, err := newClient(p.URI)
			if err != nil {
				errorLog("panic: pool new clien fail with %v", err)
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
			warnLog("new client fail with ping error:%v, will retry after %v", err, tempDelay)
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
	//check error if normal error, if it is true, the connection is well.
	if p.Err.IsNormalError(client.LastError) {
		client.LastError = nil
		p.pool <- client
		return
	}
	//all other error is meaning the connection may be having error.
	warnLog("one client will push to ping pool with error:%v", client.LastError)
	client.LastError = nil
	p.ping <- client //push back to ping pool for retry
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

// //Command will query command on db.
// func (p *Pool) Command(dbname string, query, fields interface{}, skip, limit int, v interface{}) (err error) {
// 	return p.CommandWithFlags(dbname, QueryNone, query, fields, skip, limit, 100, v)
// }

// //CommandWithFlags will query command on db.
// func (p *Pool) CommandWithFlags(dbname string, flags QueryFlags, query, fields interface{}, skip, limit, batchSize int, v interface{}) (err error) {
// 	client := p.Pop()
// 	defer client.Close()
// 	return client.CommandWithFlags(dbname, flags, query, fields, skip, limit, batchSize, v)
// }

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

//CheckIndex will craete index on collection if it is not exists.
//if clear is true, will clear all index before create index.
func (p *Pool) CheckIndex(dbname string, indexes map[string][]*Index, clear bool) (err error) {
	return CheckIndex(
		func(name string) *Collection {
			return p.C(dbname, name)
		}, indexes, clear)
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
//if c.Pool is nil, call Relase to destory raw client,
//if c.Pool is not nil, push client to pool
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
	cdbname := C.CString(dbname)
	var rawCmds, rawOpts *C.bson_t
	defer func() {
		C.free(unsafe.Pointer(cdbname))
		if rawCmds != nil {
			C.bson_destroy(rawCmds)
		}
		if rawOpts != nil {
			C.bson_destroy(rawOpts)
		}
	}()
	rawCmds, err = parseBSON(cmds)
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
	var berr C.bson_error_t
	var reply C.bson_t
	if C.mongoc_client_read_write_command_with_opts(c.raw, cdbname, rawCmds, nil, rawOpts, &reply, &berr) {
		if reflect.Indirect(reflect.ValueOf(v)).Kind() == reflect.Slice {
			//reply will destory on mongoc_cursor_new_from_command_reply
			var cursor = C.mongoc_cursor_new_from_command_reply(c.raw, &reply, 0)
			err = parseCursor(c, cursor, v)
			C.mongoc_cursor_destroy(cursor)
		} else {
			var str = C.bson_get_data(&reply)
			mbys := C.GoBytes(unsafe.Pointer(str), C.int(reply.len))
			err = bson.Unmarshal(mbys, v)
			C.bson_destroy(&reply)
		}
	} else {
		err = parseBSONError(&berr)
		c.LastError = err
		C.bson_destroy(&reply)
	}
	return
}

// //Command will query command on db.
// func (c *Client) Command(dbname string, query, fields interface{}, skip, limit int, v interface{}) (err error) {
// 	return c.CommandWithFlags(dbname, QueryNone, query, fields, skip, limit, 100, v)
// }

// //CommandWithFlags will query command on db.
// func (c *Client) CommandWithFlags(dbname string, flags QueryFlags, query, fields interface{}, skip, limit, batchSize int, v interface{}) (err error) {
// 	var cdbname = C.CString(dbname)
// 	var rawQuery, rawFields *C.bson_t
// 	defer func() {
// 		C.free(unsafe.Pointer(cdbname))
// 		if rawQuery != nil {
// 			C.bson_destroy(rawQuery)
// 		}
// 		if rawFields != nil {
// 			C.bson_destroy(rawFields)
// 		}
// 	}()
// 	if query == nil {
// 		query = map[string]interface{}{}
// 	}
// 	rawQuery, err = parseBSON(query)
// 	if err != nil {
// 		return
// 	}
// 	if fields == nil {
// 		fields = map[string]interface{}{}
// 	}
// 	rawFields, err = parseBSON(fields)
// 	if err != nil {
// 		return
// 	}
// 	{ //execute cursor
// 		fmt.Println("--->")
// 		var cursor = C.mongoc_client_command(c.raw, cdbname, C.mongoc_query_flags_t(flags),
// 			C.uint32_t(skip), C.uint32_t(limit), C.uint32_t(batchSize), rawQuery, rawFields, nil)
// 		err = parseCursor(c, cursor, v)
// 		C.mongoc_cursor_destroy(cursor)
// 	}
// 	return
// }

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

//Upserted is the update all return.
type Upserted struct {
	Index int         `bson:"index"`
	ID    interface{} `bson:"_id"`
}

//WriteError is the command error.
type WriteError struct {
	Index   int    `bson:"index"`
	Code    int    `bson:"code"`
	Message string `bson:"errmsg"`
}

//WriteErrors is the WriteError slice.
type WriteErrors []*WriteError

func (w WriteErrors) Error() string {
	bys, _ := json.Marshal(w)
	return string(bys)
}

type updataReply struct {
	Changed `bson:",inline"`
	Errors  WriteErrors `bson:"writeErrors"`
}

//Update document to database by upsert or manay
func (c *Collection) Update(selector, update interface{}, upsert, many bool) (changed *Changed, err error) {
	var client = c.Pool.Pop()
	defer client.Close()
	if selector == nil {
		selector = map[string]interface{}{}
	}
	reply := &updataReply{}
	reply.Changed.Upserted = []*Upserted{}
	changed = &reply.Changed
	err = client.Execute(c.DbName, bson.D{
		{
			Name:  "update",
			Value: c.Name,
		},
		{
			Name: "updates",
			Value: []bson.M{
				{
					"q":      selector,
					"u":      update,
					"upsert": upsert,
					"multi":  many,
				},
			},
		},
	}, nil, reply)
	if err == nil && len(reply.Errors) > 0 {
		err = reply.Errors
	}
	return
}

//UpdateMany document to database
func (c *Collection) UpdateMany(selector, update interface{}) (chnaged *Changed, err error) {
	return c.Update(selector, update, false, true)
}

//UpdateOne document to database, return ErrNotFound when document not found
func (c *Collection) UpdateOne(selector, update interface{}) (err error) {
	var changed *Changed
	changed, err = c.Update(selector, update, false, false)
	if err == nil && changed.Matched < 1 {
		err = ErrNotFound
	}
	return
}

//Remove document to database by single
func (c *Collection) Remove(selector interface{}, single bool) (n int, err error) {
	var client = c.Pool.Pop()
	defer client.Close()
	if selector == nil {
		selector = map[string]interface{}{}
	}
	var delete = bson.M{
		"q": selector,
	}
	if single {
		delete["limit"] = 1
	} else {
		delete["limit"] = 0
	}
	var reply = bson.M{}
	err = client.Execute(c.DbName, bson.D{
		{
			Name:  "delete",
			Value: c.Name,
		},
		{
			Name: "deletes",
			Value: []bson.M{
				delete,
			},
		},
	}, nil, &reply)
	if err == nil {
		n = reply["n"].(int)
	}
	return
}

//RemoveAll document to database
func (c *Collection) RemoveAll(selector interface{}) (n int, err error) {
	return c.Remove(selector, false)
}

//Changed is the findAndModify reply info.
type Changed struct {
	Upserted interface{} `bson:"upserted"`  //the upsert id
	Matched  int         `bson:"n"`         //row matched
	Updated  int         `bson:"nModified"` //row updated.
}

type lastErrorObject struct {
	Upserted        interface{} `bson:"upserted"`
	UpdatedExisting bool        `bson:"updatedExisting"`
	N               int         `bson:"n"`
	Err             string      `bson:"err"`
	OK              int         `bson:"ok"`
}

type findAndModifyReply struct {
	Value interface{}     `bson:"value"`
	Ok    int             `bson:"ok"`
	Error lastErrorObject `bson:"lastErrorObject"`
}

//FindAndModifyWithFlags will find and modify document on database.
func (c *Collection) FindAndModifyWithFlags(query, update, fields interface{}, remove, upsert, retnew bool, v interface{}) (changed *Changed, err error) {
	var client = c.Pool.Pop()
	defer client.Close()
	if query == nil {
		query = map[string]interface{}{}
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	changed = &Changed{}
	var reply = findAndModifyReply{
		Value: v,
	}
	err = client.Execute(c.DbName, bson.D{
		{
			Name:  "findAndModify",
			Value: c.Name,
		},
		{
			Name:  "query",
			Value: query,
		},
		{
			Name:  "update",
			Value: update,
		},
		{
			Name:  "fields",
			Value: fields,
		},
		{
			Name:  "remove",
			Value: remove,
		},
		{
			Name:  "upsert",
			Value: upsert,
		},
		{
			Name:  "new",
			Value: retnew,
		},
	}, nil, &reply)
	if err == nil {
		if reply.Ok < 1 {
			err = fmt.Errorf("%v", reply.Error.Err)
			return
		}
		changed.Updated = reply.Error.N
		if reply.Error.UpdatedExisting {
			changed.Matched = 1
		}
		changed.Upserted = reply.Error.Upserted
	}
	return
}

//FindAndModify document on database.
func (c *Collection) FindAndModify(query, update, fields interface{}, upsert, retnew bool, v interface{}) (chnaged *Changed, err error) {
	return c.FindAndModifyWithFlags(query, update, fields, false, upsert, retnew, v)
}

//Upsert will update or insert document to database.
func (c *Collection) Upsert(query, update interface{}) (changed *Changed, err error) {
	return c.FindAndModifyWithFlags(query, update, nil, false, true, true, nil)
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
		err = parseCursor(client, cursor, val)
		C.mongoc_cursor_destroy(cursor)
	}
	return
}

//Find the document by flags.
func (c *Collection) Find(query, fields interface{}, skip, limit int, val interface{}) (err error) {
	return c.FindWithFlags(QueryNone, query, fields, skip, limit, 100, val)
}

//FindOne the document by flags.
func (c *Collection) FindOne(query, fields interface{}, val interface{}) (err error) {
	return c.FindWithFlags(QueryNone, query, fields, 0, 1, 100, val)
}

//FindID will find one document by id.
func (c *Collection) FindID(id string, fields interface{}, val interface{}) (err error) {
	return c.FindWithFlags(QueryNone, bson.M{"_id": id}, fields, 0, 1, 10, val)
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
		err = parseCursor(client, cursor, val)
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

type distinctReply struct {
	Values interface{} `bson:"values"`
	Ok     int         `bson:"ok"`
}

//Distinct will call the distinct command to database.
func (c *Collection) Distinct(key string, query, v interface{}) (err error) {
	var client = c.Pool.Pop()
	defer client.Close()
	if query == nil {
		query = map[string]interface{}{}
	}
	err = client.Execute(c.DbName,
		bson.D{
			{
				Name:  "distinct",
				Value: c.Name,
			},
			{
				Name:  "key",
				Value: key,
			},
			{
				Name:  "query",
				Value: query,
			},
		}, nil, &distinctReply{
			Values: v,
		})
	return
}

//Index is the struct to create the mongodb index.
//for more https://docs.mongodb.com/manual/reference/command/createIndexes/
type Index struct {
	Key                     []string       `bson:"-"`
	RawKey                  bson.D         `bson:"key"`
	Name                    string         `bson:"name"`
	Background              bool           `bson:"background,omitempty"`
	Unique                  bool           `bson:"unique,omitempty"`
	PartialFilterExpression bson.M         `bson:"partialFilterExpression,omitempty"`
	Sparse                  bool           `bson:"sparse,omitempty"`
	ExpireAfterSeconds      int            `bson:"expireAfterSeconds,omitempty"`
	StorageEngine           bson.M         `bson:"storageEngine,omitempty"`
	Weights                 map[string]int `bson:"weights,omitempty"`
	DefaultLanguage         string         `bson:"default_language,omitempty"`
	LanguageOverride        string         `bson:"language_override,omitempty"`
	TextIndexVersion        int            `bson:"textIndexVersion,omitempty"`
	V2dsphereIndexVersion   int            `bson:"2dsphereIndexVersion,omitempty"`
	Bits                    int            `bson:"bits,omitempty"`
	Min                     int            `bson:"min,omitempty"`
	Max                     int            `bson:"max,omitempty"`
	BucketSize              float64        `bson:"bucketSize,omitempty"`
	Collation               bson.M         `bson:"collation,omitempty"`
	V                       int            `bson:"v,omitempty"`
	NS                      string         `bson:"ns,omitempty"`
}

type listIndexesReply struct {
	Cursor map[string][]*Index `bson:"cursor"`
}

//ListIndexes will return the collection index.
func (c *Collection) ListIndexes() (indexes []*Index, err error) {
	var client = c.Pool.Pop()
	reply := &listIndexesReply{}
	err = client.Execute(c.DbName,
		bson.M{
			"listIndexes": c.Name,
		}, nil, reply)
	if err == nil && reply.Cursor != nil {
		indexes = reply.Cursor["firstBatch"]
		for _, index := range indexes {
			index.Key = ParseDoc(index.RawKey)
		}
	}
	client.Close()
	return
}

//CreateIndexes will create indexes on collection.
func (c *Collection) CreateIndexes(indexes ...*Index) (err error) {
	for _, index := range indexes {
		index.RawKey = ParseSorted(index.Key...)
	}
	var client = c.Pool.Pop()
	err = client.Execute(c.DbName,
		bson.D{
			{
				Name:  "createIndexes",
				Value: c.Name,
			},
			{
				Name:  "indexes",
				Value: indexes,
			},
		}, nil, &bson.M{})
	client.Close()
	return
}

//DropIndexes will drop index from collection, if name is *, drop all.
func (c *Collection) DropIndexes(name string) (err error) {
	var client = c.Pool.Pop()
	err = client.Execute(c.DbName,
		bson.D{
			{
				Name:  "dropIndexes",
				Value: c.Name,
			},
			{
				Name:  "index",
				Value: name,
			},
		}, nil, &bson.M{})
	client.Close()
	return
}

//CheckIndex will craete index on collection if it is not exists.
//if clear is true, will clear all index before create index.
func (c *Collection) CheckIndex(clear bool, indexes ...*Index) (err error) {
	mapHaving := map[string]*Index{}
	if clear {
		infoLog("pool will clear all index on collection(%v.%v)", c.DbName, c.Name)
		err = c.DropIndexes("*")
		if err != nil {
			//the collection not exists error.
			if berr, ok := (err.(*BSONError)); !(ok && berr.IsCollectionNotExist()) {
				errorLog("pool drop all index on collection(%v.%v) fail with %v", c.DbName, c.Name, err)
				return
			}
		}
	} else {
		var having []*Index
		having, err = c.ListIndexes()
		if err != nil {
			//the collection not exists error.
			if berr, ok := (err.(*BSONError)); !(ok && berr.IsCollectionNotExist()) {
				errorLog("pool list all index on collection(%v.%v) fail with %v", c.DbName, c.Name, err)
				return
			}
		} else {
			for _, index := range having {
				mapHaving[index.Name] = index
			}
		}
	}
	newList := []*Index{}
	for _, index := range indexes {
		if _, ok := mapHaving[index.Name]; ok {
			continue
		}
		newList = append(newList, index)
	}
	if len(newList) < 1 {
		return
	}
	infoLog("pool will create %v index on collection(%v.%v)", len(newList), c.DbName, c.Name)
	err = c.CreateIndexes(newList...)
	if err != nil {
		errorLog("pool create index on collection(%v.%v) fail with %v", c.DbName, c.Name, err)
	}
	return
}

//NewBulk will create on bulk.
//ordered:If the operations must be performed in order.
func (c *Collection) NewBulk(ordered bool) *Bulk {
	return &Bulk{
		C:       c,
		Ordered: ordered,
	}
}

//BulkReply is bulk result.
type BulkReply struct {
	Opid     int
	Inserted int           `bson:"nInserted"`
	Modified int           `bson:"nModified"`
	Matched  int           `bson:"nMatched"`
	Removed  int           `bson:"nRemoved"`
	Upserted int           `bson:"nUpserted"`
	Errors   []interface{} `bson:"writeErrors"`
}

//Operator is one bluk operator.
type Operator struct {
	Type   string        //the operator type
	Values []interface{} //the operator arguments.
}

//Bulk is wrapper of C.mongoc_bulk_t,
//it provides an abstraction for submitting multiple write operations as a single batch.
type Bulk struct {
	Cmds    []*Operator
	C       *Collection
	Ordered bool
}

//Insert is wrapper of C.mongoc_bulk_operation_insert(),
//it will insert multi document to database by adding multi insert bulk operator.
func (b *Bulk) Insert(docs ...interface{}) {
	for _, doc := range docs {
		b.Cmds = append(b.Cmds, &Operator{
			Type:   "insert",
			Values: []interface{}{doc},
		})
	}
}

//Remove is wrapper of C.mongoc_bulk_operation_remove(),
//it will remove multi document from database by selector.
func (b *Bulk) Remove(selector interface{}) {
	b.Cmds = append(b.Cmds, &Operator{
		Type:   "remove",
		Values: []interface{}{selector},
	})
}

//RemoveOne is wrapper of C.mongoc_bulk_operation_remove_one(),
//it will remove one document from database by selector.
func (b *Bulk) RemoveOne(selector interface{}) {
	b.Cmds = append(b.Cmds, &Operator{
		Type:   "removeOne",
		Values: []interface{}{selector},
	})
}

//Replace is wrapper of C.mongoc_bulk_operation_replace(),
//it will replace document from database by selector and new document.
func (b *Bulk) Replace(selector, document interface{}, upsert bool) {
	b.Cmds = append(b.Cmds, &Operator{
		Type:   "replace",
		Values: []interface{}{selector, document, upsert},
	})
}

//Update is wrapper of C.mongoc_bulk_operation_update(),
//it will update multi document from database by selector and update options.
func (b *Bulk) Update(selector, document interface{}, upsert bool) {
	b.Cmds = append(b.Cmds, &Operator{
		Type:   "update",
		Values: []interface{}{selector, document, upsert},
	})
}

//UpdateOne is wrapper of C.mongoc_bulk_operation_update(),
//it will update one document from database by selector and update options.
func (b *Bulk) UpdateOne(selector, document interface{}, upsert bool) {
	b.Cmds = append(b.Cmds, &Operator{
		Type:   "updateOne",
		Values: []interface{}{selector, document, upsert},
	})
}

//Execute is wrapper of C.mongoc_bulk_operation_execute(),
//it will commit all execute to database.
func (b *Bulk) Execute() (reply *BulkReply, err error) {
	var client = b.C.Pool.Pop()
	var col = client.rawCollection(b.C.DbName, b.C.Name)
	var rawBluk = C.mongoc_collection_create_bulk_operation(col.raw, C.bool(b.Ordered), nil)
	defer func() {
		client.Close()
		C.mongoc_bulk_operation_destroy(rawBluk)
	}()
	for _, cmd := range b.Cmds {
		switch cmd.Type {
		case "insert":
			rawDoc, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			C.mongoc_bulk_operation_insert(rawBluk, rawDoc)
		case "remove":
			rawSelector, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			C.mongoc_bulk_operation_remove(rawBluk, rawSelector)
		case "removeOne":
			rawSelector, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			C.mongoc_bulk_operation_remove_one(rawBluk, rawSelector)
		case "replace":
			rawSelector, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			rawDoc, terr := parseBSON(cmd.Values[1])
			if terr != nil {
				err = terr
				return
			}
			upsert := (cmd.Values[2]).(bool)
			C.mongoc_bulk_operation_replace_one(rawBluk, rawSelector, rawDoc, C.bool(upsert))
		case "update":
			rawSelector, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			rawDoc, terr := parseBSON(cmd.Values[1])
			if terr != nil {
				err = terr
				return
			}
			upsert := (cmd.Values[2]).(bool)
			C.mongoc_bulk_operation_update(rawBluk, rawSelector, rawDoc, C.bool(upsert))
		case "updateOne":
			rawSelector, terr := parseBSON(cmd.Values[0])
			if terr != nil {
				err = terr
				return
			}
			rawDoc, terr := parseBSON(cmd.Values[1])
			if terr != nil {
				err = terr
				return
			}
			upsert := (cmd.Values[2]).(bool)
			C.mongoc_bulk_operation_update_one(rawBluk, rawSelector, rawDoc, C.bool(upsert))
		}
	}
	var breply C.bson_t
	var berr C.bson_error_t
	var opid = int(C.mongoc_bulk_operation_execute(rawBluk, &breply, &berr))
	if opid < 1 {
		err = parseBSONError(&berr)
		client.LastError = err
	} else {
		var str = C.bson_get_data(&breply)
		mbys := C.GoBytes(unsafe.Pointer(str), C.int(breply.len))
		reply = &BulkReply{}
		err = bson.Unmarshal(mbys, reply)
		reply.Opid = opid
	}
	C.bson_destroy(&breply)
	return
}
