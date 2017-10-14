package mongoc

import "runtime"

//SharedPool is global shared pool for SharedC
var SharedPool *Pool

//SharedDbName is global shared database name for SharedC
var SharedDbName string

//SharedC will get the collection from SharedPool by SharedDbName and name.
func SharedC(name string) *Collection {
	return SharedPool.C(SharedDbName, name)
}

//InitShared will create the SharedPool and set SharedDbName.
func InitShared(uri, dbname string) {
	SharedPool = NewPool(uri, uint32(runtime.NumCPU()), 1)
	SharedDbName = dbname
}

//SharedCheckIndex will craete index on collection if it is not exists by shared pool.
//if clear is true, will clear all index before create index.
func SharedCheckIndex(indexes map[string][]*Index, clear bool) (err error) {
	return CheckIndex(SharedC, indexes, clear)
}

//SharedExecute will call the excute by SharedPool/SharedDbName.
func SharedExecute(cmds, opts, v interface{}) error {
	return SharedPool.Execute(SharedDbName, cmds, opts, v)
}

//CheckIndex will craete index on collection if it is not exists.
//if clear is true, will clear all index before create index.
func CheckIndex(C func(name string) *Collection, indexes map[string][]*Index, clear bool) (err error) {
	for colname, colIndexes := range indexes {
		var col = C(colname)
		err = col.CheckIndex(clear, colIndexes...)
		if err != nil {
			return
		}
	}
	return
}
