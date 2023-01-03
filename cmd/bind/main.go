package main

import (
	"C"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	gache "github.com/blong14/gache/database"
)

func mustGetDB() *sql.DB {
	db, err := sql.Open("gache", gache.MEMORY)
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	return db
}

var (
	client gache.GacheClient
	db     *sql.DB
)

//export Init
func Init() {
	fmt.Println("init gache")
	db = mustGetDB()
	client = gache.New(nil, db)
}

//export Execute
func Execute(s *C.char) {
	start := time.Now()
	sql := C.GoString(s)
	var result *gache.QueryResponse
	err := db.QueryRowContext(
		context.TODO(), sql,
	).Scan(&result)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print("%\tkey\t\tvalue\n")
	if result.Success {
		fmt.Printf("1.\t%s\t\t%s\n", string(result.Key), result.Value)
	}
	fmt.Printf("[%s]", time.Since(start))
	fmt.Print("\n% ")
}

//export Get
func Get(data *C.char) uintptr {
	d := C.GoString(data)
	var jsn map[string]string
	_ = json.Unmarshal([]byte(d), &jsn)
	fmt.Println(jsn)
	ctx := context.TODO()
	value, _ := client.Get(ctx, []byte(jsn["table"]), []byte(jsn["key"]))
	p := unsafe.Pointer(&value)
	s := *(*[]byte)(p)
	return uintptr(unsafe.Pointer(&s[0]))
}

//export Set
func Set(data *C.char) {
	d := C.GoString(data)
	var jsn map[string]string
	_ = json.Unmarshal([]byte(d), &jsn)
	ctx := context.TODO()
	_ = client.Set(ctx, []byte(jsn["table"]), []byte(jsn["key"]), []byte(jsn["value"]))
}

//export Stop
func Stop() {
	if err := db.Close(); err != nil {
		panic(err)
	}
}

func main() {}
