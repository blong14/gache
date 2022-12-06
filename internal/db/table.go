package db

import (
	gfile "github.com/blong14/gache/internal/io/file"
	"os"
	"sync"

	gmtable "github.com/blong14/gache/internal/db/memtable"
	gstable "github.com/blong14/gache/internal/db/sstable"
)

type Table interface {
	Get(k []byte) ([]byte, bool)
	Set(k, v []byte) error
	Range(func(k, v []byte) bool)
	Print()
	Close()
}

type TableOpts struct {
	TableName []byte
	DataDir   []byte
	InMemory  bool
}

type fileDatabase struct {
	dir      string
	name     string
	memtable *gmtable.MemTable
	sstable  *gstable.SSTable
	handle   *os.File
}

// New creates a new Table
//
// opts *TableOpts allow for specific database
// configuration.
//
// Example:
// opts := &TableOpts{TableName: "foo", DataDir: "bar", InMemory: True}
// db := New(opts)
// defer db.Close()
// err := db.Set(k, v)
// if err != nil {
//    panic(err)
// }
// v, ok := db.Get(k)
func New(opts *TableOpts) Table {
	if opts.InMemory {
		return &inMemoryDatabase{
			name:     string(opts.TableName),
			memtable: gmtable.New(),
		}
	}
	db := &fileDatabase{
		dir:      string(opts.DataDir),
		name:     string(opts.TableName),
		memtable: gmtable.New(),
	}
	f, err := gfile.NewDatFile(string(opts.DataDir), string(opts.TableName))
	if err != nil {
		panic(err)
	}
	if err := db.Open(f); err != nil {
		panic(err)
	}
	return db
}

var once sync.Once

func (db *fileDatabase) Open(f *os.File) error {
	once.Do(func() {
		db.handle = f
		db.sstable = gstable.New(f)
	})
	return nil
}

func (db *fileDatabase) Get(k []byte) ([]byte, bool) {
	value, ok := db.memtable.Get(k)
	if ok {
		return value, true
	}
	return db.sstable.Get(k)
}

func (db *fileDatabase) Set(k, v []byte) error {
	if err := db.memtable.Set(k, v); err != nil {
		return err
	}
	err := db.memtable.Flush(db.sstable)
	if err != nil {
		return err
	}
	return nil
}

func (db *fileDatabase) Close() {
	if db.sstable != nil {
		db.sstable.Free()
	}
	db.handle = nil
}

func (db *fileDatabase) Print()                           {}
func (db *fileDatabase) Range(fnc func(k, v []byte) bool) {}

type inMemoryDatabase struct {
	name     string
	memtable *gmtable.MemTable
}

func (db *inMemoryDatabase) Get(k []byte) ([]byte, bool) {
	value, ok := db.memtable.Get(k)
	return value, ok
}

func (db *inMemoryDatabase) Set(k, v []byte) error {
	return db.memtable.Set(k, v)
}

func (db *inMemoryDatabase) Close()                           {}
func (db *inMemoryDatabase) Print()                           {}
func (db *inMemoryDatabase) Range(fnc func(k, v []byte) bool) {}
