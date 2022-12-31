package db

import (
	"log"
	"os"
	"sync"

	gmtable "github.com/blong14/gache/internal/db/memtable"
	gstable "github.com/blong14/gache/internal/db/sstable"
	gfile "github.com/blong14/gache/internal/io/file"
)

type Table interface {
	Get(k []byte) ([]byte, bool)
	Set(k, v []byte) error
	Range(func(k, v []byte) bool)
	Print()
	Connect() error
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
	onSet    chan struct{}
}

func New(opts *TableOpts) Table {
	if opts.InMemory {
		return &inMemoryDatabase{
			name:     string(opts.TableName),
			memtable: gmtable.New(),
		}
	}
	f, err := gfile.NewDatFile(string(opts.DataDir), string(opts.TableName))
	if err != nil {
		panic(err)
	}
	db := &fileDatabase{
		dir:      string(opts.DataDir),
		name:     string(opts.TableName),
		memtable: gmtable.New(),
		handle:   f,
		onSet:    make(chan struct{}),
	}
	if err := db.Connect(); err != nil {
		panic(err)
	}
	return db
}

var once sync.Once

func (db *fileDatabase) Connect() error {
	once.Do(func() {
		db.sstable = gstable.New(db.handle)
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
	return nil
}

func (db *fileDatabase) Close() {
	err := db.memtable.Flush(db.sstable)
	if err != nil {
		log.Println(err)
	}
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
func (db *inMemoryDatabase) Connect() error                   { return nil }
