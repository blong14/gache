package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	gmtable "github.com/blong14/gache/internal/db/memtable"
	gstable "github.com/blong14/gache/internal/db/sstable"
	gwal "github.com/blong14/gache/internal/db/wal"
	gfile "github.com/blong14/gache/internal/io/file"
)

type Table interface {
	Get(k []byte) ([]byte, bool)
	Set(k, v []byte) error
	Scan(s, e []byte) ([][][]byte, bool)
	Range(func(k, v []byte) bool)
	Print()
	Connect() error
	Close()
}

type TableOpts struct {
	TableName []byte
	DataDir   []byte
	InMemory  bool
	WalMode   bool
}

type fileDatabase struct {
	dir      string
	name     string
	memtable *gmtable.MemTable
	sstable  *gstable.SSTable
	handle   *os.File
	wal      *gwal.WAL
	useWal   bool
	onSet    chan struct{}
}

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
		useWal:   opts.WalMode,
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
		f, err := gfile.NewDatFile(db.dir, db.name)
		if err != nil {
			panic(err)
		}
		db.handle = f
		db.sstable = gstable.New(db.handle)

		file := fmt.Sprintf("%s-wal.dat", db.name)
		p := path.Join(db.dir, file)
		f, err = os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		db.wal = gwal.New(f)
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
	if db.useWal {
		if err := db.wal.Set(k, v); err != nil {
			return err
		}
	}
	if err := db.memtable.Set(k, v); err != nil {
		if errors.Is(err, gmtable.ErrAllowedBytesExceeded) {
			go func() {
				if db.sstable != nil {
					err := db.memtable.Flush(db.sstable)
					if err != nil {
						log.Println(err)
					}
				}
			}()
		} else {
			return err
		}
	}
	return nil
}

func (db *fileDatabase) Close() {
	if db.sstable != nil {
		err := db.memtable.Flush(db.sstable)
		if err != nil {
			log.Println(err)
		}
		db.sstable.Free()
	}
}

func (db *fileDatabase) Print()                           {}
func (db *fileDatabase) Range(fnc func(k, v []byte) bool) {}
func (db *fileDatabase) Scan(_, _ []byte) ([][][]byte, bool) {
	return nil, false
}

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

func (db *inMemoryDatabase) Scan(start, end []byte) ([][][]byte, bool) {
	value, ok := db.memtable.Scan(start, end)
	return value, ok
}

func (db *inMemoryDatabase) Close()                           {}
func (db *inMemoryDatabase) Print()                           {}
func (db *inMemoryDatabase) Range(fnc func(k, v []byte) bool) {}
func (db *inMemoryDatabase) Connect() error                   { return nil }
