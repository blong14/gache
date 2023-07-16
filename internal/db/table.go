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
	ScanWithLimit(s, e []byte, l int) ([][][]byte, bool)
	Range(func(k, v []byte) bool)
	Print()
	Connect() error
	Count() uint64
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

func (db *fileDatabase) Count() uint64 {
	return db.memtable.Count()
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
					_ = db.memtable.Flush(db.sstable)
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

func (db *fileDatabase) Print() {}

func (db *fileDatabase) Range(fnc func(k, v []byte) bool) {
	db.memtable.Range(fnc)
}

func (db *fileDatabase) Scan(s, e []byte) ([][][]byte, bool) {
	out := make([][][]byte, 0)
	db.memtable.Scan(s, e, func(k, v []byte) bool {
		out = append(out, [][]byte{k, v})
		return true
	})
	return out, true
}

func (db *fileDatabase) ScanWithLimit(s, e []byte, limit int) ([][][]byte, bool) {
	out := make([][][]byte, 0)
	db.memtable.Scan(s, e, func(k, v []byte) bool {
		out = append(out, [][]byte{k, v})
		if limit > 0 && len(out) >= limit {
			return false
		}
		return true
	})
	return out, true
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

func (db *inMemoryDatabase) Scan(s, e []byte) ([][][]byte, bool) {
	out := make([][][]byte, 0)
	db.memtable.Scan(s, e, func(k, v []byte) bool {
		out = append(out, [][]byte{k, v})
		return true
	})
	return out, true
}

func (db *inMemoryDatabase) ScanWithLimit(s, e []byte, limit int) ([][][]byte, bool) {
	out := make([][][]byte, 0)
	db.memtable.Scan(s, e, func(k, v []byte) bool {
		out = append(out, [][]byte{k, v})
		if limit > 0 && len(out) >= limit {
			return false
		}
		return true
	})
	return out, true
}

func (db *inMemoryDatabase) Range(fnc func(k, v []byte) bool) {
	db.memtable.Range(fnc)
}

func (db *inMemoryDatabase) Count() uint64  { return db.memtable.Count() }
func (db *inMemoryDatabase) Close()         {}
func (db *inMemoryDatabase) Print()         {}
func (db *inMemoryDatabase) Connect() error { return nil }
