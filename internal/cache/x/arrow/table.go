package arrow

import (
	"bytes"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"sync"
)

type Table struct {
	sync.RWMutex
	schema  *arrow.Schema
	builder *array.RecordBuilder
	rows    []arrow.Record
}

func New() *Table {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "key", Type: arrow.BinaryTypes.Binary},
			{Name: "value", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Reserve(1)
	return &Table{schema: schema, builder: builder, rows: make([]arrow.Record, 0)}
}

func (t *Table) Get(k []byte) ([]byte, bool) {
	t.Lock()
	defer t.Unlock()
	a, _ := array.NewRecordReader(t.schema, t.rows)
	defer a.Release()
	for a.Next() {
		arr := a.Record()
		key := arr.Columns()[0].(*array.Binary)
		if bytes.Equal(k, key.ValueBytes()) {
			value := arr.Columns()[1].(*array.Binary)
			return value.ValueBytes(), true
		}
	}
	return nil, false
}

func (t *Table) Set(k, v []byte) error {
	t.Lock()
	defer t.Unlock()
	kb := t.builder.Field(0).(*array.BinaryBuilder)
	kb.Append(k)
	ib := t.builder.Field(1).(*array.BinaryBuilder)
	ib.Append(v)
	t.rows = append(t.rows, t.builder.NewRecord())
	return nil
}
