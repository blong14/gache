package database

import (
	"strings"
	"testing"

	gdb "github.com/blong14/gache/internal/db"
)

func TestParse(t *testing.T) {
	tests := map[string]*gdb.Query{
		"select value from default where key = __key__;": {
			Header: gdb.QueryHeader{
				Inst:      gdb.GetValue,
				TableName: []byte("default")},
			Key: []byte("__key__"),
		},
		"insert into default set key = _key, value = _value;": {
			Header: gdb.QueryHeader{
				Inst:      gdb.SetValue,
				TableName: []byte("default"),
			},
			Key:   []byte("_key"),
			Value: []byte("_value"),
		},
		"copy default from ./persons.csv;": {
			Header: gdb.QueryHeader{
				Inst:      gdb.Load,
				TableName: []byte("default"),
				FileName:  []byte("./persons.csv"),
			},
		},
		"create table default;": {
			Header: gdb.QueryHeader{
				Inst:      gdb.AddTable,
				TableName: []byte("default"),
			},
		},
	}
	for test, expected := range tests {
		t.Run(test, func(t *testing.T) {
			reader := strings.NewReader(test)
			query, err := parse(reader)
			if err != nil {
				t.Error(err)
			}
			if query.String() != expected.String() {
				t.Errorf("e %s g %s", expected, query)
			}
			t.Log(query.String())
		})
	}
}
