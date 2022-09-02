package database

import (
	"strings"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
)

func TestParse(t *testing.T) {
	tests := map[string]*gactors.Query{
		"select value from default where key = __key__;": {
			Header: gactors.QueryHeader{
				Inst:      gactors.GetValue,
				TableName: []byte("default")},
			Key: []byte("__key__"),
		},
		"insert into default set key = _key, value = _value;": {
			Header: gactors.QueryHeader{
				Inst:      gactors.SetValue,
				TableName: []byte("default"),
			},
			Key:   []byte("_key"),
			Value: []byte("_value"),
		},
		"copy default from ./persons.csv;": {
			Header: gactors.QueryHeader{
				Inst:      gactors.Load,
				TableName: []byte("default"),
				FileName:  []byte("./persons.csv"),
			},
		},
		"create table default;": {
			Header: gactors.QueryHeader{
				Inst:      gactors.AddTable,
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
