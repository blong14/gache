package client

import (
	"bytes"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []string{
		"select value from default where key = __key__;",
		"insert into default set key = __key__, value = __value__;",
	}
	for _, test := range tests {
		reader := strings.NewReader(test)
		query, err := parse(reader)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(query.Key, []byte("__key__")) {
			t.Error("did not find key")
		}
		t.Log(query.String())
	}
}
