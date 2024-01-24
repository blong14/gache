package arena

import (
	"reflect"
	"testing"

	ga "arena"
)

func Test_arena_AllocateByteSlice(t *testing.T) {
	type fields struct {
		malloc Arena
	}
	type args struct {
		len_ int
		cap  int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name:   "should create a byte slice",
			fields: fields{malloc: NewArena()},
			args:   args{len_: 3, cap: 10},
			want:   []byte{'a', 'a', 'a'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fields.malloc.AllocateByteSlice(tt.args.len_, tt.args.cap)
			for i := 0; i < tt.args.len_; i++ {
				got[i] = byte('a')
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AllocateByteSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_arena_Free(t *testing.T) {
	type fields struct {
		malloc *ga.Arena
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &arena{
				malloc: tt.fields.malloc,
			}
			a.Free()
		})
	}
}
