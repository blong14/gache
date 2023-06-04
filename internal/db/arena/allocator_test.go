package arena

import (
	ga "arena"
	"context"
	"reflect"
	"testing"
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

func Test_worker_Context(t *testing.T) {
	type fields struct {
		ctx     context.Context
		cancel  context.CancelFunc
		mailbox chan *Message
	}
	tests := []struct {
		name   string
		fields fields
		want   context.Context
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				ctx:     tt.fields.ctx,
				cancel:  tt.fields.cancel,
				mailbox: tt.fields.mailbox,
			}
			if got := w.Context(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Context() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_worker_Send(t *testing.T) {
	type fields struct {
		ctx     context.Context
		cancel  context.CancelFunc
		mailbox chan *Message
	}
	type args struct {
		msg *Message
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   <-chan *Message
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				ctx:     tt.fields.ctx,
				cancel:  tt.fields.cancel,
				mailbox: tt.fields.mailbox,
			}
			if got := w.Send(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Send() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_worker_Start(t *testing.T) {
	type fields struct {
		ctx     context.Context
		cancel  context.CancelFunc
		mailbox chan *Message
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				ctx:     tt.fields.ctx,
				cancel:  tt.fields.cancel,
				mailbox: tt.fields.mailbox,
			}
			if err := w.Start(); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_worker_Stop(t *testing.T) {
	type fields struct {
		ctx     context.Context
		cancel  context.CancelFunc
		mailbox chan *Message
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				ctx:     tt.fields.ctx,
				cancel:  tt.fields.cancel,
				mailbox: tt.fields.mailbox,
			}
			if err := w.Stop(); (err != nil) != tt.wantErr {
				t.Errorf("Stop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewWorker(t *testing.T) {
	type args struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	ctx, cancel := context.WithCancel(context.Background())
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should create a worker",
			args: args{ctx, cancel},
			want: "ok",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWorker(tt.args.ctx, tt.args.cancel)
			defer func() {
				if err := w.Stop(); err != nil {
					t.Errorf("Stop() %s", err)
				}
			}()
			go func() {
				if err := w.Start(); err != nil {
					t.Errorf("Start %s", err)
				}
			}()
			msg := NewMessage()
			select {
			case <-tt.args.ctx.Done():
			case got := <-w.Send(msg):
				if got.Status != tt.want {
					t.Errorf("Status not ok: %s", got.Status)
				}
			}
		})
	}
}
