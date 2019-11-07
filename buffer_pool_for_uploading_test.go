package goseaweedfs

import (
	"testing"
)

func TestBufferPool_Put(t *testing.T) {
	type fields struct {
		BufferLen int
		BufferCap int
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "test new, init, get, put",
			fields: fields{
				BufferLen: 0,
				BufferCap: 1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := InitBufferPool(tt.fields.BufferLen, tt.fields.BufferCap)
			buf := pool.Get()
			if len(buf.Bytes()) != tt.fields.BufferLen {
				t.Errorf("len not match")
				return
			}
			if cap(buf.Bytes()) != tt.fields.BufferCap {
				t.Errorf("cap not match")
				return
			}
			pool.Put(buf)
			t.Logf("Done")
		})
	}
}
