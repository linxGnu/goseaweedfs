package libs

import (
	"testing"
)

func TestBufferPool_Put(t *testing.T) {
	type fields struct {
		BufferCap int
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "test new, init, get, put",
			fields: fields{
				BufferCap: 1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewBufferPool(tt.fields.BufferCap)
			buf := pool.Get()
			if cap(buf.Bytes()) != tt.fields.BufferCap {
				t.Errorf("cap not match")
				return
			}
			pool.Put(buf)
			t.Logf("Done")
		})
	}
}
