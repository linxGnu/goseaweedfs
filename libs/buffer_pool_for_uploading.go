package libs

import (
	"bytes"
	"sync"
)

//var gBufferPool *BufferPool
//
//// !! For user who wants to use buffer pool when uploading, please explicitly call InitBufferPool(bufferLen, bufferCap int) !!
//func InitBufferPool(bufferLen, bufferCap int) *BufferPool {
//	gBufferPool = NewBufferPool(bufferLen, bufferCap)
//	buf := gBufferPool.Get() // pre-cache
//	gBufferPool.Put(buf)     // pre-cache
//	return gBufferPool
//}
//
//func GetBufferPool() *BufferPool {
//	return gBufferPool
//}

// BufferPool : will be used when uploading with multipart which will pre-allocate and reuse memory, and reduce memory usage significantly if we can estimate the file size we are uploading.
type BufferPool struct {
	BufferLen int
	BufferCap int
	*sync.Pool
}

func NewBufferPool(bufferLen int, bufferCap int) *BufferPool {
	pool := &BufferPool{BufferLen: bufferLen, BufferCap: bufferCap}
	pool.Pool = &sync.Pool{New: func() interface{} {
		return bytes.NewBuffer(make([]byte, pool.BufferLen, pool.BufferCap))
	}}
	return pool
}

func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}

func (bp *BufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	bp.Pool.Put(buf)
}

//
