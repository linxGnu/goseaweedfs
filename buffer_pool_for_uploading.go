package goseaweedfs

import "github.com/oxtoacart/bpool"

// !! For user who wants to use buffer pool when uploading, please explicitly call SetBufferPoolForUploading(poolSize, BufferSize) !!

var gBufferPool *bpool.SizedBufferPool

////BufferPoolOptionsForUploading : will be used when uploading with multipart which will pre-allocate and reuse memory, and reduce memory usage significantly if we can estimate the file size we are uploading.
//type BufferPoolOptionsForUploading struct {
//	PoolSize   int // how many buffers in the pool, eg: 16
//	BufferSize int // length of a buffer, eg: 32 * 1024 * 1024
//}
//
//func NewBufferPoolOptionsForUploading(poolSize int, bufferSize int) *BufferPoolOptionsForUploading {
//	return &BufferPoolOptionsForUploading{PoolSize: poolSize, BufferSize: bufferSize}
//}

//SetUploadOptions : will be used when uploading with multipart which will pre-allocate and reuse memory, and reduce memory usage significantly if we can estimate the size of uploading file.
func SetBufferPoolForUploading(poolSize, bufferSize int) *bpool.SizedBufferPool {
	gBufferPool = bpool.NewSizedBufferPool(poolSize, bufferSize)
	return gBufferPool
}

func GetBufferPoolForUploading() *bpool.SizedBufferPool {
	return gBufferPool
}
