# goseaweedfs

[![Build Status](https://travis-ci.org/linxGnu/goseaweedfs.svg?branch=master)](https://travis-ci.org/linxGnu/goseaweedfs)
[![Go Report Card](https://goreportcard.com/badge/github.com/linxGnu/goseaweedfs)](https://goreportcard.com/report/github.com/linxGnu/goseaweedfs)
[![Coverage Status](https://coveralls.io/repos/github/linxGnu/goseaweedfs/badge.svg?branch=master)](https://coveralls.io/github/linxGnu/goseaweedfs?branch=master)
[![godoc](https://img.shields.io/badge/docs-GoDoc-green.svg)](https://godoc.org/github.com/linxGnu/goseaweedfs)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://github.com/linxGnu/goseaweedfs/blob/master/LICENSE)

A complete Golang client for [SeaweedFS](https://github.com/chrislusf/seaweedfs). Inspired by:
- [tnextday/goseaweed](https://github.com/tnextday/goseaweed)
- [ginuerzh/weedo](https://github.com/ginuerzh/weedo)

## This fork
Add buffer pool support when uploading in multipart-format, which will pre-allocate and re-use memory and reduce memory usage significantly if the size of uploading file can be estimated.

### How to use the pool
```bash
go get -u github.com/darkdarkfruit/goseaweedfs

# If the PR is accepted(Not yet), then do:
# go get -u github.com/linxGnu/goseaweedfs 
```
Just call
```go

// make a client
weed := goseaweedfs.NewSeaweed(c.MasterScheme, c.MasterAddr, nil, 4*1024*1024, time.Minute)

// then set the pool, eg: bufferLen: 0, bufferCap: 32M
weed.Client.Client.InitBufferPool(0, 32*1024*1024)

// ---
// or one call
weed := goseaweedfs.NewSeaweedWithBufferPoolSupport(c.MasterScheme, c.MasterAddr, nil, 4*1024*1024, time.Minute, 0, 32*1024*1024)
``` 

 

## Installation
```
go get -u github.com/linxGnu/goseaweedfs
```

## Usage
Please refer to [Test Cases](https://github.com/linxGnu/goseaweedfs/blob/master/seaweed_test.go) for sample code.

## Supported

- [x] Grow
- [x] Status
- [x] Cluster Status
- [x] Filer
- [x] Upload
- [x] Submit
- [x] Delete
- [x] Replace
- [x] Upload large file with builtin manifest handler, auto file split and chunking
- [ ] Admin Operations (mount, unmount, delete volumn, etc)

## Contributing
Please issue me for things gone wrong or:

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D