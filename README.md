# goseaweedfs
A complete Golang client for [SeaweedFS](https://github.com/chrislusf/seaweedfs). Inspired by:
- [tnextday/goseaweed](https://github.com/tnextday/goseaweed)
- [ginuerzh/weedo](https://github.com/ginuerzh/weedo)

## Installation
```
go get -u github.com/linxGnu/goseaweedfs
```

## Usage
Please refer to [Test](https://github.com/linxGnu/goseaweedfs/tree/master/test) for sample code.

TODO: more sample codes and test cases added in next minor version

## Supported

- [x] Grow
- [x] Status
- [x] Cluster Status
- [x] Filer
- [x] Upload
- [x] Submit
- [x] Delete
- [x] Replace
- [x] Upload large file with builtin manifest handler
- [ ] Admin Operations (mount, unmount, delete volumn, etc)

## Contributing
Please issue me for things gone wrong or:

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D