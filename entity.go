package goseaweedfs

import "time"

type FileInfo struct {
	// name of the file
	Name string `json:"name"`

	// absolute path of the file
	Path string `json:"path"`

	// MD5 hash
	Md5 string `json:"md5"`
}

type FilerListDirResponse struct {
	Path    string
	Entries []FilerFileInfo
}

type FilerFileInfo struct {
	FullPath        string
	Mtime           time.Time
	Crtime          time.Time
	Mode            int
	Uid             int
	Gid             int
	Mime            string
	Replication     string
	Collection      string
	TtlSec          int
	UserName        string
	GroupNames      string
	SymlinkTarget   string
	Md5             string
	FileSize        int64
	Extended        string
	HardLinkId      string
	HardLinkCounter int64
	Chunks          interface{}
	Children        []FilerFileInfo
	Name            string
	Extension       string
	IsDir           bool
}
