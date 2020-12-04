package goseaweedfs

import "time"

type FileInfo struct {
	// name of the file
	Name string

	// absolute path of the file
	Path string
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
	FileSize        int
	Extended        string
	HardLinkId      string
	HardLinkCounter int
}