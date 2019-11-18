package goseaweedfs

import (
	"encoding/json"
	"net/url"
)

// File structure according to filer API at https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type File struct {
	FileID string `json:"fid"`
	Name   string `json:"name"`
}

// Dir directory of filer. According to https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type Dir struct {
	Path    string `json:"Directory"`
	Files   []*File
	Subdirs []*File `json:"Subdirectories"`
}

// Filer client
type Filer struct {
	base       *url.URL
	httpClient *httpClient
}

// FilerUploadResult upload result which responsed from filer server. According to https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type FilerUploadResult struct {
	Name    string `json:"name,omitempty"`
	FileURL string `json:"url,omitempty"`
	FileID  string `json:"fid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewFiler new filer with filer server's url
func NewFiler(u string, httpClient *httpClient) (f *Filer, err error) {
	base, err := url.Parse(u)
	if err != nil {
		return
	}
	f = &Filer{
		base:       base,
		httpClient: httpClient,
	}
	return
}

// Dir list in directory
func (f *Filer) Dir(path string) (result *Dir, err error) {
	data, _, err := f.httpClient.getWithHeaders(f.fullpath(path), map[string]string{
		"Accept": "application/json",
	})
	if err != nil {
		return nil, err
	}

	result = &Dir{}
	if err = json.Unmarshal(data, result); err != nil {
		return
	}

	return
}

// Upload a file.
func (f *Filer) Upload(filePath, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp, err := NewFilePart(filePath)
	if err == nil {
		fp.Collection = collection
		fp.TTL = ttl

		var data []byte
		data, _, err = f.httpClient.upload(f.fullpath(newPath), filePath, fp.Reader, fp.IsGzipped, fp.MimeType)
		if err == nil {
			result = &FilerUploadResult{}
			err = json.Unmarshal(data, result)
		}

		_ = fp.Close()
	}
	return
}

// Delete a file/dir.
func (f *Filer) Delete(path string, recursive bool) (err error) {
	_, err = f.httpClient.delete(f.fullpath(path), recursive)
	return
}

func (f *Filer) fullpath(path string) string {
	u := *f.base
	u.Path = path
	return u.String()
}
