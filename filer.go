package goseaweedfs

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

// File structure according to filer API at https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type File struct {
	FileID string `json:"fid"`
	Name   string `json:"name"`
}

// Dir directory of filer. According to https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type Dir struct {
	Directory      string  `json:"Directory"`
	Files          []*File `json:"Files"`
	Subdirectories []*File `json:"Subdirectories"`
}

// Filer client
type Filer struct {
	base   *url.URL
	client *httpClient
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
func NewFiler(u string, client *http.Client) (f *Filer, err error) {
	base, err := parseURI(u)
	if err != nil {
		return
	}

	f = &Filer{
		base:   base,
		client: newHTTPClient(client),
	}
	return
}

var dirHeader = map[string]string{
	"Accept": "application/json",
}

// Dir list in directory.
func (f *Filer) Dir(path string, args url.Values) (result *Dir, err error) {
	data, _, err := f.client.get(f.base, path, args, dirHeader)
	if err == nil {
		result = &Dir{}
		err = json.Unmarshal(data, result)
	}
	return
}

// Upload a file.
func (f *Filer) Upload(localFilePath, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp, err := NewFilePart(localFilePath)
	if err == nil {
		fp.Collection = collection
		fp.TTL = ttl

		var data []byte
		data, _, err = f.client.upload(f.fullpath(newPath), localFilePath, fp.Reader, fp.MimeType)
		if err == nil {
			result = &FilerUploadResult{}
			err = json.Unmarshal(data, result)
		}

		_ = fp.Close()
	}
	return
}

// Download a file.
func (f *Filer) Download(path string, callback func(io.Reader) error) (err error) {
	_, err = f.client.Download(f.fullpath(path), callback)
	return
}

// Delete a file/dir.
func (f *Filer) Delete(path string, recursive bool) (err error) {
	_, err = f.client.delete(f.fullpath(path), recursive)
	return
}

func (f *Filer) fullpath(path string) string {
	u := *f.base
	u.Path = path
	return u.String()
}
