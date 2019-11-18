package goseaweedfs

import (
	"encoding/json"
	"path/filepath"
	"strings"
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
	URL        string `json:"url"`
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
func NewFiler(url string, httpClient *httpClient) *Filer {
	if !strings.HasPrefix(url, "http:") && !strings.HasPrefix(url, "https:") {
		url = "http://" + url
	}
	return &Filer{
		URL:        url,
		httpClient: httpClient,
	}
}

// Dir list in directory
func (f *Filer) Dir(pathname string) (result *Dir, err error) {
	data, _, err := f.httpClient.getWithHeaders(filepath.Join(f.URL, pathname), map[string]string{"Accept": "application/json"})
	if err != nil {
		return nil, err
	}

	result = &Dir{}
	if err = json.Unmarshal(data, result); err != nil {
		return
	}

	return
}

// UploadFile a file
func (f *Filer) UploadFile(filePath, newFilerPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp, err := NewFilePart(filePath)
	if err != nil {
		return
	}
	fp.Collection = collection
	fp.TTL = ttl

	data, _, err := f.httpClient.upload(filepath.Join(f.URL, newFilerPath), filePath, fp.Reader, fp.IsGzipped, fp.MimeType)
	if err != nil {
		return
	}

	result = &FilerUploadResult{}
	if err = json.Unmarshal(data, result); err != nil {
		return
	}

	return
}

// Delete a file/dir
func (f *Filer) Delete(pathname string, recursive bool) (err error) {
	_, err = f.httpClient.delete(filepath.Join(f.URL, pathname), recursive)
	return
}
