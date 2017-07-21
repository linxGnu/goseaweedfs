package model

import (
	"encoding/json"
	"strings"

	"github.com/linxGnu/goseaweedfs/libs"
)

// File ...
type File struct {
	FileID string `json:"fid"`
	Name   string `json:"name"`
}

// Dir ...
type Dir struct {
	Path    string `json:"Directory"`
	Files   []*File
	Subdirs []*File `json:"Subdirectories"`
}

// Filer ...
type Filer struct {
	URL        string `json:"url"`
	HTTPClient *libs.HTTPClient
}

// FilerUploadResult ...
type FilerUploadResult struct {
	Name    string `json:"name,omitempty"`
	FileURL string `json:"url,omitempty"`
	FileID  string `json:"fid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewFiler ...
func NewFiler(url string, httpClient *libs.HTTPClient) *Filer {
	if !strings.HasPrefix(url, "http:") && !strings.HasPrefix(url, "https:") {
		url = "http://" + url
	}

	return &Filer{
		URL:        url,
		HTTPClient: httpClient,
	}
}

// Dir list in directory
func (f *Filer) Dir(pathname string) (result *Dir, err error) {
	if !strings.HasPrefix(pathname, "/") {
		pathname = "/" + pathname
	}
	if !strings.HasSuffix(pathname, "/") {
		pathname = pathname + "/"
	}

	data, _, err := f.HTTPClient.GetWithURL(f.URL + pathname)
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
func (f *Filer) UploadFile(filePath string, collection, ttl string) (result *FilerUploadResult, err error) {
	fp, err := NewFilePart(filePath)
	if err != nil {
		return
	}
	fp.Collection = collection
	fp.Ttl = ttl

	if !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}

	data, _, err := f.HTTPClient.Upload(f.URL+filePath, fp.FileName, fp.Reader, fp.IsGzipped, fp.MimeType)
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
func (f *Filer) Delete(pathname string) (err error) {
	if !strings.HasPrefix(pathname, "/") {
		pathname = "/" + pathname
	}

	_, err = f.HTTPClient.Delete(f.URL + pathname)
	return
}
