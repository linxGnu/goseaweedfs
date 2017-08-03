package model

import (
	"encoding/json"
	"strings"

	"github.com/linxGnu/goseaweedfs/libs"
)

type File struct {
	FileID string `json:"fid"`
	Name   string `json:"name"`
}

type Dir struct {
	Path    string `json:"Directory"`
	Files   []*File
	Subdirs []*File `json:"Subdirectories"`
}

type Filer struct {
	URL        string `json:"url"`
	HTTPClient *libs.HTTPClient
}

type FilerUploadResult struct {
	Name    string `json:"name,omitempty"`
	FileURL string `json:"url,omitempty"`
	FileID  string `json:"fid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewFiler new filer with filer server's url
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

	data, _, err := f.HTTPClient.GetWithHeaders(f.URL+pathname, map[string]string{"Accept": "application/json"})
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
	fp.Ttl = ttl

	if !strings.HasPrefix(newFilerPath, "/") {
		newFilerPath = "/" + newFilerPath
	}

	data, _, err := f.HTTPClient.Upload(f.URL+newFilerPath, filePath, fp.Reader, fp.IsGzipped, fp.MimeType)
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
