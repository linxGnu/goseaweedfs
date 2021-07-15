package goseaweedfs

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Filer client
type Filer struct {
	base    *url.URL
	client  *httpClient
	authKey string
}

// FilerUploadResult upload result of response from filer server. According to https://github.com/chrislusf/seaweedfs/wiki/Filer-Server-API.
type FilerUploadResult struct {
	Name    string `json:"name,omitempty"`
	FileURL string `json:"url,omitempty"`
	FileID  string `json:"fid,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewFiler new filer with filer server's url
func NewFiler(u string, client *http.Client, opts ...FilerOption) (f *Filer, err error) {
	return newFiler(u, client, opts...)
}

func newFiler(u string, client *http.Client, opts ...FilerOption) (f *Filer, err error) {
	// base url
	base, err := parseURI(u)
	if err != nil {
		return
	}

	// filer
	f = &Filer{
		base: base,
	}

	// apply options
	for _, opt := range opts {
		opt(f)
	}

	// client
	var clientOpts []HttpClientOption
	if f.authKey != "" {
		clientOpts = append(clientOpts, WithHttpClientAuthKey(f.authKey))
	}
	f.client = newHTTPClient(client, clientOpts...)

	return f, nil
}

// Close underlying daemons.
func (f *Filer) Close() (err error) {
	if f.client != nil {
		err = f.client.Close()
	}
	return
}

// UploadFile a file.
func (f *Filer) UploadFile(localFilePath, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp, err := NewFilePart(localFilePath)
	if err != nil {
		return result, err
	}
	defer fp.Close()
	var data []byte
	data, status, err := f.client.upload(encodeURI(*f.base, newPath, normalize(nil, collection, ttl)), localFilePath, fp.Reader, fp.MimeType)
	if err != nil {
		return result, err
	}
	var res FilerUploadResult
	if err = json.Unmarshal(data, &res); err != nil {
		if status == 404 {
			return nil, errors.New("404 not found")
		}
		return result, err
	}
	result = &res
	if status >= 400 {
		return result, errors.New(res.Error)
	}
	return result, nil
}

// UploadDir upload files from a directory.
func (f *Filer) UploadDir(localDirPath, newPath, collection, ttl string) (results []*FilerUploadResult, err error) {
	if strings.HasSuffix(localDirPath, "/") {
		localDirPath = localDirPath[:(len(localDirPath) - 1)]
	}
	if !strings.HasPrefix(newPath, "/") {
		newPath = "/" + newPath
	}
	files, err := listFilesRecursive(localDirPath)
	if err != nil {
		return results, err
	}
	for _, info := range files {
		newFilePath := newPath + strings.Replace(info.Path, localDirPath, "", -1)
		result, err := f.UploadFile(info.Path, newFilePath, collection, ttl)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return
}

// Upload content.
func (f *Filer) Upload(content io.Reader, fileSize int64, newPath, collection, ttl string) (result *FilerUploadResult, err error) {
	fp := NewFilePartFromReader(ioutil.NopCloser(content), newPath, fileSize)

	var data []byte
	data, _, err = f.client.upload(encodeURI(*f.base, newPath, normalize(nil, collection, ttl)), newPath, ioutil.NopCloser(content), "")
	if err == nil {
		result = &FilerUploadResult{}
		err = json.Unmarshal(data, result)
	}

	_ = fp.Close()

	return
}

// ListDir List a directory.
func (f *Filer) ListDir(path string) (files []FilerFileInfo, err error) {
	data, _, err := f.GetJson(path, nil)
	if err != nil {
		return files, err
	}
	if len(data) == 0 {
		return
	}
	var res FilerListDirResponse
	if err := json.Unmarshal(data, &res); err != nil {
		return files, err
	}
	for _, file := range res.Entries {
		file = getFileWithExtendedFields(file)
		files = append(files, file)
	}
	return
}

// ListDirRecursive List a directory recursively.
func (f *Filer) ListDirRecursive(path string) (files []FilerFileInfo, err error) {
	entries, err := f.ListDir(path)
	if err != nil {
		return files, err
	}
	for _, file := range entries {
		file = getFileWithExtendedFields(file)
		if file.IsDir {
			file.Children, err = f.ListDirRecursive(file.FullPath)
			if err != nil {
				return files, err
			}
		}
		files = append(files, file)
	}
	return
}

// Get response data from filer.
func (f *Filer) Get(path string, args url.Values, header map[string]string) (data []byte, statusCode int, err error) {
	data, statusCode, err = f.client.get(encodeURI(*f.base, path, args), header)
	return
}

// GetJson Get response data from filer.
func (f *Filer) GetJson(path string, args url.Values) (data []byte, statusCode int, err error) {
	header := map[string]string{
		"Accept": "application/json",
	}
	data, statusCode, err = f.client.get(encodeURI(*f.base, path, args), header)
	return
}

// Download a file.
func (f *Filer) Download(path string, args url.Values, callback func(io.Reader) error) (err error) {
	_, err = f.client.download(encodeURI(*f.base, path, args), callback)
	return
}

// Delete a file/dir.
func (f *Filer) Delete(path string, args url.Values) (err error) {
	_, err = f.client.delete(encodeURI(*f.base, path, args))
	return
}

// DeleteDir a dir.
func (f *Filer) DeleteDir(path string) (err error) {
	args := map[string][]string{"recursive": {"true"}}
	_, err = f.client.delete(encodeURI(*f.base, path, args))
	return
}

// DeleteFile a file.
func (f *Filer) DeleteFile(path string) (err error) {
	_, err = f.client.delete(encodeURI(*f.base, path, nil))
	return
}
