package goseaweedfs

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	workerpool "github.com/linxGnu/gumble/worker-pool"
)

func createWorkerPool() *workerpool.Pool {
	return workerpool.NewPool(context.Background(), workerpool.Option{
		NumberWorker: runtime.NumCPU() << 1,
	})
}

func parseURI(uri string) (u *url.URL, err error) {
	u, err = url.Parse(uri)
	if err == nil && u.Scheme == "" {
		u.Scheme = "http"
	}
	return
}

func encodeURI(base url.URL, path string, args url.Values) string {
	base.Path += path
	query := base.Query()
	args = normalize(args, "", "")
	for k, vs := range args {
		for _, v := range vs {
			query.Add(k, v)
		}
	}
	base.RawQuery = query.Encode()
	return base.String()
}

func valid(c rune) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || '.' == c || '-' == c || '_' == c
}

func normalizeName(st string) string {
	for _, _c := range st {
		if !valid(_c) {
			var sb strings.Builder
			sb.Grow(len(st))

			for _, c := range st {
				if valid(c) {
					_, _ = sb.WriteRune(c)
				}
			}

			return sb.String()
		}
	}
	return st
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(ioutil.Discard, body)
	_ = body.Close()
}

func normalize(values url.Values, collection, ttl string) url.Values {
	if values == nil {
		values = make(url.Values)
	}

	if len(collection) > 0 {
		values.Set(ParamCollection, collection)
	}

	if len(ttl) > 0 {
		values.Set(ParamTTL, ttl)
	}

	return values
}

func readAll(r *http.Response) (body []byte, statusCode int, err error) {
	statusCode = r.StatusCode
	body, err = ioutil.ReadAll(r.Body)
	r.Body.Close()
	return
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func listFilesRecursive(dirPath string) (files []FileInfo, err error) {
	if err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if !isDir(path) {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			md5sum, err := getFileMd5sum(path)
			if err != nil {
				return err
			}
			path, err = filepath.Abs(path)
			if err != nil {
				return err
			}
			files = append(files, FileInfo{
				Name: f.Name(),
				Path: path,
				Md5:  md5sum,
			})
		}
		return nil
	}); err != nil {
		return files, err
	}
	return
}

func ListLocalFilesRecursive(dirPath string) (files []FileInfo, err error) {
	return listFilesRecursive(dirPath)
}

func getFileName(fullPath string) (fileName string) {
	// get file name
	arr := strings.Split(fullPath, "/")
	fileName = arr[len(arr)-1]
	return fileName
}

func getFileExtension(fileName string) (extension string) {
	// get file extension
	if strings.HasPrefix(fileName, ".") {
		fileName = fileName[1:(len(fileName) - 1)]
	}
	arr := strings.Split(fileName, ".")
	if len(arr) > 1 {
		extension = arr[len(arr)-1]
	}
	return extension
}

func getFileWithExtendedFields(file FilerFileInfo) (res FilerFileInfo) {
	// get isDir
	file.IsDir = file.Chunks == nil

	// get name
	file.Name = getFileName(file.FullPath)

	// get file extension
	if !file.IsDir {
		file.Extension = getFileExtension(file.Name)
	}

	return file
}

func getFileMd5sum(filePath string) (md5sum string, err error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return md5sum, err
	}
	return getBytesMd5sum(data)
}

func GetFileMd5sum(filePath string) (md5sum string, err error) {
	return getFileMd5sum(filePath)
}

func getBytesMd5sum(data []byte) (md5sum string, err error) {
	h := md5.New()
	content := strings.NewReader(string(data))
	_, err = content.WriteTo(h)
	if err != nil {
		return md5sum, err
	}
	md5sum = base64.StdEncoding.EncodeToString(h.Sum(nil))
	return md5sum, nil
}

func GetBytesMd5sum(data []byte) (md5sum string, err error) {
	return getBytesMd5sum(data)
}
