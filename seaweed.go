package goseaweedfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	workerpool "github.com/linxGnu/gumble/worker-pool"
)

var (
	// ErrFileNotFound return file not found error
	ErrFileNotFound = fmt.Errorf("File not found")
)

const (
	// ParamCollection http param to specify collection which files belong. According to SeaweedFS API.
	ParamCollection = "collection"

	// ParamTTL http param to specify time to live. According to SeaweedFS API.
	ParamTTL = "ttl"

	// ParamCount http param to specify how many file ids to reserve. According to SeaweedFS API.
	ParamCount = "count"

	// ParamAssignReplication http param to assign files with a specific replication type.
	ParamAssignReplication = "replication"

	// ParamAssignCount http param to specify how many file ids to reserve.
	ParamAssignCount = "count"

	// ParamAssignDataCenter http param to assign a specific data center
	ParamAssignDataCenter = "dataCenter"

	// ParamLookupVolumeID http param to specify volume ID for looking up.
	ParamLookupVolumeID = "volumeId"

	// ParamLookupPretty http param to make json response prettified or not. Default should not be set.
	ParamLookupPretty = "pretty"

	// ParamLookupCollection http param to specify known collection, this would make file look up/search faster.
	ParamLookupCollection = "collection"

	// ParamVacuumGarbageThreshold if your system has many deletions, the deleted file's disk space will not be synchronously re-claimed.
	// There is a background job to check volume disk usage. If empty space is more than the threshold,
	// default to 0.3, the vacuum job will make the volume readonly, create a new volume with only existing files,
	// and switch on the new volume. If you are impatient or doing some testing, vacuum the unused spaces this way.
	ParamVacuumGarbageThreshold = "GarbageThreshold"

	// ParamGrowReplication http param to specify a specific replication.
	ParamGrowReplication = "replication"

	// ParamGrowCount http param to specify number of empty volume to grow.
	ParamGrowCount = "count"

	// ParamGrowDataCenter http param to specify datacenter of growing volume.
	ParamGrowDataCenter = "dataCenter"

	// ParamGrowCollection http param to specify collection of files for growing.
	ParamGrowCollection = "collection"

	// ParamGrowTTL specify time to live for growing api. Refers to: https://github.com/chrislusf/seaweedfs/wiki/Store-file-with-a-Time-To-Live
	// 3m: 3 minutes
	// 4h: 4 hours
	// 5d: 5 days
	// 6w: 6 weeks
	// 7M: 7 months
	// 8y: 8 years
	ParamGrowTTL = "ttl"

	// admin operations
	// ParamAssignVolumeReplication = "replication"
	// ParamAssignVolume            = "volume"
	// ParamDeleteVolume            = "volume"
	// ParamMountVolume             = "volume"
	// ParamUnmountVolume           = "volume"
)

// Seaweed client containing almost features/operations to interact with SeaweedFS
type Seaweed struct {
	master    *url.URL
	filers    []*Filer
	chunkSize int64
	client    *httpClient
	workers   *workerpool.Pool
}

// NewSeaweed create new seaweed client. Master url must be a valid uri (which includes scheme).
func NewSeaweed(masterURL string, filers []string, chunkSize int64, client *http.Client) (res *Seaweed, err error) {
	u, err := parseURI(masterURL)
	if err != nil {
		return
	}

	workers := createWorkerPool()

	res = &Seaweed{
		master:    u,
		client:    newHTTPClient(client, workers),
		chunkSize: chunkSize,
		workers:   workers,
	}

	if len(filers) > 0 {
		res.filers = make([]*Filer, 0, len(filers))
		for i := range filers {
			var filer *Filer
			filer, err = newFiler(filers[i], res.client)
			if err != nil {
				return
			}
			res.filers = append(res.filers, filer)
		}
	}

	// start underlying workers
	res.workers.Start()

	return
}

// Close underlying daemons.
func (c *Seaweed) Close() (err error) {
	c.workers.Stop()
	return
}

// Filers returns initialized filer(s).
func (c *Seaweed) Filers() []*Filer {
	return c.filers
}

// Grow pre-Allocate Volumes.
func (c *Seaweed) Grow(count int, collection, replication, dataCenter string) error {
	args := make(url.Values)
	if count > 0 {
		args.Set(ParamGrowCount, strconv.Itoa(count))
	}
	if collection != "" {
		args.Set(ParamGrowCollection, collection)
	}
	if replication != "" {
		args.Set(ParamGrowReplication, replication)
	}
	if dataCenter != "" {
		args.Set(ParamGrowDataCenter, dataCenter)
	}
	return c.GrowArgs(args)
}

// GrowArgs pre-Allocate volumes with args.
func (c *Seaweed) GrowArgs(args url.Values) (err error) {
	_, _, err = c.client.get(c.master, "/vol/grow", args, nil)
	return
}

// Lookup volume ID.
func (c *Seaweed) Lookup(volID string, args url.Values) (result *LookupResult, err error) {
	result, err = c.doLookup(volID, args)
	return
}

func (c *Seaweed) doLookup(volID string, args url.Values) (result *LookupResult, err error) {
	if args == nil {
		args = make(url.Values)
	}
	args.Set(ParamLookupVolumeID, volID)

	jsonBlob, _, err := c.client.get(c.master, "/dir/lookup", args, nil)
	if err == nil {
		result = &LookupResult{}
		if err = json.Unmarshal(jsonBlob, result); err == nil {
			if result.Error != "" {
				err = errors.New(result.Error)
			}
		}
	}

	return
}

// LookupServerByFileID lookup server by file id.
func (c *Seaweed) LookupServerByFileID(fileID string, args url.Values, readonly bool) (server string, err error) {
	var parts []string
	if strings.Contains(fileID, ",") {
		parts = strings.Split(fileID, ",")
	} else {
		parts = strings.Split(fileID, "/")
	}

	if len(parts) != 2 { // wrong file id format
		return "", errors.New("Invalid fileID " + fileID)
	}

	lookup, lookupError := c.Lookup(parts[0], args)
	if lookupError != nil {
		err = lookupError
	} else if len(lookup.VolumeLocations) == 0 {
		err = ErrFileNotFound
	}

	if err == nil {
		if readonly {
			server = lookup.VolumeLocations.RandomPickForRead().URL
		} else {
			server = lookup.VolumeLocations.Head().URL
		}
	}

	return
}

// LookupFileID lookup file by id.
func (c *Seaweed) LookupFileID(fileID string, args url.Values, readonly bool) (fullURL string, err error) {
	u, err := c.LookupServerByFileID(fileID, args, readonly)
	if err == nil {
		base := *c.master
		base.Host = u
		base.Path = fileID
		fullURL = base.String()
	}
	return
}

// GC force Garbage Collection.
func (c *Seaweed) GC(threshold float64) (err error) {
	args := url.Values{
		"garbageThreshold": []string{strconv.FormatFloat(threshold, 'f', -1, 64)},
	}
	_, _, err = c.client.get(c.master, "/vol/vacuum", args, nil)
	return
}

// Status check System Status.
func (c *Seaweed) Status() (result *SystemStatus, err error) {
	data, _, err := c.client.get(c.master, "/dir/status", nil, nil)
	if err == nil {
		result = &SystemStatus{}
		err = json.Unmarshal(data, result)
	}
	return
}

// ClusterStatus get cluster status.
func (c *Seaweed) ClusterStatus() (result *ClusterStatus, err error) {
	data, _, err := c.client.get(c.master, "/cluster/status", nil, nil)
	if err == nil {
		result = &ClusterStatus{}
		err = json.Unmarshal(data, result)
	}
	return
}

// Assign do assign api.
func (c *Seaweed) Assign() (result *AssignResult, err error) {
	jsonBlob, _, err := c.client.get(c.master, "/dir/assign", nil, nil)
	if err == nil {
		result = &AssignResult{}
		if err = json.Unmarshal(jsonBlob, result); err != nil {
			err = fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
		} else if result.Count == 0 {
			err = errors.New(result.Error)
		}
	}

	return
}

// Submit file directly to master.
func (c *Seaweed) Submit(filePath string, collection, ttl string) (result *SubmitResult, err error) {
	fp, err := NewFilePart(filePath)
	if err == nil {
		fp.Collection = collection
		fp.TTL = ttl
		result, err = c.SubmitFilePart(fp, nil)
		_ = fp.Close()
	}
	return
}

// SubmitFilePart directly to master.
func (c *Seaweed) SubmitFilePart(f *FilePart, args url.Values) (result *SubmitResult, err error) {
	data, _, err := c.client.upload(encodeURI(*c.master, "/submit", args), f.FileName, f.Reader, f.MimeType)
	if err == nil {
		result = &SubmitResult{}
		err = json.Unmarshal(data, result)
	}
	return
}

// Upload file by reader.
func (c *Seaweed) Upload(fileReader io.Reader, fileName string, size int64, collection, ttl string) (fp *FilePart, fileID string, err error) {
	fp = NewFilePartFromReader(ioutil.NopCloser(fileReader), fileName, size)
	fp.Collection, fp.TTL = collection, ttl
	_, fileID, err = c.UploadFilePart(fp)
	return
}

// UploadFile with full file dir/path.
func (c *Seaweed) UploadFile(filePath string, collection, ttl string) (cm *ChunkManifest, fp *FilePart, fileID string, err error) {
	fp, err = NewFilePart(filePath)
	if err == nil {
		fp.Collection, fp.TTL = collection, ttl
		cm, fileID, err = c.UploadFilePart(fp)
		_ = fp.Close()
	}
	return
}

// UploadFilePart uploads a file part.
func (c *Seaweed) UploadFilePart(f *FilePart) (cm *ChunkManifest, fileID string, err error) {
	if f.FileID == "" {
		res, err := c.Assign()
		if err != nil {
			return nil, "", err
		}
		f.Server, f.FileID = res.URL, res.FileID
	}

	if f.Server == "" {
		if f.Server, err = c.LookupServerByFileID(f.FileID, url.Values{ParamCollection: []string{f.Collection}}, false); err != nil {
			return
		}
	}

	baseName := path.Base(f.FileName)
	if c.chunkSize > 0 && f.FileSize > c.chunkSize {
		chunks := f.FileSize/c.chunkSize + 1

		cm = &ChunkManifest{
			Name:   baseName,
			Size:   f.FileSize,
			Mime:   f.MimeType,
			Chunks: make([]*ChunkInfo, chunks),
		}
		args := url.Values{ParamCollection: []string{f.Collection}}
		args.Set("Content-Type", "multipart/form-data")

		for i := int64(0); i < chunks; i++ {
			_, id, count, e := c.uploadChunk(f, baseName+"_"+strconv.FormatInt(i+1, 10))
			if e != nil { // delete all uploaded chunks
				_ = c.DeleteChunks(cm, args)
				return nil, "", e
			}

			cm.Chunks[i] = &ChunkInfo{
				Offset: i * c.chunkSize,
				Size:   int64(count),
				Fid:    id,
			}
		}

		if err = c.uploadManifest(f, cm); err != nil { // delete all uploaded chunks
			_ = c.DeleteChunks(cm, args)
		}
	} else {
		args := make(url.Values)
		if f.ModTime != 0 {
			args.Set("ts", strconv.FormatInt(f.ModTime, 10))
		}
		args.Set("Content-Type", "multipart/form-data")

		_, _, err = c.client.upload(encodeURI(*c.master, f.FileID, args), baseName, f.Reader, f.MimeType)
	}

	if err == nil {
		fileID = f.FileID
	}

	return
}

// BatchUploadFiles batch uploads files.
func (c *Seaweed) BatchUploadFiles(files []string, collection, ttl string) (results []*SubmitResult, err error) {
	fps, err := NewFileParts(files)
	if err == nil {
		results, err = c.BatchUploadFileParts(fps, collection, ttl)
		closeFileParts(fps)
	}
	return
}

// BatchUploadFileParts uploads multiple file parts at once.
func (c *Seaweed) BatchUploadFileParts(files []*FilePart, collection string, ttl string) ([]*SubmitResult, error) {
	results := make([]*SubmitResult, len(files))
	for index, file := range files {
		results[index] = &SubmitResult{
			FileName: file.FileName,
		}
	}

	assigned, err := c.Assign()
	if err != nil {
		for i := range files {
			results[i].Error = err.Error()
		}
		return results, err
	}

	tasks := make([]*workerpool.Task, 0, len(files))
	for i, file := range files {
		file.FileID = assigned.FileID
		if i > 0 {
			file.FileID = file.FileID + "_" + strconv.Itoa(i)
		}
		file.Server = assigned.URL
		file.Collection = collection

		task := c.uploadTask(file, assigned, results[i])
		c.workers.Do(task)
		tasks = append(tasks, task)
	}

	for i := range tasks {
		r := <-tasks[i].Result()
		if r.Err != nil {
			results[i].Error = r.Err.Error()
		}
	}

	return results, nil
}

func (c *Seaweed) uploadTask(file *FilePart, assigned *AssignResult, result *SubmitResult) *workerpool.Task {
	return workerpool.NewTask(context.Background(), func(ctx context.Context) (interface{}, error) {
		result.Size = file.FileSize
		result.FileID = file.FileID
		result.FileURL = assigned.PublicURL + "/" + file.FileID
		_, _, err := c.UploadFilePart(file)
		return nil, err
	})
}

// Replace file content with new one.
func (c *Seaweed) Replace(fileID string, newContent io.Reader, fileName string, size int64, collection, ttl string, deleteFirst bool) (err error) {
	fp := NewFilePartFromReader(ioutil.NopCloser(newContent), fileName, size)
	fp.Collection, fp.TTL = collection, ttl
	fp.FileID = fileID
	_, err = c.ReplaceFilePart(fp, deleteFirst)
	return
}

// ReplaceFile replaces file with local file.
func (c *Seaweed) ReplaceFile(fileID, localFilePath string, deleteFirst bool) (err error) {
	fp, err := NewFilePart(localFilePath)
	if err == nil {
		fp.FileID = fileID
		_, err = c.ReplaceFilePart(fp, deleteFirst)
		_ = fp.Close()
	}
	return
}

// ReplaceFilePart replaces file part.
func (c *Seaweed) ReplaceFilePart(f *FilePart, deleteFirst bool) (fileID string, err error) {
	if deleteFirst && f.FileID != "" {
		if err = c.DeleteFile(f.FileID, url.Values{ParamCollection: []string{f.Collection}}); err == nil {
			_, fileID, err = c.UploadFilePart(f)
		}
	}
	return
}

func (c *Seaweed) uploadChunk(f *FilePart, filename string) (assignResult *AssignResult, fileID string, size int64, err error) {
	// Assign first to get file id and url for uploading
	assignResult, err = c.Assign()

	if err == nil {
		fileID = assignResult.FileID

		base := *c.master
		base.Host = assignResult.URL

		// do upload
		var v []byte
		v, _, err = c.client.upload(
			encodeURI(base, assignResult.FileID, nil),
			filename, io.LimitReader(f.Reader, c.chunkSize),
			"application/octet-stream")
		if err == nil {
			// parsing response data
			uploadResult := UploadResult{}
			if err = json.Unmarshal(v, &uploadResult); err == nil {
				size = uploadResult.Size
			}
		}
	}

	return
}

func (c *Seaweed) uploadManifest(f *FilePart, manifest *ChunkManifest) (err error) {
	buf, err := manifest.Marshal()
	if err == nil {
		bufReader := bytes.NewReader(buf)

		args := make(url.Values)
		if f.ModTime != 0 {
			args.Set("ts", strconv.FormatInt(f.ModTime, 10))
		}
		args.Set("cm", "true")

		_, _, err = c.client.upload(encodeURI(*c.master, f.FileID, args), manifest.Name, bufReader, "application/json")
	}
	return
}

// Download file by id.
func (c *Seaweed) Download(fileID string, args url.Values, callback func(io.Reader) error) (fileName string, err error) {
	fileURL, err := c.LookupFileID(fileID, args, true)
	if err == nil {
		fileName, err = c.client.Download(fileURL, callback)
	}
	return
}

// DeleteChunks concurrently delete chunks.
func (c *Seaweed) DeleteChunks(cm *ChunkManifest, args url.Values) (err error) {
	if cm == nil || len(cm.Chunks) == 0 {
		return nil
	}

	tasks := make([]*workerpool.Task, 0, len(cm.Chunks))
	for _, ci := range cm.Chunks {
		task := c.deleteFileTask(ci.Fid, args)
		c.workers.Do(task)
		tasks = append(tasks, task)
	}

	for i := range tasks {
		if r := <-tasks[i].Result(); r.Err != nil {
			err = r.Err
			return
		}
	}

	return
}

func (c *Seaweed) deleteFileTask(fileID string, args url.Values) *workerpool.Task {
	return workerpool.NewTask(context.Background(), func(ctx context.Context) (interface{}, error) {
		return nil, c.DeleteFile(fileID, args)
	})
}

// DeleteFile by id.
func (c *Seaweed) DeleteFile(fileID string, args url.Values) (err error) {
	fileURL, err := c.LookupFileID(fileID, args, false)
	if err == nil {
		_, err = c.client.delete(fileURL, false)
	}
	return
}
