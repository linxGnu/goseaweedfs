// The following environment variables, if set, will be used:
//
//  * GOSWFS_MASTER_URL
//  * GOSWFS_MEDIUM_FILE
//  * GOSWFS_SMALL_FILE
//  * GOSWFS_FILER_URL
//
package goseaweedfs

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var sw *Seaweed

var MediumFile, SmallFile string

func init() {
	masterURL := os.Getenv("GOSWFS_MASTER_URL")
	if masterURL == "" {
		panic("Master URL is required")
	}

	// check master url
	var filer []string
	if _filer := os.Getenv("GOSWFS_FILER_URL"); _filer != "" {
		filer = []string{_filer}
	}

	sw, _ = NewSeaweed(masterURL, filer, 8096, &http.Client{Timeout: 5 * time.Minute})
	_ = sw.Close()

	sw, _ = NewSeaweed(masterURL, filer, 8096, &http.Client{Timeout: 5 * time.Minute})

	MediumFile = os.Getenv("GOSWFS_MEDIUM_FILE")
	SmallFile = os.Getenv("GOSWFS_SMALL_FILE")
}

func TestUploadLookupserverReplaceDeleteFile(t *testing.T) {
	for i := 0; i < 2; i++ {
		_, fp, err := sw.UploadFile(MediumFile, "", "")
		require.Nil(t, err)

		_, err = sw.LookupServerByFileID(fp.FileID, nil, true)
		require.Nil(t, err)

		// verify by downloading
		downloaded := verifyDownloadFile(t, fp.FileID)
		fh, err := os.Open(MediumFile)
		require.Nil(t, err)
		allContent, _ := ioutil.ReadAll(fh)
		require.Nil(t, fh.Close())
		require.EqualValues(t, downloaded, allContent)

		// try to looking up
		_, err = sw.LookupFileID(fp.FileID, nil, true)
		require.Nil(t, err)

		// try to replace with small file
		require.Nil(t, sw.ReplaceFile(fp.FileID, SmallFile, false))
		_, err = sw.LookupFileID(fp.FileID, nil, true)
		require.Nil(t, err)

		// verify by downloading
		downloaded = verifyDownloadFile(t, fp.FileID)
		fh, err = os.Open(SmallFile)
		require.Nil(t, err)
		allContent, _ = ioutil.ReadAll(fh)
		require.Nil(t, fh.Close())
		require.EqualValues(t, downloaded, allContent)

		// replace again but delete first
		require.Nil(t, sw.ReplaceFile(fp.FileID, SmallFile, true))
		_, err = sw.LookupFileID(fp.FileID, nil, true)
		require.Nil(t, err)

		// verify by downloading
		downloaded = verifyDownloadFile(t, fp.FileID)
		fh, err = os.Open(SmallFile)
		require.Nil(t, err)
		allContent, _ = ioutil.ReadAll(fh)
		require.Nil(t, fh.Close())
		require.EqualValues(t, downloaded, allContent)

		// delete file
		require.Nil(t, sw.DeleteFile(fp.FileID, nil))

		// uploading with file reader
		fh, err = os.Open(MediumFile)
		require.Nil(t, err)
		var size int64
		fi, fiErr := fh.Stat()
		require.Nil(t, fiErr)
		size = fi.Size()
		fp, err = sw.Upload(fh, "test.txt", size, "col", "")
		require.Nil(t, err)
		require.Nil(t, fh.Close())

		// Replace with small file reader
		fs, err := os.Open(SmallFile)
		require.Nil(t, err)
		fi, fiErr = fs.Stat()
		require.Nil(t, fiErr)
		size = fi.Size()
		require.Nil(t, sw.Replace(fp.FileID, fs, "ta.txt", size, "", "", false))
		require.Nil(t, sw.DeleteFile(fp.FileID, nil))
		fs.Close()
	}
}

func TestBatchUploadFiles(t *testing.T) {
	_, err := sw.BatchUploadFiles([]string{MediumFile, SmallFile}, "", "")
	require.Nil(t, err)
}

func TestLookup(t *testing.T) {
	_, err := sw.Lookup("1", nil)
	require.Nil(t, err)
}

func TestGrowAndGC(t *testing.T) {
	err := sw.GC(1024 * 1024)
	require.Nil(t, err)
}

func TestStatus(t *testing.T) {
	_, err := sw.Status()
	require.Nil(t, err)
}

func TestClusterStatus(t *testing.T) {
	_, err := sw.ClusterStatus()
	require.Nil(t, err)
}

func TestDownloadFile(t *testing.T) {
	result, err := sw.Submit(SmallFile, "", "")
	require.Nil(t, err)
	require.NotNil(t, result)

	// return fake error
	_, err = sw.Download(result.FileID, nil, func(r io.Reader) error {
		return fmt.Errorf("Fake error")
	})
	require.NotNil(t, err)

	// verifying
	verifyDownloadFile(t, result.FileID)
}

func verifyDownloadFile(t *testing.T, fid string) (data []byte) {
	_, err := sw.Download(fid, nil, func(r io.Reader) (err error) {
		data, err = ioutil.ReadAll(r)
		return
	})
	require.Nil(t, err)
	require.NotZero(t, len(data))
	return
}

func TestDeleteChunks(t *testing.T) {
	if MediumFile != "" {
		cm, _, err := sw.UploadFile(MediumFile, "", "")
		require.Nil(t, err)

		err = sw.DeleteChunks(cm, nil)
		require.Nil(t, err)
	}
}

func TestFiler(t *testing.T) {
	// test with prefix
	filer := sw.filers[0]

	_, err := filer.UploadFile(SmallFile, "/js/test.txt", "", "")
	require.Nil(t, err)

	// try to download
	var buf bytes.Buffer
	err = filer.Download("/js/test.txt", nil, func(r io.Reader) error {
		_, err := io.Copy(&buf, r)
		require.Nil(t, err)
		return nil
	})
	require.Nil(t, err)
	require.NotZero(t, buf.Len())

	// try to delete this file
	err = filer.Delete("/js/test.txt", nil)
	require.Nil(t, err)

	// test with non prefix
	_, err = filer.UploadFile(SmallFile, "js/test1.jsx", "", "")
	require.Nil(t, err)

	data, _, err := filer.Get("js", nil, nil)
	require.Nil(t, err)
	require.NotZero(t, len(data))

	// try to download
	err = filer.Download("js/test1.jsx", nil, func(r io.Reader) error {
		return fmt.Errorf("Fake error")
	})
	require.NotNil(t, err)

	// try to delete this file
	err = filer.Delete("js/test1.jsx", nil)
	require.Nil(t, err)
}

func TestUnzipAndLoading(t *testing.T) {
	cm1 := &ChunkManifest{
		Mime: "images_test",
		Name: "test.txt",
		Size: 12345,
		Chunks: []*ChunkInfo{
			{
				Fid:    "abc",
				Offset: 2,
				Size:   3,
			},
		},
	}
	mar, _ := json.Marshal(cm1)

	// gzip after json marshaling
	var b bytes.Buffer
	writer := gzip.NewWriter(&b)
	_, _ = writer.Write(mar)
	writer.Close()

	// try to load chunk manifest
	cm2, err := loadChunkManifest(b.Bytes(), true)
	require.Nil(t, err)

	require.Equal(t, cm1.Mime, cm2.Mime)
	require.Equal(t, cm1.Name, cm2.Name)
	require.Equal(t, cm1.Size, cm2.Size)

	require.Equal(t, 1, len(cm2.Chunks))
	require.Equal(t, cm1.Chunks[0].Fid, cm2.Chunks[0].Fid)
	require.Equal(t, cm1.Chunks[0].Offset, cm2.Chunks[0].Offset)
	require.Equal(t, cm1.Chunks[0].Size, cm2.Chunks[0].Size)
}

func unzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	return output, err
}

func loadChunkManifest(buffer []byte, isGzipped bool) (*ChunkManifest, error) {
	if isGzipped {
		var err error
		if buffer, err = unzipData(buffer); err != nil {
			return nil, err
		}
	}

	cm := ChunkManifest{}
	if e := json.Unmarshal(buffer, &cm); e != nil {
		return nil, e
	}

	sort.Slice(cm.Chunks, func(i, j int) bool {
		return cm.Chunks[i].Offset < cm.Chunks[j].Offset
	})

	return &cm, nil
}
