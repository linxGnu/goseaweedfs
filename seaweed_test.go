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
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var sw *Seaweed

var MediumFile, SmallFile string

func init() {
	// check master url
	if masterURL := os.Getenv("GOSWFS_MASTER_URL"); masterURL != "" {
		var filer []string
		if _filer := os.Getenv("GOSWFS_FILER_URL"); _filer != "" {
			filer = []string{_filer}
		}

		sw, _ = NewSeaweed(masterURL, filer, 2*1024*1024, &http.Client{Timeout: 5 * time.Minute})
	}

	MediumFile = os.Getenv("GOSWFS_MEDIUM_FILE")
	SmallFile = os.Getenv("GOSWFS_SMALL_FILE")

	time.Sleep(10 * time.Second)
}

func TestUploadLookupserverReplaceDeleteFile(t *testing.T) {
	for i := 0; i < 2; i++ {
		_, _, fID, err := sw.UploadFile(MediumFile, "", "")
		require.Nil(t, err)

		_, err = sw.LookupServerByFileID(fID, nil, true)
		require.Nil(t, err)

		//
		_, err = sw.LookupFileID(fID, nil, true)
		require.Nil(t, err)

		//
		err = sw.ReplaceFile(fID, SmallFile, false)
		require.Nil(t, err)

		//
		err = sw.ReplaceFile(fID, SmallFile, true)
		require.Nil(t, err)

		err = sw.DeleteFile(fID, nil)
		require.Nil(t, err)

		// test upload file
		fh, err := os.Open(MediumFile)
		require.Nil(t, err)
		var size int64
		fi, fiErr := fh.Stat()
		require.Nil(t, fiErr)
		size = fi.Size()
		_, fID, err = sw.Upload(fh, "test.txt", size, "col", "")
		require.Nil(t, err)
		require.Nil(t, fh.Close())

		// Replace with small file
		fs, err := os.Open(SmallFile)
		require.Nil(t, err)
		fi, fiErr = fs.Stat()
		require.Nil(t, fiErr)
		size = fi.Size()
		require.Nil(t, sw.Replace(fID, fs, "ta.txt", size, "", "", false))
		require.Nil(t, sw.DeleteFile(fID, nil))
		fs.Close()
	}
}

func TestBatchUploadFiles(t *testing.T) {
	if MediumFile != "" && SmallFile != "" {
		_, err := sw.BatchUploadFiles([]string{MediumFile, SmallFile}, "", "")
		require.Nil(t, err)
	} else if MediumFile != "" {
		_, err := sw.BatchUploadFiles([]string{MediumFile, MediumFile}, "", "")
		require.Nil(t, err)
	} else if SmallFile != "" {
		_, err := sw.BatchUploadFiles([]string{SmallFile, SmallFile}, "", "")
		require.Nil(t, err)
	}
}

func TestLookup(t *testing.T) {
	_, err := sw.Lookup("1", nil)
	require.Nil(t, err)

	_, err = sw.LookupNoCache("1", nil)
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

func TestSubmit(t *testing.T) {
	if SmallFile != "" {
		_, err := sw.Submit(SmallFile, "", "")
		require.Nil(t, err)
	}
}

func TestDownloadFile(t *testing.T) {
	if SmallFile != "" {
		_, err := sw.Download(SmallFile, nil, func(r io.Reader) error {
			return fmt.Errorf("Fake error")
		})
		require.NotNil(t, err)
	}
}

func TestDeleteChunks(t *testing.T) {
	if MediumFile != "" {
		cm, _, _, err := sw.UploadFile(MediumFile, "", "")
		require.Nil(t, err)

		err = sw.DeleteChunks(cm, nil)
		require.Nil(t, err)
	}
}

func TestFiler(t *testing.T) {
	// test with prefix
	filer := sw.filers[0]

	_, err := filer.Upload(SmallFile, "/js/test.txt", "", "")
	require.Nil(t, err)

	// try to download
	err = filer.Download("/js/test.txt", func(r io.Reader) error {
		return nil
	})
	require.Nil(t, err)

	// try to delete this file
	err = filer.Delete("/js/test.txt", false)
	require.Nil(t, err)

	// test with non prefix
	filer = sw.filers[0]
	_, err = filer.Upload(SmallFile, "jsx/test1.jsx", "", "")
	require.Nil(t, err)

	// try to download
	err = filer.Download("jsx/test1.jsx", func(r io.Reader) error {
		return fmt.Errorf("Fake error")
	})
	require.NotNil(t, err)

	// try to delete this file
	err = filer.Delete("jsx/test1.jsx", true)
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
	cm2, err := LoadChunkManifest(b.Bytes(), true)
	require.Nil(t, err)

	require.Equal(t, cm1.Mime, cm2.Mime)
	require.Equal(t, cm1.Name, cm2.Name)
	require.Equal(t, cm1.Size, cm2.Size)

	require.Equal(t, 1, len(cm2.Chunks))
	require.Equal(t, cm1.Chunks[0].Fid, cm2.Chunks[0].Fid)
	require.Equal(t, cm1.Chunks[0].Offset, cm2.Chunks[0].Offset)
	require.Equal(t, cm1.Chunks[0].Size, cm2.Chunks[0].Size)
}
