package goseaweedfs

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func setup() (f *Filer, err error) {
	filerUrl := "http://localhost:8888"
	httpClient := http.Client{Timeout: 5 * time.Minute}
	f, err = NewFiler(filerUrl, &httpClient)
	if err != nil {
		panic(err)
	}
	return
}

func TestFiler_UploadDir(t *testing.T) {
	f, _ := setup()
	localDirPath, _ := filepath.Abs(".")
	newDirPath := "test_dir"
	collection := "test_collection"

	// test UploadDir
	_, err := f.UploadDir(localDirPath, newDirPath, collection, "")
	require.Nil(t, err)

	// test ListDir
	files, err := f.ListDir(newDirPath)
	require.Nil(t, err)
	require.Greater(t, len(files), 0)

	// cleanup
	_ = f.DeleteDir(newDirPath)

	_ = f.Close()
}

func TestFiler_ListDir(t *testing.T) {
	f, _ := setup()
	localDirPath, _ := filepath.Abs(".")
	newDirPath := "test_dir"
	collection := "test_collection"

	// test UploadDir
	_, err := f.UploadDir(localDirPath, newDirPath, collection, "")
	require.Nil(t, err)

	// test ListDir
	files, err := f.ListDir(newDirPath)
	require.Nil(t, err)
	require.Greater(t, len(files), 0)

	// cleanup
	_ = f.DeleteDir(newDirPath)
}

func TestFiler_ListDirRecursive(t *testing.T) {
	f, _ := setup()
	localDirPath, _ := filepath.Abs(".")
	newDirPath := "test_dir"
	collection := "test_collection"

	// test UploadDir
	_, err := f.UploadDir(localDirPath, newDirPath, collection, "")
	require.Nil(t, err)

	// test ListDirRecursive
	files, err := f.ListDirRecursive(newDirPath)
	require.Nil(t, err)
	valid := false
	for _, f1 := range files {
		if f1.Name == "test" {
			require.NotNil(t, f1.Children)
			for _, f2 := range f1.Children {
				if f2.Name == "nested" {
					require.NotNil(t, f2.Children)
					for _, f3 := range f2.Children {
						if f3.Name == "nested_file.txt" && f3.Extension == "txt" {
							valid = true
						}
					}
				}
			}
		}
	}
	require.True(t, valid)

	// cleanup
	_ = f.DeleteDir(newDirPath)
}
