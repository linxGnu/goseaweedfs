package goseaweedfs

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/linxGnu/goseaweedfs/model"
)

func TestUnzipAndLoading(t *testing.T) {
	cm1 := &model.ChunkManifest{
		Mime: "images_test",
		Name: "test.txt",
		Size: 12345,
		Chunks: []*model.ChunkInfo{
			&model.ChunkInfo{
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
	writer.Write(mar)
	writer.Close()

	// try to load chunk manifest
	cm2, err := model.LoadChunkManifest(b.Bytes(), true)
	if err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}

	if cm1.Mime != cm2.Mime || cm1.Name != cm2.Name || cm1.Size != cm2.Size {
		t.Fatal(fmt.Errorf("LoadChunkManifest and Gzip failed"))
	}

	if len(cm2.Chunks) != 1 || cm2.Chunks[0].Fid != cm1.Chunks[0].Fid || cm2.Chunks[0].Offset != cm1.Chunks[0].Offset || cm2.Chunks[0].Size != cm1.Chunks[0].Size {
		t.Fatal(fmt.Errorf("LoadChunkManifest and Gzip failed"))
	}
}
