package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/linxGnu/goseaweedfs"
)

const (
	// around 79 MB File
	MediumFile = "/Volumes/MacSpace/Setup/mysql-5.6.27-linux-x86_64.tar.gz"

	// around 3.4 MB
	SmallFile = "/Volumes/MacSpace/Setup/sdb1.sql"
)

func TestUploadLookupserverReplaceDeleteFile(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)

	for i := 1; i <= 2; i++ {
		_, fp, fID, err := sw.UploadFile(MediumFile, "", "")
		if err != nil {
			fmt.Println(err)
			t.Fail()
			return
		}
		fmt.Println(fp, fID)

		//
		if server, err := sw.LookupServerByFileID(fID, nil, true); err != nil {
			fmt.Println(err)
			t.Fail()
			return
		} else {
			fmt.Println(server)
		}

		//
		if fullURL, err := sw.LookupFileID(fID, nil, true); err != nil {
			fmt.Println(err)
			t.Fail()
			return
		} else {
			fmt.Println(fullURL)
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, false); err != nil {
			fmt.Println(err)
			t.Fail()
			return
		} else {
			fmt.Println("Replaced:", fID)
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, true); err != nil {
			fmt.Println(err)
			t.Fail()
			return
		} else {
			fmt.Println("Replaced:", fID)
		}

		err = sw.DeleteFile(fID, nil)
		if err != nil {
			fmt.Println(err)
			t.Fail()
			return
		}
	}
}

func TestBatchUploadFiles(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)

	//
	result, err := sw.BatchUploadFiles([]string{MediumFile, SmallFile}, "", "")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	for _, res := range result {
		fmt.Println(res)
	}
}

func TestGrow(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)
	if err := sw.Grow(12, "imgs", "000", "dc1"); err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

func TestLookup(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)
	_, err := sw.Lookup("1", nil)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
}

func TestLookupVolumeIDs(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)
	res, err := sw.LookupVolumeIDs([]string{"50", "51", "1"})
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	for k, v := range res {
		fmt.Println(k, v)
	}
}

func TestStatus(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)
	status, err := sw.Status()
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
	mar, _ := json.Marshal(status)
	fmt.Println(string(mar))
}

func TestClusterStatus(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)
	status, err := sw.ClusterStatus()
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
	mar, _ := json.Marshal(status)
	fmt.Println(string(mar))
}

func TestSubmit(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 0, 2*time.Minute)

	res, err := sw.Submit(SmallFile, "", "")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	fmt.Println(res)
}

func TestDeleteChunks(t *testing.T) {
	sw := goseaweedfs.NewSeaweed("http", "localhost:8898", nil, 33554432, 2*time.Minute)

	cm, fp, fID, err := sw.UploadFile(MediumFile, "", "")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
	fmt.Println(fp, fID)
	fmt.Println(cm)
	for _, v := range cm.Chunks {
		fmt.Println(v)
	}

	err = sw.DeleteChunks(cm, nil)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
}
