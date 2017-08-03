// The following environment variables, if set, will be used:
//
//	* GOSWFS_MASTER_URL
//  * GOSWFS_SCHEME
//	* GOSWFS_MEDIUM_FILE
//	* GOSWFS_SMALL_FILE
//
package goseaweedfs

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

var sw *Seaweed

var MediumFile, SmallFile string

func init() {
	// check master url
	if masterURL := os.Getenv("GOSWFS_MASTER_URL"); masterURL != "" {
		scheme := os.Getenv("GOSWFS_SCHEME")
		if scheme == "" {
			scheme = "http"
		}
		sw = NewSeaweed(scheme, []string{masterURL}, nil, 0, 2*time.Minute)
	}

	MediumFile = os.Getenv("GOSWFS_MEDIUM_FILE")
	SmallFile = os.Getenv("GOSWFS_SMALL_FILE")
}

func TestUploadLookupserverReplaceDeleteFile(t *testing.T) {
	if sw == nil {
		return
	}

	if MediumFile == "" {
		return
	}

	for i := 1; i <= 2; i++ {
		_, fp, fID, err := sw.UploadFile(MediumFile, "", "")
		if err != nil {
			t.Fail()
			return
		}
		fmt.Println(fp, fID)

		//
		if server, err := sw.LookupServerByFileID(fID, nil, true); err != nil {
			t.Fail()
			return
		} else {
			fmt.Println(server)
		}

		//
		if fullURL, err := sw.LookupFileID(fID, nil, true); err != nil {
			t.Fail()
			return
		} else {
			fmt.Println(fullURL)
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, false); err != nil {
			t.Fail()
			return
		} else {
			fmt.Println("Replaced:", fID)
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, true); err != nil {
			t.Fail()
			return
		} else {
			fmt.Println("Replaced:", fID)
		}

		err = sw.DeleteFile(fID, nil)
		if err != nil {
			t.Fail()
			return
		}
	}
}

func TestBatchUploadFiles(t *testing.T) {
	if sw == nil {
		return
	}

	if MediumFile != "" && SmallFile != "" {
		_, err := sw.BatchUploadFiles([]string{MediumFile, SmallFile}, "", "")
		if err != nil {
			t.Fail()
			return
		}
	} else if MediumFile != "" {
		_, err := sw.BatchUploadFiles([]string{MediumFile, MediumFile}, "", "")
		if err != nil {
			t.Fail()
			return
		}
	} else if SmallFile != "" {
		_, err := sw.BatchUploadFiles([]string{SmallFile, SmallFile}, "", "")
		if err != nil {
			t.Fail()
			return
		}
	}
}

func TestGrow(t *testing.T) {
	if sw == nil {
		return
	}

	if err := sw.Grow(12, "imgs", "000", "dc1"); err != nil {
		t.Fail()
	}
}

func TestLookup(t *testing.T) {
	if sw == nil {
		return
	}

	_, err := sw.Lookup("1", nil)
	if err != nil {
		t.Fail()
		return
	}
}

func TestLookupVolumeIDs(t *testing.T) {
	if sw == nil {
		return
	}

	res, err := sw.LookupVolumeIDs([]string{"50", "51", "1"})
	if err != nil {
		t.Fail()
		return
	}

	for k, v := range res {
		fmt.Println(k, v)
	}
}

func TestStatus(t *testing.T) {
	if sw == nil {
		return
	}

	status, err := sw.Status()
	if err != nil {
		t.Fail()
		return
	}

	mar, _ := json.Marshal(status)
	fmt.Println(string(mar))
}

func TestClusterStatus(t *testing.T) {
	if sw == nil {
		return
	}

	status, err := sw.ClusterStatus()
	if err != nil {
		t.Fail()
		return
	}

	mar, _ := json.Marshal(status)
	fmt.Println(string(mar))
}

func TestSubmit(t *testing.T) {
	if sw == nil {
		return
	}

	if SmallFile != "" {
		res, err := sw.Submit(SmallFile, "", "")
		if err != nil {
			t.Fail()
			return
		}

		fmt.Println(res)
	}
}

func TestDeleteChunks(t *testing.T) {
	if sw == nil {
		return
	}

	if MediumFile != "" {
		cm, fp, fID, err := sw.UploadFile(MediumFile, "", "")
		if err != nil {
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
			t.Fail()
			return
		}
	}
}
