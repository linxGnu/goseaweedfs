// The following environment variables, if set, will be used:
//
//	* GOSWFS_MASTER_URL
//  * GOSWFS_SCHEME
//	* GOSWFS_MEDIUM_FILE
//	* GOSWFS_SMALL_FILE
//
package goseaweedfs

import (
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
		sw = NewSeaweed(scheme, []string{masterURL}, nil, 1024*1024*2, 5*time.Minute)
	}

	MediumFile = os.Getenv("GOSWFS_MEDIUM_FILE")
	SmallFile = os.Getenv("GOSWFS_SMALL_FILE")

	time.Sleep(5 * time.Second)
}

func TestUploadLookupserverReplaceDeleteFile(t *testing.T) {
	if sw == nil {
		return
	}

	if MediumFile == "" {
		return
	}

	for i := 1; i <= 2; i++ {
		_, _, fID, err := sw.UploadFile(MediumFile, "", "")
		if err != nil {
			t.Fail()
			return
		}

		//
		if _, err := sw.LookupServerByFileID(fID, nil, true); err != nil {
			t.Fail()
			return
		}

		//
		if _, err := sw.LookupFileID(fID, nil, true); err != nil {
			t.Fail()
			return
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, false); err != nil {
			t.Fail()
			return
		}

		//
		if err := sw.ReplaceFile(fID, SmallFile, true); err != nil {
			t.Fail()
			return
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

	_, err := sw.LookupVolumeIDs([]string{"50", "51", "1"})
	if err != nil {
		t.Fail()
		return
	}
}

func TestStatus(t *testing.T) {
	if sw == nil {
		return
	}

	_, err := sw.Status()
	if err != nil {
		t.Fail()
		return
	}
}

func TestClusterStatus(t *testing.T) {
	if sw == nil {
		return
	}

	_, err := sw.ClusterStatus()
	if err != nil {
		t.Fail()
		return
	}
}

func TestSubmit(t *testing.T) {
	if sw == nil {
		return
	}

	if SmallFile != "" {
		_, err := sw.Submit(SmallFile, "", "")
		if err != nil {
			t.Fail()
			return
		}
	}
}

func TestDeleteChunks(t *testing.T) {
	if sw == nil {
		return
	}

	if MediumFile != "" {
		cm, _, _, err := sw.UploadFile(MediumFile, "", "")
		if err != nil {
			t.Fail()
			return
		}

		err = sw.DeleteChunks(cm, nil)
		if err != nil {
			t.Fail()
			return
		}
	}
}
