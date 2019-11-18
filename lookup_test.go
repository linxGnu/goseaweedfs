package goseaweedfs

import (
	"fmt"
	"testing"
)

func TestLookUp(t *testing.T) {
	vols := make(VolumeLocations, 0)

	if vols.Head() != nil {
		t.Fatal(fmt.Errorf("VolumeLocation func head invalid"))
	}

	if vols.RandomPickForRead() != nil {
		t.Fatal(fmt.Errorf("VolumeLocation func random pick invalid"))
	}
}
