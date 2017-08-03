package model

import (
	"fmt"
	"math/rand"
)

type VolumeLocation struct {
	URL       string `json:"url,omitempty"`
	PublicURL string `json:"publicUrl,omitempty"`
}

// VolumeLocations returned VolumeLocations (volumes)
type VolumeLocations []*VolumeLocation

// Head get first location in list
func (c VolumeLocations) Head() *VolumeLocation {
	if len(c) == 0 {
		return nil
	}

	return c[0]
}

// RandomPickForRead random pick a location for further read request
func (c VolumeLocations) RandomPickForRead() *VolumeLocation {
	if len(c) == 0 {
		return nil
	}

	return c[rand.Intn(len(c))]
}

type LookupResult struct {
	VolumeID        string          `json:"volumeId,omitempty"`
	VolumeLocations VolumeLocations `json:"locations,omitempty"`
	Error           string          `json:"error,omitempty"`
}

func (c *LookupResult) String() string {
	return fmt.Sprintf("VolumeId:%s, Locations:%v, Error:%s", c.VolumeID, c.VolumeLocations, c.Error)
}
