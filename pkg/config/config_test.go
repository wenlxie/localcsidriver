package config

import (
	"reflect"
	"testing"
)

var testconfigTemp = `
backendType: "lvm"
supportedFilesystems: "ext4,xfs"
lvmConfig:
- name: "vg1"
  discoveryDir: "/csi/discovery/vg1"
  tags: "tag1"
  blockCleanerCommand:
  - "/scripts/dd_zero.sh"
  - "2"
  needCleanupDevice: true
- name: "vg2"
  discoveryDir: "/csi/discovery/vg2"
  tags: "tag2"
`

func TestLoadDriverConfig(t *testing.T) {
	expectedConfig := &DriverConfig{
		BackendType: "lvm",
		SupportedFilesystems: "ext4,xfs",
		DefaultVolumeSize: defaultDefaultVolumeSize,
		LvmConfig: []VolumeGroupConfig{
			{
				Name: "vg1",
				DiscoveryDir: "/csi/discovery/vg1",
				Tags: "tag1",
				BlockCleanerCommand: []string{"/scripts/dd_zero.sh", "2"},
				NeedCleanupDevice: true,
			},
			{
				Name: "vg2",
				DiscoveryDir: "/csi/discovery/vg2",
				Tags: "tag2",
				BlockCleanerCommand: []string{},
				NeedCleanupDevice: false,
			},
		},

	}
	out, err := LoadDriverConfig(testconfigTemp)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedConfig, out) {
		t.Fatalf("Expected config %v but got %v", expectedConfig, out)
	}
}