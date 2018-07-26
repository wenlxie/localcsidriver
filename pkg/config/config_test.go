package config

import (
	"io"
	"io/ioutil"
	"os"
	"path"
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
				NeedCleanupDevice: false,
			},
		},
	}
	dirName, err := mkTempDir("config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	fileName := path.Join(dirName, "test-conf")
	if err := writeFile(fileName, []byte(testconfigTemp)); err != nil {
		t.Fatal(err)
	}

	out, err := LoadDriverConfig(fileName)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectedConfig, out) {
		t.Fatalf("Expected config %v but got %v", expectedConfig, out)
	}
}

func mkTempDir(prefix string) (string, error) {
	return ioutil.TempDir(os.TempDir(), prefix)
}

func writeFile(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}
