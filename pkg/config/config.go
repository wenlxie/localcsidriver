package config

import (
	"fmt"
	"io/ioutil"

	"github.com/ghodss/yaml"
)

const (
	defaultDefaultFs         = "ext4"
	defaultDefaultVolumeSize = 10 << 30
)

// LocalVolumeEnv will contain the device path when script is invoked
const LocalVolumeEnv = "LOCAL_VOL_BLKDEVICE"

// VolumeGroupConfig is config item for a volume group in lvm.
type VolumeGroupConfig struct {
	Name string `json:"name" yaml:"name"`
	// Path to export devices,
	// which are expected to be added into the group.
	DiscoveryDir string `json:"discoveryDir" yaml:"discoveryDir"`
	// Value to tag the volume group with.
	// +optional
	Tags string `json:"tags" yaml:"tags"`
	// The type of block cleaner to use before deleting a volume.
	BlockCleanerCommand []string `json:"blockCleanerCommand" yaml:"blockCleanerCommand"`
	// Whether to erase possible data on discovered devices before taking them into group,
	// If set the field to false, users must ensure the devices are clean before exporting them to driver.
	// +optional
	NeedCleanupDevice bool `json:"needCleanupDevice" yaml:"needCleanupDevice"`
}

// DriverConfig defines driver configuration objects.
type DriverConfig struct {
	// Underlying mechanism of the driver.
	// currently only lvm is supported.
	BackendType string `json:"backendType" yaml:"backendType"`
	// LvmConfig defines lvm related configuration.
	LvmConfig []VolumeGroupConfig `json:"lvmConfig" yaml:"lvmConfig"`
	// Supported filesystems to format new volumes with.
	// Default to ext4 if not set.
	// +optional
	SupportedFilesystems string `json:"supportedFilesystems" yaml:"supportedFilesystems"`
	// The default volume size in bytes.
	// Default to 10 GiB if not set.
	// +optional
	DefaultVolumeSize uint64 `json:"defaultVolumeSize" yaml:"defaultVolumeSize"`
	// Node name to construct access topology for created volumes
	// +optional
	NodeName string `json:"nodeName" yaml:"nodeName"`
}

// LoadDriverConfig loads config from given file.
func LoadDriverConfig(configPath string) (*DriverConfig, error) {
	driverConfig := &DriverConfig{}
	fileContents, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %s due to: %v", configPath, err)
	}

	if err := yaml.Unmarshal(fileContents, driverConfig); err != nil {
		return nil, fmt.Errorf("fail to Unmarshal yaml due to: %#v", err)
	}

	initConfigItems(driverConfig)

	return driverConfig, nil
}

func initConfigItems(conf *DriverConfig) {
	if conf.SupportedFilesystems == "" {
		conf.SupportedFilesystems = defaultDefaultFs
	}

	if conf.DefaultVolumeSize == 0 {
		conf.DefaultVolumeSize = defaultDefaultVolumeSize
	}
}
