package config

// VolumeGroupConfig is config item for a volume group in lvm.
type VolumeGroupConfig struct {
	Name string `json:"name" yaml:"name"`
	// Path to export devices,
	// which are expected to be added into the group.
	DiscoveryDir string `json:"discoveryDir" yaml:"discoveryDir"`
	// Value to tag the volume group with.
	// +optional
	Tags string `json:"tags" yaml:"tags"`
}

// DriverConfig defines driver configuration objects.
// TODO Need to find a way to marshal the struct more efficiently.
type DriverConfig struct {
	// Underlying mechanism of the driver.
	// currently only lvm is supported.
	StorageBackend string `json:"storageBackend" yaml:"storageBackend"`
	// LvmConfig defines lvm releated configuration.
	LvmConfig []VolumeGroupConfig `json:"lvmConfig" yaml:"lvmConfig"`
	// Supported filesystems to format new volumes with
	// +optional
	SupportedFilesystems string `json:"supportedFilesystems" yaml:"supportedFilesystems"`
	// The default volume size in bytes
	// +optional
	DefaultVolumeSize uint64 `json:"defaultVolumeSize" yaml:"defaultVolumeSize"`
}
