package config

// DriverConfiguration defines driver configuration objects
// TODO Need to find a way to marshal the struct more efficiently.
type DriverConfiguration struct {
	// The default filesystem to format new volumes with
	DefaultFilesystem string `json:"defaultFilesystem" yaml:"defaultFilesystem"`
	// The default volume size in bytes
	DefaultVolumeSize uint64 `json:"defaultVolumeSize" yaml:"defaultVolumeSize"`
	// StorageClassConfig defines configuration of Provisioner's storage classes
	StorageClassConfig map[string]MountConfig `json:"storageClassMap" yaml:"storageClassMap"`
	// NodeLabelsForPV contains a list of node labels to be copied to the PVs created by the provisioner
	// +optional
	NodeLabelsForPV []string `json:"nodeLabelsForPV" yaml:"nodeLabelsForPV"`
}