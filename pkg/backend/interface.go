package backend

type Volume interface {
	Name() string
	SizeInBytes() uint64
	// Path returns the device path for the volume.
	Path() (string, error)
	// Tags returns the volume group tags.
	Tags() ([]string, error)
}

// StorageBackend is used to de-couple local storage manager and backend storage.
type StorageBackend interface {
	Name() string
	BytesTotal() (uint64, error)
	BytesFree() (uint64, error)
	CreateVolume(name string, sizeInBytes uint64) (Volume, error)
	LookupVolume(name string) (Volume, error)
	ListVolumeNames() ([]string, error)
	DeleteVolume(volName string) error
	// Sync should be called by server regularly,
	// to keep the backend in an healthy state, and data up-to-date.
	Sync() error
}
