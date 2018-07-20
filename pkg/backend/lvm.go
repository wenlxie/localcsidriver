package backend

import (
	"fmt"

	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"github.com/kubernetes-csi/localcsidriver/pkg/util"
	"k8s.io/kubernetes/pkg/util/mount"
)

type LvmBackend struct {
	*lvm.VolumeGroup
	discoveryDir string
	mounter      mount.Interface
}

func New(groupName, discoveryDir string) *LvmBackend {
	return &LvmBackend{
		VolumeGroup:  lvm.NewVolumeGroup(groupName),
		discoveryDir: discoveryDir,
		mounter:      mount.New("" /* default mount path */),
	}
}

func (l *LvmBackend) CreateVolume(name string, sizeInBytes uint64, tags []string) (Volume, error) {
	return l.CreateLogicalVolume(name, sizeInBytes, tags)
}

func (l *LvmBackend) LookupVolume(name string) (Volume, error) {
	return l.LookupLogicalVolume(name)
}

func (l *LvmBackend) ListVolumeNames() ([]string, error) {
	return l.ListLogicalVolumeNames()
}

func (l *LvmBackend) DeleteVolume(volName string) error {
	lv, err := l.LookupLogicalVolume(volName)
	if err != nil {
		return err
	}

	return lv.Remove()
}

// Sync walk through the files under its discovery path,
// and add them into its group if some of them are not already in.
func (l *LvmBackend) Sync() error {
	// List all of the mount points
	mountPoints, mountPointsErr := l.mounter.List()
	if mountPointsErr != nil {
		return fmt.Errorf("failed to list mount points: %v", mountPointsErr)
	}

	// List all of the files under discovery path
	files, err := util.ReadDir(l.discoveryDir)
	if err != nil {
		return fmt.Errorf("failed to read files under discovery path: %v", err)
	}

	// Find and record the devices that exported via files in discovery path.
	// Files that has not according device found would be ignored.
	devices := []string{}
	for _, file := range files {
		for _, mp := range mountPoints {
			if l.mounter.IsMountPointMatch(mp, file) {
				devices = append(devices, mp.Path)
				break
			}
		}
	}

	// Check if the devices exist in the group, add if not in.
	devicesToAdd := []*lvm.PhysicalVolume{}
	for _, device := range devices {
		pv, err := lvm.LookupPhysicalVolume(device)
		if err != nil {
			if err != lvm.ErrPhysicalVolumeNotFound {
				return fmt.Errorf("error looking up physical volume for %s: %v", device, err)
			}
			// PV not exist, create one
			if _, err := lvm.CreatePhysicalVolume(device); err != nil {
				return fmt.Errorf("error creating physical volume for %s: %v", device, err)
			}
			devicesToAdd = append(devicesToAdd, device)
		} else {
			groupName := pv.GroupName()
			if groupName == "" {
				// PV need to be added.
				devicesToAdd = append(devicesToAdd, device)
			}

			if groupName != l.VolumeGroup.Name() {
				fmt.Errorf("device is expected to be added to group %s, but turned out added to group %s", device, l.VolumeGroup.Name(), groupName)
			}
		}
	}

	if l.VolumeGroup == nil {
		lvm.CreateVolumeGroup()
	}
}