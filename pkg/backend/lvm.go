package backend

import (
	"fmt"

	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"github.com/kubernetes-csi/localcsidriver/pkg/util"
	"k8s.io/kubernetes/pkg/util/mount"
)

type LvmBackend struct {
	*lvm.VolumeGroup
	tags         []string
	discoveryDir string
	mounter      mount.Interface
}

func NewLvmBackend(groupName, discoveryDir string, tags []string, mounter mount.Interface) (*LvmBackend, error) {
	for _, tag := range tags {
		if err := lvm.ValidateTag(tag); err != nil {
			return nil, fmt.Errorf("invalid tag %s: %v", tag, err)
		}
	}
	return &LvmBackend{
		VolumeGroup:  lvm.NewVolumeGroup(groupName),
		discoveryDir: discoveryDir,
		tags:         tags,
		mounter:      mounter,
	}, nil
}

func (l *LvmBackend) CreateVolume(name string, sizeInBytes uint64) (Volume, error) {
	return l.CreateLogicalVolume(name, sizeInBytes, l.tags)
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
	// Files that has no according device found would be ignored.
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
			newPv, err := lvm.CreatePhysicalVolume(device)
			if err != nil {
				return fmt.Errorf("error creating physical volume for %s: %v", device, err)
			}
			devicesToAdd = append(devicesToAdd, newPv)
		} else {
			groupName := pv.GroupName()
			if groupName == "" {
				// PV need to be added.
				devicesToAdd = append(devicesToAdd, pv)
				continue
			}

			if groupName != l.Name() {
				fmt.Errorf("device %s is expected to be added to group %s, but turned out added to group %s", device, l.VolumeGroup.Name(), groupName)
			}
		}
	}

	_, err = lvm.LookupVolumeGroup(l.Name())
	if err != nil {
		if err != lvm.ErrVolumeGroupNotFound {
			return fmt.Errorf("error looking up volume group %s: %v", l.Name(), err)
		}

		// Group not exist, create it.
		if _, err := lvm.CreateVolumeGroup(l.Name(), devicesToAdd, l.tags); err != nil {
			return fmt.Errorf("error creating volume group %s: %v", l.Name(), err)
		}
	} else {
		// Extend existing group.
		return lvm.ExtendVolumeGroup(l.Name(), devicesToAdd)
	}

	return nil
}