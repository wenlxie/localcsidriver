package backend

import (
	"fmt"
	"log"
	"os"

	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"github.com/kubernetes-csi/localcsidriver/pkg/util"
)

type LvmBackend struct {
	*lvm.VolumeGroup
	tags         []string
	discoveryDir string
}

func NewLvmBackend(groupName, discoveryDir string, tags []string) (*LvmBackend, error) {
	for _, tag := range tags {
		if err := lvm.ValidateTag(tag); err != nil {
			return nil, fmt.Errorf("invalid tag %s: %v", tag, err)
		}
	}
	return &LvmBackend{
		VolumeGroup:  lvm.NewVolumeGroup(groupName),
		discoveryDir: discoveryDir,
		tags:         tags,
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
	// List all of the files under discovery path
	files, err := util.ReadDir(l.discoveryDir)
	if err != nil {
		return fmt.Errorf("failed to read files under discovery path: %v", err)
	}

	// Read links of the devices exported via files in discovery path.
	// Ignore the files whose links are failed to be read.
	devices := []string{}
	for _, file := range files {
		device, err := os.Readlink(file)
		if err != nil {
			log.Printf("Failed to read link of file %s: %v", file, err)
			continue
		}

		devices = append(devices, device)
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
			if err := util.ZeroPartitionTable(device); err != nil {
				return fmt.Errorf(
					"Cannot zero partition table on %s: %v",
					device, err)
			}
			newPv, err := lvm.CreatePhysicalVolume(device)
			if err != nil {
				return fmt.Errorf("error creating physical volume for %s: %v", device, err)
			}
			devicesToAdd = append(devicesToAdd, newPv)
		} else {
			groupName := pv.GroupName()
			if groupName == "" {
				// PV need to be added to group.
				devicesToAdd = append(devicesToAdd, pv)
				continue
			}

			if groupName != l.Name() {
				fmt.Errorf("device %s is expected to be added to group %s, but turned out in group %s", device, l.VolumeGroup.Name(), groupName)
			}
		}
	}

	_, err = lvm.LookupVolumeGroup(l.Name())
	if err != nil {
		if err != lvm.ErrVolumeGroupNotFound {
			return fmt.Errorf("error looking up volume group %s: %v", l.Name(), err)
		}

		// Group not exist, create it.
		if len(devicesToAdd) == 0 {
			return fmt.Errorf("volume group %s not exist, and no source device provided", l.Name())
		}

		if _, err := lvm.CreateVolumeGroup(l.Name(), devicesToAdd, l.tags); err != nil {
			return fmt.Errorf("error creating volume group %s: %v", l.Name(), err)
		}
	} else if len(devicesToAdd) > 0 {
		// Extend existing group.
		return lvm.ExtendVolumeGroup(l.Name(), devicesToAdd)
	}

	// TODO: consider detecting removed files from discovery dir,
	// and reduce the group accordingly.

	return nil
}
