package backend

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/kubernetes-csi/localcsidriver/pkg/config"
	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"github.com/kubernetes-csi/localcsidriver/pkg/util"
)

type LvmBackend struct {
	*lvm.VolumeGroup
	discoveryDir        string
	blockCleanerCommand []string
	needCleanupDevice   bool
	tags                []string
}

func NewLvmBackend(config config.VolumeGroupConfig) (*LvmBackend, error) {
	tags := []string{}
	for _, tag := range strings.Split(config.Tags, ",") {
		if tag == "" {
			continue
		}
		if err := lvm.ValidateTag(tag); err != nil {
			return nil, fmt.Errorf("invalid tag %s: %v", tag, err)
		}

		tags = append(tags, tag)
	}
	return &LvmBackend{
		VolumeGroup:         lvm.NewVolumeGroup(config.Name),
		discoveryDir:        config.DiscoveryDir,
		blockCleanerCommand: config.BlockCleanerCommand,
		needCleanupDevice:   config.NeedCleanupDevice,
		tags:                tags,
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

	volPath, err := lv.Path()
	if err != nil {
		return err
	}

	// Cleaning up data on device
	if err := l.cleanupDataOnVolume(volName, volPath); err != nil {
		return err
	}

	return lv.Remove()
}

func (l *LvmBackend) cleanupDataOnVolume(volName, volPath string) error {
	if len(l.blockCleanerCommand) < 1 {
		// No cleanup command specified, use the default.
		return util.CleanupDataOnDevice(volPath)
	}

	err := l.execScript(volName, volPath, l.blockCleanerCommand[0], l.blockCleanerCommand[1:]...)
	if err != nil {
		return err
	}

	return nil
}

func (l *LvmBackend) execScript(volName, volPath string, exe string, exeArgs ...string) error {
	cmd := exec.Command(exe, exeArgs...)
	// Scripts should be able to fetch volume path from env.
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", config.LocalVolumeEnv, volPath))
	var wg sync.WaitGroup
	// Wait for stderr & stdout  go routines
	wg.Add(2)

	outReader, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	go func() {
		defer wg.Done()
		outScanner := bufio.NewScanner(outReader)
		for outScanner.Scan() {
			outstr := outScanner.Text()
			log.Printf("Cleanup lv %q: StdoutBuf - %q", volName, outstr)
		}
	}()

	errReader, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		defer wg.Done()
		errScanner := bufio.NewScanner(errReader)
		for errScanner.Scan() {
			errstr := errScanner.Text()
			log.Printf("Cleanup lv %q: StderrBuf - %q", volName, errstr)
		}
	}()

	err = cmd.Start()
	if err != nil {
		return err
	}

	wg.Wait()
	err = cmd.Wait()
	if err != nil {
		return err
	}

	return nil
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
			// It might take quite a while to erase the data,
			// so make it configurable.
			if l.needCleanupDevice {
				if err := util.CleanupDataOnDevice(device); err != nil {
					return fmt.Errorf(
						"Cannot zero partition table on %s: %v",
						device, err)
				}
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
