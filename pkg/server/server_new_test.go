package server

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	stdlog "log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/google/uuid"
	"github.com/kubernetes-csi/localcsidriver/pkg/cleanup"
	"github.com/kubernetes-csi/localcsidriver/pkg/config"
	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
)

// The size of the source devices we create in our tests.
const devsize = 3 << 30 // 3GiB
const defaultsize = 1 << 30
const vgname = "test-group"


func init() {
	// Set test logging
	stdlog.SetFlags(stdlog.LstdFlags | stdlog.Lshortfile)
}

// IdentityService RPCs

func testGetPluginInfoRequest() *csi.GetPluginInfoRequest {
	req := &csi.GetPluginInfoRequest{}
	return req
}

func TestGetPluginInfo(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testGetPluginInfoRequest()
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetName() != PluginName {
		t.Fatalf("Expected plugin name %s but got %s", PluginName, resp.GetName())
	}
	if resp.GetVendorVersion() != PluginVersion {
		t.Fatalf("Expected plugin version %s but got %s", PluginVersion, resp.GetVendorVersion())
	}
	if resp.GetManifest() != nil {
		t.Fatalf("Expected a nil manifest but got %s", resp.GetManifest())
	}
}

func testGetPluginCapabilitiesRequest() *csi.GetPluginCapabilitiesRequest {
	req := &csi.GetPluginCapabilitiesRequest{}
	return req
}

func TestGetPluginCapabilities(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testGetPluginCapabilitiesRequest()
	resp, err := client.GetPluginCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if x := resp.GetCapabilities(); len(x) != 1 {
		t.Fatalf("Expected 1 capability, but got %v", x)
	}
	if x := resp.GetCapabilities()[0].GetService().Type; x != csi.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Fatalf("Expected plugin to have capability CONTROLLER_SERVICE but had %v", x)
	}
}

// ControllerService RPCs

func testCreateVolumeRequest() *csi.CreateVolumeRequest {
	const requiredBytes = 2 << 30
	const limitBytes = 5 << 30
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: "xfs",
					MountFlags: nil,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.CreateVolumeRequest{
		Name: "test-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: requiredBytes,
			LimitBytes: limitBytes,
		},
		VolumeCapabilities: volumeCapabilities,
		Parameters: nil,
		ControllerCreateSecrets: nil,
	}
	return req
}

type repeater struct {
	src byte
}

func (r repeater) Read(buf []byte) (int, error) {
	n := copy(buf, bytes.Repeat([]byte{r.src}, len(buf)))
	return n, nil
}

func TestCreateVolume(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetId(), req.GetName())
	}
}

func TestCreateVolumeDefaultSize(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testCreateVolumeRequest()
	// Specify no CapacityRange so the volume gets the default
	// size.
	req.CapacityRange = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if uint64(info.GetCapacityBytes()) != defaultsize {
		t.Fatalf("Expected defaultVolumeSize (%v) to match volume size (%v).", devsize, info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetId(), req.GetName())
	}
}

func TestCreateVolumeIdempotent(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Check that trying to create the exact same volume again succeeds.
	resp2, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	volInfo := resp2.GetVolume()
	if got := volInfo.GetCapacityBytes(); got != req.CapacityRange.RequiredBytes {
		t.Fatalf("Unexpected capacity_bytes %v != %v", got, req.CapacityRange.RequiredBytes)
	}
	if got := volInfo.GetId(); got != resp1.GetVolume().GetId() {
		t.Fatalf("Unexpected id %v != %v", got, resp1.GetVolume().GetId())
	}
	if got := volInfo.GetAttributes(); !reflect.DeepEqual(got, resp1.GetVolume().GetAttributes()) {
		t.Fatalf("Unexpected attributes %v != %v", got, resp1.GetVolume().GetAttributes())
	}
}

func TestCreateVolumeAlreadyExistsCapacityRange(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	_, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Check that trying to create a volume with the same name but
	// incompatible capacity_range fails.
	req.CapacityRange.RequiredBytes += 1
	_, err = client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeAlreadyExists) {
		t.Fatal(err)
	}
}

func TestCreateVolumeAlreadyExistsVolumeCapabilities(t *testing.T) {
	client,_,  clean := setupServer()
	defer clean()
	// Create a test volume.
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Format the newly created volume with xfs.
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.LookupLogicalVolume(resp1.GetVolume().GetId())
	if err != nil {
		t.Fatal(err)
	}
	lvpath, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := formatDevice(lvpath, "xfs"); err != nil {
		t.Fatal(err)
	}
	// Wait for filesystem creation to be reflected in udev.
	_, err = exec.Command("udevadm", "settle").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}

	// Try and create the same volume, with 'ext4' specified as fs_type in
	// a mount volume_capability.
	req.VolumeCapabilities[1].GetMount().FsType = "ext4"
	_, err = client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeAlreadyExists) {
		t.Fatal(err)
	}
}

func TestCreateVolumeIdempotentUnspecifiedExistingFsType(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create a test volume.
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Format the newly created volume with xfs.
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.LookupLogicalVolume(resp1.GetVolume().GetId())
	if err != nil {
		t.Fatal(err)
	}
	lvpath, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := formatDevice(lvpath, "xfs"); err != nil {
		t.Fatal(err)
	}
	// Wait for filesystem creation to be reflected in udev.
	_, err = exec.Command("udevadm", "settle").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}

	// Try and create the same volume, with no specified fs_type in a mount
	// volume_capability.
	req.VolumeCapabilities[1].GetMount().FsType = ""
	resp2, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	volInfo := resp2.GetVolume()
	if got := volInfo.GetCapacityBytes(); got != req.CapacityRange.RequiredBytes {
		t.Fatalf("Unexpected capacity_bytes %v != %v", got, req.CapacityRange.RequiredBytes)
	}
	if got := volInfo.GetId(); got != resp1.GetVolume().GetId() {
		t.Fatalf("Unexpected id %v != %v", got, resp1.GetVolume().GetId())
	}
	if got := volInfo.GetAttributes(); !reflect.DeepEqual(got, resp1.GetVolume().GetAttributes()) {
		t.Fatalf("Unexpected attributes %v != %v", got, resp1.GetVolume().GetAttributes())
	}
}

func TestCreateVolumeCapacityRangeNotSatisfied(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create a test volume.
	req := testCreateVolumeRequest()
	// Make requirement beyond available capacity
	req.CapacityRange.RequiredBytes *= 2
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrInsufficientCapacity) {
		t.Fatal(err)
	}
}

func TestCreateVolumeInvalidVolumeName(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create a test volume.
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.Name = "invalid name : /"
	_, err := client.CreateVolume(context.Background(), req)
	expdesc := "Name contains invalid character"
	if !strings.Contains(err.Error(), expdesc) {
		t.Fatal(err)
	}
}

func testDeleteVolumeRequest(volumeId string) *csi.DeleteVolumeRequest {
	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeId,
		ControllerDeleteSecrets: nil,
	}
	return req
}

func TestDeleteVolume(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolumeIdempotent(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolumeUnknownVolume(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testDeleteVolumeRequest("test-group_missing-volume")
	_, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestControllerPublishVolumeNotSupported(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := &csi.ControllerPublishVolumeRequest{}
	_, err := client.ControllerPublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func TestControllerUnpublishVolumeNotSupported(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := &csi.ControllerUnpublishVolumeRequest{}
	_, err := client.ControllerUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func testValidateVolumeCapabilitiesRequest(volumeId string, filesystem string, mountOpts []string) *csi.ValidateVolumeCapabilitiesRequest {
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: filesystem,
					MountFlags: mountOpts,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: volumeId,
		VolumeCapabilities: volumeCapabilities,
		VolumeAttributes: nil,
	}
	return req
}

func TestValidateVolumeCapabilities(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	req := testValidateVolumeCapabilitiesRequest(volumeId, "xfs", nil)
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSupported() {
		t.Fatal("Expected requested volume capabilities to be supported.")
	}
}

func TestValidateVolumeCapabilitiesMissingVolume(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create the volume that we'll be publishing.
	validateReq := testValidateVolumeCapabilitiesRequest("test-group_foo", "xfs", nil)
	_, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if !grpcErrorEqual(err, ErrVolumeNotFound) {
		t.Fatal(err)
	}
}

func testListVolumesRequest() *csi.ListVolumesRequest {
	req := &csi.ListVolumesRequest{
		MaxEntries: 0,
		StartingToken: "",
	}
	return req
}

func TestListVolumesNoVolumes(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testListVolumesRequest()
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatal("Expected no entries.")
	}
}

func TestListVolumesTwoVolumes(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	var infos []*csi.Volume
	// Add the first volume.
	req := testCreateVolumeRequest()
	req.Name = "test-volume-1"
	req.CapacityRange.RequiredBytes /= 2
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolume())
	// Add the second volume.
	req = testCreateVolumeRequest()
	req.Name = "test-volume-2"
	req.CapacityRange.RequiredBytes /= 2
	resp, err = client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolume())
	// Check that ListVolumes returns the two volumes.
	listReq := testListVolumesRequest()
	listResp, err := client.ListVolumes(context.Background(), listReq)
	if err != nil {
		t.Fatal(err)
	}
	entries := listResp.GetEntries()
	if len(entries) != len(infos) {
		t.Fatalf("ListVolumes returned %v entries, expected %d.", len(entries), len(infos))
	}
	for _, entry := range entries {
		had := false
		for _, info := range infos {
			if reflect.DeepEqual(info, entry.GetVolume()) {
				had = true
				break
			}
		}
		if !had {
			t.Fatalf("Cannot find volume info %+v in %+v.", entry.GetVolume(), infos)
		}
	}
}

func testGetCapacityRequest(fstype string) *csi.GetCapacityRequest {
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: fstype,
					MountFlags: nil,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.GetCapacityRequest{
		VolumeCapabilities: volumeCapabilities,
		Parameters: nil,
	}
	return req
}

func TestGetCapacity(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := testGetCapacityRequest("xfs")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Two extents are reserved for metadata.
	const extentSize = uint64(2 << 20)
	const metadataExtents = 2
	exp := devsize - extentSize*metadataExtents
	if got := resp.GetAvailableCapacity(); got != int64(exp) {
		t.Fatalf("Expected %d bytes free but got %v.", exp, got)
	}
}

func TestControllerGetCapabilities(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	req := &csi.ControllerGetCapabilitiesRequest{}
	resp, err := client.ControllerGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	expected := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	}
	got := []csi.ControllerServiceCapability_RPC_Type{}
	for _, capability := range resp.GetCapabilities() {
		got = append(got, capability.GetRpc().GetType())
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("Expected capabilities %+v but got %+v", expected, got)
	}
}

// NodeService RPCs

func testNodePublishVolumeRequest(volumeId string, targetPath string, stagingTargetPath string, filesystem string, mountOpts []string) *csi.NodePublishVolumeRequest {
	var volumeCapability *csi.VolumeCapability
	if filesystem == "block" {
		volumeCapability = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	} else {
		volumeCapability = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: filesystem,
					MountFlags: mountOpts,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	}
	const readonly = false
	req := &csi.NodePublishVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		TargetPath:        targetPath,
		VolumeCapability:  volumeCapability,
		Readonly:          readonly,
	}
	return req
}

func testNodeUnpublishVolumeRequest(volumeId string, targetPath string) *csi.NodeUnpublishVolumeRequest {
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeId,
		TargetPath: targetPath,
	}
	return req
}

func TestNodePublishVolumeNodeUnpublishVolumeBlockVolume(t *testing.T) {
	client, _, clean := setupServer()
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	// Take volume path as symlink source.
	volumePath := createResp.GetVolume().GetAttributes()[VolumePathKey]
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "server_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	// As we're publishing as a BlockVolume we need to bind mount
	// the device over a file, not a directory.
	if file, err := os.Create(targetPath); err != nil {
		t.Fatal(err)
	} else {
		// Immediately close the file, we're just creating it
		// as a mount target.
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	defer os.Remove(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, volumePath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	link, err := os.Readlink(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if link == "" {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
}

func TestNodeStagePublishUnpublishUnstageMountVolume(t *testing.T) {
	client, server, clean := setupServer()
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)

	// Prepare another temporary directory as staging (global mount) path.
	stagingPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(stagingPath)

	volumePath := createResp.GetVolume().GetAttributes()[VolumePathKey]
	// Stage the volume
	stageReq := testNodeStageVolumeRequest(volumeId, volumePath, stagingPath, "xfs", []string{})
	_, err = client.NodeStageVolume(context.Background(), stageReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnstageVolumeRequest(volumeId, stageReq.StagingTargetPath)
		_, err = client.NodeUnstageVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Publish the volume
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, stagingPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()


	// Ensure that the device was mounted.
	notMnt, err := server.mounter.IsNotMountPoint(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if notMnt {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Create a file on the mounted volume.
	file, err := os.Create(filepath.Join(targetPath, "test"))
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	// Check that the file exists where it is expected.
	matches, err := filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
	// Unpublish to check that the file is now missing.
	req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Check that the targetPath is now no longer a mountpoint.
	notMnt, err = server.mounter.IsNotMountPoint(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !notMnt {
		t.Fatalf("Expected target path %v not to be a mountpoint.", publishReq.TargetPath)
	}
	// Check that the file is now missing.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("Expected to see no files but got %v.", matches)
	}
	// Publish again to make sure the file comes back
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = false
	// Check that the file exists where it is expected.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
}

func testNodeStageVolumeRequest(volumeId string, sourcepath string, stagingTargetPath string, filesystem string, mountOpts []string) *csi.NodeStageVolumeRequest {
	var volumeCapability *csi.VolumeCapability
	if filesystem == "block" {
		volumeCapability = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	} else {
		volumeCapability = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: filesystem,
					MountFlags: mountOpts,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	}

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
		VolumeCapability:  volumeCapability,
		VolumeAttributes: map[string]string{VolumePathKey: sourcepath},
	}
	return req
}

func testNodeUnstageVolumeRequest(volumeId string, stagingTargetPath string) *csi.NodeUnstageVolumeRequest {
	req := &csi.NodeUnstageVolumeRequest{
		VolumeId:          volumeId,
		StagingTargetPath: stagingTargetPath,
	}
	return req
}

func setupServer() (client *Client, server *Server, cleanupFn func()) {
	// Generate discovery directory for test.
	discoveryDir, err := ioutil.TempDir(os.TempDir(), "server-test")
	if err != nil {
		panic(err)
	}
	// Generate source device for test
	device, err := lvm.CreateLoopDevice(devsize)
	if err != nil {
		panic(err)
	}

	// Symlink to discovery directory.
	exportedFile := path.Join(discoveryDir, "test")
	err = os.Symlink(device.Path(), exportedFile)
	if err != nil {
		panic(err)
	}

	conf := &config.DriverConfig{
		BackendType: "lvm",
		SupportedFilesystems: "ext4,xfs",
		DefaultVolumeSize: defaultsize,
		LvmConfig: []config.VolumeGroupConfig{
			{
				Name: "test-group",
				DiscoveryDir: discoveryDir,
				BlockCleanerCommand: []string{"echo"},
			},
		},
	}

	var clean cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			clean.Unwind()
			panic(x)
		}
	}()
	lis, err := net.Listen("unix", "@/local-test-"+uuid.New().String())
	if err != nil {
		panic(err)
	}
	clean.Add(lis.Close)
	clean.Add(func() error {
		os.RemoveAll(discoveryDir)
		device.Close()

		return nil
	})
	clean.Add(func() error {
		pv, err := lvm.LookupPhysicalVolume(device.Path())
		if err != nil {
			if err == lvm.ErrPhysicalVolumeNotFound {
				return nil
			}
			panic(err)
		}
		if err := pv.Remove(); err != nil {
			panic(err)
		}
		return nil
	})
	clean.Add(func() error {
		vg, err := lvm.LookupVolumeGroup(vgname)
		if err == lvm.ErrVolumeGroupNotFound {
			// Already removed this volume group in the test.
			return nil
		}
		if err != nil {
			panic(err)
		}
		return vg.Remove()
	})
	clean.Add(func() error {
		vg, err := lvm.LookupVolumeGroup(vgname)
		if err == lvm.ErrVolumeGroupNotFound {
			// Already removed this volume group in the test.
			return nil
		}
		if err != nil {
			panic(err)
		}
		lvnames, err := vg.ListLogicalVolumeNames()
		if err != nil {
			panic(err)
		}
		for _, lvname := range lvnames {
			lv, err := vg.LookupLogicalVolume(lvname)
			if err != nil {
				panic(err)
			}
			if err := lv.Remove(); err != nil {
				panic(err)
			}
		}
		return nil
	})


	s, err := New(conf)
	if err != nil {
		panic(err)
	}

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.UnaryInterceptor(
			ChainUnaryServer(
				LoggingInterceptor(),
			),
		),
	)
	// setup logging
	logflags := stdlog.LstdFlags | stdlog.Lshortfile
	SetLogger(stdlog.New(os.Stderr, "server-test", logflags))
	// Start a grpc server listening on the socket.
	grpcServer := grpc.NewServer(opts...)
	csi.RegisterIdentityServer(grpcServer, s)
	csi.RegisterControllerServer(grpcServer, s)
	csi.RegisterNodeServer(grpcServer, s)
	go grpcServer.Serve(lis)

	// Start a grpc client connected to the server.
	unixDialer := func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	}
	clientOpts := []grpc.DialOption{
		grpc.WithDialer(unixDialer),
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(lis.Addr().String(), clientOpts...)
	if err != nil {
		panic(err)
	}
	clean.Add(conn.Close)
	client = NewClient(conn)

	if err := s.Setup(make(chan struct{})); err != nil {
		panic(err)
	}
	return client, s, clean.Unwind
}

func formatDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("csilvm: formatDevice: " + string(output))
	}
	return nil
}
