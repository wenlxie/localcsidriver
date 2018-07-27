package server

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	stdlog "log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/google/uuid"
	"github.com/kubernetes-csi/localcsidriver/pkg/cleanup"
	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_UnspecifiedFS(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "", nil)
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
	if !targetPathIsMountPoint(publishReq.TargetPath) {
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
	if targetPathIsMountPoint(publishReq.TargetPath) {
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

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_ReadOnly(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	// Publish the volume.
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
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
	if !targetPathIsMountPoint(publishReq.TargetPath) {
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
	if targetPathIsMountPoint(publishReq.TargetPath) {
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
	// Unpublish to volume so that we can republish it as readonly.
	req = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Publish the volume again, as SINGLE_NODE_READER_ONLY (ie., readonly).
	targetPathRO := filepath.Join(tmpdirPath, volumeId+"-ro")
	if err := os.Mkdir(targetPathRO, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPathRO)
	publishReqRO := testNodePublishVolumeRequest(volumeId, targetPathRO, "xfs", nil)
	publishReqRO.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	_, err = client.NodePublishVolume(context.Background(), publishReqRO)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReqRO.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that the file exists at the new, readonly targetPath.
	roFilepath := filepath.Join(publishReqRO.TargetPath, filepath.Base(file.Name()))
	matches, err = filepath.Glob(roFilepath)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != roFilepath {
		t.Fatalf("Expected to see file %v but got %v.", roFilepath, matches[0])
	}
	// Check that we cannot create a new file at this location.
	_, err = os.Create(roFilepath + ".2")
	if err.(*os.PathError).Err != syscall.EROFS {
		t.Fatal("Expected file creation to fail due to read-only filesystem.")
	}
}

func TestNodePublishVolume_BlockVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
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
	// Check that calling NodePublishVolume with the same
	// parameters succeeds and doesn't mount anything new at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	publishReq = testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent publish to not mount anything new at targetPath.")
	}
}

func TestNodePublishVolume_BlockVolume_TargetPathOccupied(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolume().GetId()
	// Create a second volume.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolume().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId1)
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
	publishReq1 := testNodePublishVolumeRequest(volumeId1, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq1)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId1, publishReq1.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that mounting the second volume at the same target path will fail.
	publishReq2 := testNodePublishVolumeRequest(volumeId2, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq2)
	if !grpcErrorEqual(err, ErrTargetPathNotEmpty) {
		t.Fatal(err)
	}
}

func TestNodePublishVolume_MountVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
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
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Check that calling NodePublishVolume with the same
	// parameters succeeds and doesn't mount anything new at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	publishReq = testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent publish to not mount anything new at targetPath.")
	}
}

func TestNodePublishVolume_MountVolume_TargetPathOccupied(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolume().GetId()
	// Create a second volume that we'll try to publish to the same targetPath.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolume().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId1)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq1 := testNodePublishVolumeRequest(volumeId1, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId1, publishReq1.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq1.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq1.TargetPath)
	}
	publishReq2 := testNodePublishVolumeRequest(volumeId2, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq2)
	if err == nil {
		req := testNodeUnpublishVolumeRequest(volumeId2, publishReq2.TargetPath)
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("Expected operation to fail")
	}
}

func TestNodeUnpublishVolume_BlockVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
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
	// Unpublish the volume.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Check that calling NodeUnpublishVolume with the same
	// parameters succeeds and doesn't modify the mounts at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume again to check that it is idempotent.
	unpublishReq = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent unpublish to not modify mountpoints at targetPath.")
	}
}

func TestNodeUnpublishVolume_MountVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
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
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
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
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Unpublish the volume.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Unpublish the volume again to check that it is idempotent.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	unpublishReq = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent unpublish to not modify mountpoints at targetPath.")
	}
}

func targetPathIsMountPoint(path string) bool {
	mp, err := getMountAt(path)
	if err != nil {
		panic(err)
	}
	if mp == nil {
		return false
	}
	return true
}

func TestNodeGetIdNotSupported(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := &csi.NodeGetIdRequest{}
	_, err := client.NodeGetId(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func testProbeRequest() *csi.ProbeRequest {
	req := &csi.ProbeRequest{}
	return req
}

func TestProbe(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testProbeRequest()
	_, err := client.Probe(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes_WithTag(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	tag := "blue"
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tag))
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := vg.Tags()
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{tag}
	if !reflect.DeepEqual(tags, expected) {
		t.Fatalf("Expected tags not found %v != %v", expected, tags)
	}
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes_WithMalformedTag(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	tag := "-some-malformed-tag"
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tag))
	defer clean()
	experr := fmt.Sprintf("Invalid tag '%v': err=%v",
		tag,
		"lvm: Tag must consist of only [A-Za-z0-9_+.-] and cannot start with a '-'")
	err := server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_BusyPhysicalVolume(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	// Format and mount loop1 so it appears busy.
	if err := formatDevice(pv1name, "xfs"); err != nil {
		t.Fatal(err)
	}
	targetPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	if merr := syscall.Mount(pv1name, targetPath, "xfs", 0, ""); merr != nil {
		t.Fatal(merr)
	}
	defer func() {
		if merr := syscall.Unmount(targetPath, 0); merr != nil {
			t.Fatal(merr)
		}
	}()
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := fmt.Sprintf(
		"Cannot create LVM2 physical volume %s: err=lvm: CreatePhysicalVolume: Can't open %s exclusively.  Mounted filesystem?",
		pv1name,
		pv1name,
	)
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetupNewVolumeGroupFormattedPhysicalVolume(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	if err := exec.Command("mkfs", "-t", "xfs", pv2name).Run(); err != nil {
		t.Fatal(err)
	}
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
}

func TestSetupNewVolumeGroupNewPhysicalVolumes(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames, RemoveVolumeGroup())
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
	vgs, err := lvm.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, vg := range vgs {
		if vg == vgname {
			t.Fatal("Unexpected volume group")
		}
	}
}

func TestSetupExistingVolumeGroup(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pv1, err := lvm.CreatePhysicalVolume(loop1.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv1.Remove()
	pv2, err := lvm.CreatePhysicalVolume(loop2.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv2.Remove()
	pvs := []*lvm.PhysicalVolume{pv1, pv2}
	vgname := "test-vg-" + uuid.New().String()
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
}

func testNodeGetCapabilitiesRequest() *csi.NodeGetCapabilitiesRequest {
	req := &csi.NodeGetCapabilitiesRequest{}
	return req
}

func TestNodeGetCapabilities(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testNodeGetCapabilitiesRequest()
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeGetCapabilitiesRemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
	req := testNodeGetCapabilitiesRequest()
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
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
