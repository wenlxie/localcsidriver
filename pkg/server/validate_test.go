package server

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"google.golang.org/grpc/status"
)

// IdentityService RPCs

// ...

// ControllerService RPCs

func TestCreateVolumeMissingName(t *testing.T) {
	req := testCreateVolumeRequest()
	req.Name = ""
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingName) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingVolumeCapabilities(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities = nil
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapabilities) {
		t.Fatal(err)
	}
}

/*
func TestCreateVolumeMissingVolumeCapabilitiesAccessType(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessType = nil
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}
*/

func TestCreateVolumeMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode = nil
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeUnsupported(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrUnsupportedAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeInvalid(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = 1000
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrInvalidAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesReadonlyBlock(t *testing.T) {
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrBlockVolNoRO) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesCapacityRangeRequiredLessThanLimit(t *testing.T) {
	req := testCreateVolumeRequest()
	req.CapacityRange.RequiredBytes = 1000
	req.CapacityRange.LimitBytes = req.CapacityRange.RequiredBytes - 1
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrCapacityRangeInvalidSize) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesCapacityRangeUnspecified(t *testing.T) {
	req := testCreateVolumeRequest()
	req.CapacityRange.RequiredBytes = 0
	req.CapacityRange.LimitBytes = 0
	err := testServer().validateCreateVolumeRequest(req)
	if !grpcErrorEqual(err, ErrCapacityRangeUnspecified) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeMissingVolumeId(t *testing.T) {
	req := testDeleteVolumeRequest("test-volume")
	req.VolumeId = ""
	err := testServer().validateDeleteVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeId(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeId = ""
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilities(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities = nil
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapabilities) {
		t.Fatal(err)
	}
}

/*
func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessType(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessType = nil
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}
*/

func TestValidateVolumeCapabilitiesNodeUnpublishVolume_MountVolume_BadFilesystem(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "ext4", nil)
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrUnsupportedFilesystem) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessMode = nil
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	err := testServer().validateValidateVolumeCapabilitiesRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestGetCapacityBadFilesystem(t *testing.T) {
	req := testGetCapacityRequest("ext4")
	err := testServer().validateGetCapacityRequest(req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetCapacityMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode = nil
	err := testServer().validateGetCapacityRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestGetCapacityVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	err := testServer().validateGetCapacityRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

// NodeService RPCs

var fakeDevicePath = "/dev/device1"
var fakeStagingPath = "/run/mnt/global"
var fakeMountDir = "/run/localvolume/mnt"

func TestNodePublishVolumeMissingVolumeId(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.VolumeId = ""
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumePresentPublishVolumeInfo(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.PublishInfo = map[string]string{"foo": "bar"}
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrSpecifiedPublishInfo) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingTargetPath(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.TargetPath = ""
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingStagingTargetPath(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.StagingTargetPath = ""
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingStagingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeCapability(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.VolumeCapability = nil
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapability) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeCapabilityAccessMode(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.VolumeCapability.AccessMode = nil
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeVolumeCapabilityAccessModeUNKNOWN(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "", nil)
	req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeBadFilesystem(t *testing.T) {
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, fakeStagingPath, "ext4", nil)
	err := testServer().validateNodePublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrUnsupportedFilesystem) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeMissingVolumeId(t *testing.T) {
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeMountDir)
	req.VolumeId = ""
	err := testServer().validateNodeUnpublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeMissingTargetPath(t *testing.T) {
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeMountDir)
	req.TargetPath = ""
	err := testServer().validateNodeUnpublishVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingVolumeId(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeId = ""
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumePresentPublishVolumeInfo(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.PublishInfo = map[string]string{"foo": "bar"}
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrSpecifiedPublishInfo) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingStagingTargetPath(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.StagingTargetPath = ""
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingStagingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingVolumeAttributes(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeAttributes = nil
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeAttributes) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingVolumePath(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeAttributes = map[string]string{}
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumePath) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingVolumeCapability(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeCapability = nil
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapability) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeMissingVolumeCapabilityAccessMode(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeCapability.AccessMode = nil
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeVolumeCapabilityAccessModeUNKNOWN(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "", nil)
	req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestNodeStageVolumeBadFilesystem(t *testing.T) {
	req := testNodeStageVolumeRequest("fake_volume_id", fakeDevicePath, fakeStagingPath, "ext4", nil)
	err := testServer().validateNodeStageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrUnsupportedFilesystem) {
		t.Fatal(err)
	}
}

func TestNodeUnstageVolumeMissingVolumeId(t *testing.T) {
	req := testNodeUnstageVolumeRequest("fake_volume_id", fakeStagingPath)
	req.VolumeId = ""
	err := testServer().validateNodeUnstageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodeUnstageVolumeMissingStagingTargetPath(t *testing.T) {
	req := testNodeUnstageVolumeRequest("fake_volume_id", fakeStagingPath)
	req.StagingTargetPath = ""
	err := testServer().validateNodeUnstageVolumeRequest(req)
	if !grpcErrorEqual(err, ErrMissingStagingTargetPath) {
		t.Fatal(err)
	}
}

func grpcErrorEqual(gotErr, expErr error) bool {
	got, ok := status.FromError(gotErr)
	if !ok {
		return false
	}
	exp, ok := status.FromError(expErr)
	if !ok {
		return false
	}
	return got.Code() == exp.Code() && got.Message() == exp.Message()
}

func testServer() *Server {
	return &Server{
		supportedFilesystems: map[string]string{
			"xfs": "xfs",
			"": "xfs",
		},
	}
}

