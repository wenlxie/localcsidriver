package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/kubernetes-csi/localcsidriver/pkg/backend"
	"github.com/kubernetes-csi/localcsidriver/pkg/config"
	"github.com/kubernetes-csi/localcsidriver/pkg/util"
	"github.com/kubernetes-csi/localcsidriver/pkg/util/pendingoperations"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

const PluginName = "kubernetes.io.csi.local"
const PluginVersion = "0.2.0"
const DefaultVolumeSize = 10 << 30
const LvmBackendSelectionKey = "volume-group"

// Key specified in field "VolumeAttributes" to indicate source path of the volume.
// e.g volume-path=/dev/vg1/lv1
// Users need to manually specify the field for static volumes.
const VolumePathKey = "volume-path"

// TODO: should we make the key more general,
// and convert it on the provisioner side?
const HostnameKey = "kubernetes.io/hostname"

type Server struct {
	pendingOperations pendingoperations.PendingOperations
	backends map[string]backend.StorageBackend
	// Key to select backend when performing volume creation.
	// It should be specified in StorageClass as parameters.
	// For lvm, the key is default to "volume-group",
	// For example, when the server finds "volume-group=vg1",
	// it will find backend in its backends map with key of "vg1".
	backendSelectionKey  string
	defaultVolumeSize    uint64
	supportedFilesystems map[string]string
	// Node name to construct access topology for created volumes
	nodeName string
	mounter  *mount.SafeFormatAndMount
}

// New returns a new Server that will manage multi LVM volume groups.
// The Setup method must be called before any other further method calls
// are performed in order to keep backends (volume groups for LVM) up-to-date.
func New(config *config.DriverConfig) (*Server, error) {
	s := &Server{
		pendingOperations:    pendingoperations.New(),
		defaultVolumeSize:    config.DefaultVolumeSize,
		supportedFilesystems: map[string]string{},
		backends:             map[string]backend.StorageBackend{},
		mounter:              &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()},
	}

	// Try to get node name from env if not set.
	s.nodeName = os.Getenv("KUBE_NODE_NAME")
	if s.nodeName == "" {
		// Take hostname as node name if it's not set via env.
		// This way, users must ensure that node labels of CO side is same to hostname.
		var err error
		s.nodeName, err = os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("failed to get hostname of the node: %v", err)
		}
	}

	supportedFilesystems := strings.Split(config.SupportedFilesystems, ",")
	for _, filesystem := range supportedFilesystems {
		s.supportedFilesystems[filesystem] = filesystem
	}

	// Take the first one as default filesystem.
	s.supportedFilesystems[""] = supportedFilesystems[0]

	switch config.BackendType {
	case "lvm":
		s.backendSelectionKey = LvmBackendSelectionKey
		for index, group := range config.LvmConfig {
			lvmBackend, err := backend.NewLvmBackend(group)
			if err != nil {
				return nil, fmt.Errorf("failed to create backend for volume group %v: %v", group, err)
			}
			s.backends[group.Name] = lvmBackend

			// Take the first one as default backend.
			if index == 0 {
				s.backends[""] = lvmBackend
			}
		}
	default:
		return nil, fmt.Errorf("%s not supported for now", config.BackendType)
	}

	log.Printf("NewServer: %v", s)

	return s, nil
}

// Setup calls sync func of the backends in server,
// to keep things up-to-date.
func (s *Server) Setup(stopCh chan struct{}) error {
	for index, storageBackend := range s.backends {
		if index == "" {
			// Default backend should have been synced, skip it.
			continue
		}
		if err := storageBackend.Sync(); err != nil {
			return fmt.Errorf("error syncing %s: %v", storageBackend.Name(), err)
		}
	}

	go func() {
		ticker := time.NewTicker(120 * time.Second) // TODO: make this configurable ?
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				for index, storageBackend := range s.backends {
					if index == "" {
						// Default backend should have been synced, skip it.
						continue
					}
					if err := storageBackend.Sync(); err != nil {
						log.Printf("Error syncing %s: %v", storageBackend.Name(), err)
					}
				}
			}
		}
	}()

	return nil
}

// IdentityService RPCs

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	if err := s.validateGetPluginInfoRequest(request); err != nil {
		return nil, err
	}
	response := &csi.GetPluginInfoResponse{
		Name:          PluginName,
		VendorVersion: PluginVersion,
		Manifest:      nil,
	}

	return response, nil
}

func (s *Server) GetPluginCapabilities(
	ctx context.Context,
	request *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	if err := s.validateGetPluginCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	response := &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}
	return response, nil
}

// Probe is currently a no-op.
// TODO: consider calling Sync func here if Probe is used by upper components.
func (s *Server) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if err := s.validateProbeRequest(request); err != nil {
		return nil, err
	}

	return &csi.ProbeResponse{}, nil
}

// ControllerService RPCs

var ErrVolumeAlreadyExists = status.Error(codes.AlreadyExists, "The volume already exists")
var ErrInsufficientCapacity = status.Error(codes.OutOfRange, "Not enough free space")

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := s.validateCreateVolumeRequest(request); err != nil {
		return nil, err
	}

	// Get storage backend via the key specified in parameters.
	storageBackend, err := s.getBackendFromParams(request)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error getting storage backend: %v",
			err)
	}

	volumeId := storageBackend.Name() + "_" + request.GetName()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(volumeId); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(volumeId, existingOp.Type)
	}

	createOperation := func() error {
		// Check whether a volume with the given name already exists.
		log.Printf("Determining whether volume with id=%v already exists", volumeId)

		vol, err := storageBackend.LookupVolume(volumeId)
		if err == nil {
			log.Printf("Volume %s already exists.", request.GetName())
			// The volume already exists. Determine whether or not the
			// existing volume satisfies the request. If so, return a
			// successful response. If not, return ErrVolumeAlreadyExists.
			if err := s.validateExistingVolume(vol, request); err != nil {
				return err
			}

			return nil
		}
		if !isVolumeNotFoundError(err) {
			// Get unexpected error
			return status.Errorf(
				codes.Internal,
				"Error getting volume: %v",
				err)
		}

		log.Printf("Volume with id=%v does not already exist", volumeId)
		// Determine the capacity.
		size := s.defaultVolumeSize
		if capacityRange := request.GetCapacityRange(); capacityRange != nil {
			bytesFree, err := storageBackend.BytesFree()
			if err != nil {
				return status.Errorf(
					codes.Internal,
					"Error in BytesFree: %v",
					err)
			}
			log.Printf("BytesFree: %v", bytesFree)

			// Round up to GiB
			sizeInRequest := uint64(volumeutil.RoundUpSize(capacityRange.GetRequiredBytes(), volumeutil.GIB)*volumeutil.GIB)
			if sizeInRequest >= size {
				size = sizeInRequest
			}
			// Check whether there is enough free space available.
			if bytesFree < size {
				return ErrInsufficientCapacity
			}
		}
		log.Printf("Creating volume id=%v, size=%v", volumeId, size)
		_, err = storageBackend.CreateVolume(volumeId, size)
		if err != nil {
			return status.Errorf(
				codes.Internal,
				"Error in CreateVolume: %v",
				err)
		}

		return nil
	}

	// Perform create operation.
	if err := s.pendingOperations.Run(volumeId, &pendingoperations.Operation{
		OperationFunc: createOperation,
		Type: pendingoperations.CreateOperation}); err != nil {
			return nil, err
	}

	vol, err := storageBackend.LookupVolume(volumeId)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error getting volume: %v",
			err)
	}

	volPath, err := vol.Path()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in getting volume path: %v",
			err)
	}

	response := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: int64(vol.SizeInBytes()),
			Id:            vol.Name(),
			Attributes: map[string]string{
				VolumePathKey: volPath,
			},
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						HostnameKey: s.nodeName,
					},
				},
			},
		},
	}
	return response, nil
}

func (s *Server) validateExistingVolume(vol backend.Volume, request *csi.CreateVolumeRequest) error {
	// Determine whether the existing volume satisfies the capacity_range
	// of the current request.
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		// If required_bytes is specified, is that requirement
		// satisfied by the existing volume?
		if requiredBytes := capacityRange.GetRequiredBytes(); requiredBytes != 0 {
			if requiredBytes > int64(vol.SizeInBytes()) {
				log.Printf("Existing volume does not satisfy request: required_bytes > volume size (%d > %d)", requiredBytes, vol.SizeInBytes())
				// The existing volume is not big enough.
				return ErrVolumeAlreadyExists
			}
		}
		if limitBytes := capacityRange.GetLimitBytes(); limitBytes != 0 {
			if limitBytes < int64(vol.SizeInBytes()) {
				log.Printf("Existing volume does not satisfy request: limit_bytes < volume size (%d < %d)", limitBytes, vol.SizeInBytes())
				// The existing volume is too big.
				return ErrVolumeAlreadyExists
			}
		}
		// We know that one of limit_bytes or required_bytes was
		// specified, thanks to the specification and the request
		// validation logic.
	}
	// The existing volume matches the requested capacity_range. We
	// determine whether the existing volume satisfies all requested
	// volume_capabilities.
	sourcePath, err := vol.Path()
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Volume path is %v", sourcePath)
	existingFsType, err := util.GetDiskFormat(sourcePath)
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: %v",
			err)
	}
	log.Printf("Existing filesystem type is '%v'", existingFsType)
	for _, volumeCapability := range request.GetVolumeCapabilities() {
		if mnt := volumeCapability.GetMount(); mnt != nil {
			// This is a MOUNT_VOLUME capability. We know that the
			// requested filesystem type is supported on this host
			// thanks to the request validation logic.
			if existingFsType != "" {
				// The volume has already been formatted with
				// some filesystem. If the requested
				// volume_capability.fs_type is different to
				// the filesystem already on the volume, then
				// this volume_capability is unsatisfiable
				// using the existing volume and we return an
				// error.
				requestedFstype := mnt.GetFsType()
				if requestedFstype != "" && requestedFstype != existingFsType {
					// The existing volume is already
					// formatted with a filesystem that
					// does not match the requested
					// volume_capability so it does not
					// satisfy the request.
					log.Printf("Existing volume does not satisfy request: fs_type != volume fs (%v != %v)", requestedFstype, existingFsType)
					return ErrVolumeAlreadyExists
				}
				// The existing volume satisfies this
				// volume_capability.
			} else {
				// The existing volume has not been formatted
				// with a filesystem and can therefore satisfy
				// this volume_capability (by formatting it
				// with the specified fs_type, whatever it is).
			}
			// We ignore whether or not the volume_capability
			// specifies readonly as any filesystem can be mounted
			// readonly or not depending on how it gets published.
		}
	}
	return nil
}

var ErrVolumeNotFound = status.Error(codes.NotFound, "The volume does not exist.")

func (s *Server) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if err := s.validateDeleteVolumeRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(id); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(id, existingOp.Type)
	}

	deleteOperation := func() error {
		// Get Backend via volume ID
		storageBackend, err := s.getBackendFromVolumeID(id)
		if err != nil {
			return status.Errorf(
				codes.Internal,
				"Error getting storage backend: %v",
				err)
		}

		log.Printf("Looking up volume with id=%v", id)
		vol, err := storageBackend.LookupVolume(id)
		if isVolumeNotFoundError(err) {
			// Volume has already been removed
			return nil
		} else if err != nil {
			return status.Errorf(
				codes.Internal,
				"Error getting volume: %v",
				err)
		}
		/*
			log.Printf("Determining volume path")
			path, err := vol.Path()
			if err != nil {
				return nil, status.Errorf(
					codes.Internal,
					"Error in Path(): %v",
					err)
			}
			log.Printf("Cleaning up data on device %v", path)
			if err := util.CleanupDataOnDevice(path); err != nil {
				return nil, status.Errorf(
					codes.Internal,
					"Cannot cleanup data from device: %v",
					err)
			}
		*/
		log.Printf("Removing volume")
		if err := storageBackend.DeleteVolume(vol.Name()); err != nil {
			return status.Errorf(
				codes.Internal,
				"Failed to remove volume: %v",
				err)
		}

		return nil
	}

	// Perform delete operation.
	if err := s.pendingOperations.Run(id, &pendingoperations.Operation{
		OperationFunc: deleteOperation,
		Type: pendingoperations.DeleteOperation}); err != nil {
		return nil, err
	}

	response := &csi.DeleteVolumeResponse{}
	return response, nil
}

var ErrCallNotImplemented = status.Error(codes.Unimplemented, "That RPC is not implemented.")

func (s *Server) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Printf("ControllerPublishVolume not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Printf("ControllerUnpublishVolume not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	log.Printf("CreateSnapshot not supported")
	return nil, ErrCallNotImplemented
}
func (s *Server) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	log.Printf("DeleteSnapshot not supported")
	return nil, ErrCallNotImplemented
}
func (s *Server) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	log.Printf("ListSnapshots not supported")
	return nil, ErrCallNotImplemented
}

var ErrMismatchedFilesystemType = status.Error(
	codes.InvalidArgument,
	"The requested fs_type does not match the existing filesystem on the volume.")

func (s *Server) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if err := s.validateValidateVolumeCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()

	// Get Backend via volume ID
	storageBackend, err := s.getBackendFromVolumeID(id)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error getting storage backend: %v",
			err)
	}

	log.Printf("Looking up volume with id=%v", id)
	vol, err := storageBackend.LookupVolume(id)
	if err != nil {
		return nil, ErrVolumeNotFound
	}
	log.Printf("Determining volume path")
	sourcePath, err := vol.Path()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Determining filesystem type at %v", sourcePath)
	existingFstype, err := util.GetDiskFormat(sourcePath)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Printf("Existing filesystem type is '%v'", existingFstype)
	for _, capability := range request.GetVolumeCapabilities() {
		if mnt := capability.GetMount(); mnt != nil {
			if existingFstype != "" {
				// The volume has already been formatted.
				if mnt.GetFsType() != "" && existingFstype != mnt.GetFsType() {
					// The requested fstype does not match the existing one.
					return nil, ErrMismatchedFilesystemType
				}
			}
		}
	}
	response := &csi.ValidateVolumeCapabilitiesResponse{
		Supported: true,
		Message:   "",
	}
	return response, nil
}

// The interface has not way to specify backend (volume group) in request,
// we'll return the volumes of the default group for now.
// TODO: revisit the part when we really need the interface.
func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if err := s.validateListVolumesRequest(request); err != nil {
		return nil, err
	}

	storageBackend := s.backends[""]

	volnames, err := storageBackend.ListVolumeNames()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot list volume names: err=%v",
			err)
	}
	var entries []*csi.ListVolumesResponse_Entry
	for _, volname := range volnames {
		log.Printf("Looking up volume '%v'", volname)
		vol, err := storageBackend.LookupVolume(volname)
		if err != nil {
			return nil, ErrVolumeNotFound
		}

		volPath, err := vol.Path()
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Error getting volume path: %v",
				err)
		}

		info := &csi.Volume{
			CapacityBytes: int64(vol.SizeInBytes()),
			Id:            vol.Name(),
			Attributes: map[string]string{
				VolumePathKey: volPath,
			},
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						HostnameKey: s.nodeName,
					},
				},
			},
		}
		log.Printf("Found volume %v (%v bytes)", volname, vol.SizeInBytes())
		entry := &csi.ListVolumesResponse_Entry{Volume: info}
		entries = append(entries, entry)
	}
	response := &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: "",
	}
	return response, nil
}

// We'll return total capacity for now.
// TODO: consider update CSI interface to return both available and free capacity.
func (s *Server) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	if err := s.validateGetCapacityRequest(request); err != nil {
		return nil, err
	}

	for _, volumeCapability := range request.GetVolumeCapabilities() {
		// Check for unsupported filesystem type in order to return 0
		// capacity if it isn't supported.
		if mnt := volumeCapability.GetMount(); mnt != nil {
			// This is a MOUNT_VOLUME request.
			fstype := mnt.GetFsType()
			if _, ok := s.supportedFilesystems[fstype]; !ok {
				// Zero capacity for unsupported filesystem type.
				response := &csi.GetCapacityResponse{AvailableCapacity: 0}
				return response, nil
			}
		}
	}

	// Get storage  backend via the key specified in parameters.
	storageBackend, err := s.getBackendFromParams(request)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error getting storage backend: %v",
			err)
	}

	bytesToal, err := storageBackend.BytesTotal()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in bytesToal: %v",
			err)
	}
	log.Printf("BytesToal: %v", bytesToal)
	response := &csi.GetCapacityResponse{AvailableCapacity: int64(bytesToal)}
	return response, nil
}

func (s *Server) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if err := s.validateControllerGetCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	capabilities := []*csi.ControllerServiceCapability{
		// CREATE_DELETE_VOLUME
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		// PUBLISH_UNPUBLISH_VOLUME
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		// GET_CAPACITY
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
	}
	response := &csi.ControllerGetCapabilitiesResponse{Capabilities: capabilities}
	return response, nil
}

// NodeService RPCs

func (s *Server) NodeStageVolume(
	ctx context.Context,
	request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if err := s.validateNodeStageVolumeRequest(request); err != nil {
		return nil, err
	}

	id := request.GetVolumeId()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(id); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(id, existingOp.Type)
	}

	stageOperation := func() error {
		// As path of static volumes cannot be found via storage backend,
		// we'll need to specify path as volume attributes.
		volPath := request.GetVolumeAttributes()[VolumePathKey]
		targetPath := request.GetStagingTargetPath()
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		mountFlags := request.GetVolumeCapability().GetMount().GetMountFlags()

		fsType := ""
		if mnt := request.GetVolumeCapability().GetMount(); mnt != nil {
			fsType = mnt.GetFsType()
		}
		if fsType == "" {
			fsType = s.supportedFilesystems[""]
		}

		switch accessType := request.GetVolumeCapability().GetAccessType().(type) {
		case *csi.VolumeCapability_Block:
			if err := s.doSymlink(volPath, targetPath); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		case *csi.VolumeCapability_Mount:
			if err := s.doMount(volPath, targetPath, fsType, readonly, mountFlags); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		default:
			return status.Errorf(
				codes.OutOfRange,
				"unknown access_type: %v",
				accessType,
			)
		}
		return nil
	}

	// Perform stage operation.
	if err := s.pendingOperations.Run(id, &pendingoperations.Operation{
		OperationFunc: stageOperation,
		Type: pendingoperations.NodeStageOperation}); err != nil {
		return nil, err
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *Server) NodeUnstageVolume(
	ctx context.Context,
	request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if err := s.validateNodeUnstageVolumeRequest(request); err != nil {
		return nil, err
	}

	id := request.GetVolumeId()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(id); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(id, existingOp.Type)
	}

	unstageOperation := func() error {
		if err := s.doUnmount(request.GetStagingTargetPath()); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		return nil
	}

	// Perform unstage operation.
	if err := s.pendingOperations.Run(id, &pendingoperations.Operation{
		OperationFunc: unstageOperation,
		Type: pendingoperations.NodeUnstageOperation}); err != nil {
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

var ErrTargetPathNotEmpty = status.Error(
	codes.InvalidArgument,
	"Unexpected device already mounted at targetPath.")

var ErrTargetPathRO = status.Error(
	codes.InvalidArgument,
	"The targetPath is already mounted readonly.")

var ErrTargetPathRW = status.Error(
	codes.InvalidArgument,
	"The targetPath is already mounted read-write.")

func (s *Server) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if err := s.validateNodePublishVolumeRequest(request); err != nil {
		return nil, err
	}

	id := request.GetVolumeId()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(id); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(id, existingOp.Type)
	}

	publishOperation := func() error {
		sourcePath := request.GetStagingTargetPath()
		targetPath := request.GetTargetPath()
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		readonly = readonly || request.GetReadonly()
		mountFlags := request.GetVolumeCapability().GetMount().GetMountFlags()

		switch accessType := request.GetVolumeCapability().GetAccessType().(type) {
		case *csi.VolumeCapability_Block:
			if err := s.doSymlink(sourcePath, targetPath); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		case *csi.VolumeCapability_Mount:
			// We'll only need bind mount for NodePublishVolume,
			// so don't care about fstype here.
			if err := s.doMount(sourcePath, targetPath, "", readonly, mountFlags); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		default:
			return status.Errorf(
				codes.OutOfRange,
				"unknown access_type: %v",
				accessType,
			)
		}

		return nil
	}

	// Perform publish operation.
	if err := s.pendingOperations.Run(id, &pendingoperations.Operation{
		OperationFunc: publishOperation,
		Type: pendingoperations.NodePublishOperation}); err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *Server) doMount(sourcePath, targetPath, fsType string, readonly bool, mountFlags []string) error {
	notMnt, err := s.mounter.IsNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return fmt.Errorf("error creating target path: %v", err)
			}
			notMnt = true
		} else {
			return fmt.Errorf("error validating target path: %v", err)
		}
	}

	if !notMnt {
		return nil
	}

	options := mountFlags
	if readonly {
		options = append(options, "ro")
	}

	// Mount
	volumeType, err := s.mounter.GetFileType(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to get volume type: %v", err)
	}

	switch volumeType {
	case mount.FileTypeBlockDev:
		err = s.mounter.FormatAndMount(sourcePath, targetPath, fsType, options)
	case mount.FileTypeFile, mount.FileTypeDirectory:
		options = append([]string{"bind"}, options...)
		err = s.mounter.Mount(sourcePath, targetPath, "", options)
	default:
		err = fmt.Errorf("unsupported volume source type: %v", volumeType)
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) doSymlink(sourcePath, linkFile string) error {
	mapPath := filepath.Dir(linkFile)
	if !filepath.IsAbs(mapPath) {
		return fmt.Errorf("The map path should be absolute: map path: %s", mapPath)
	}

	// Check and create mapPath
	_, err := os.Stat(mapPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.MkdirAll(mapPath, 0750); err != nil {
		return fmt.Errorf("failed to mkdir %s: %v", mapPath, err)
	}
	// Remove old symbolic link(or file) then create new one.
	// This should be done because current symbolic link is
	// stale across node reboot.
	if err = os.Remove(linkFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	err = os.Symlink(sourcePath, linkFile)
	return err
}

func (s *Server) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if err := s.validateNodeUnpublishVolumeRequest(request); err != nil {
		return nil, err
	}

	id := request.GetVolumeId()

	// Check if there is pending operation for the volume.
	if existingOp, opExist := s.pendingOperations.HasPendingOperation(id); opExist {
		return nil, pendingoperations.NewAlreadyExistsError(id, existingOp.Type)
	}

	unpublishOperation := func() error {
		err := s.doUnmount(request.GetTargetPath())
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		return nil
	}

	// Perform unpublish operation.
	if err := s.pendingOperations.Run(id, &pendingoperations.Operation{
		OperationFunc: unpublishOperation,
		Type: pendingoperations.NodeUnpublishOperation}); err != nil {
		return nil, err
	}

	response := &csi.NodeUnpublishVolumeResponse{}
	return response, nil
}

func (s *Server) NodeGetId(
	ctx context.Context,
	request *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	log.Printf("NodeGetId not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	log.Printf("NodeGetInfo not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if err := s.validateNodeGetCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	response := &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}
	return response, nil
}

// CSI Requests with parameters specified. e.g CreateVolumeRequest/GetCapacityRequest
// the server can get storage backend form it.
type RequestWithParameters interface {
	GetParameters() map[string]string
}

func (s *Server) getBackendFromParams(request RequestWithParameters) (backend.StorageBackend, error) {
	backendKey := ""
	params := request.GetParameters()
	if params != nil {
		backendKey = params[s.backendSelectionKey]
	}

	storageBackend := s.backends[backendKey]
	if storageBackend == nil {
		return nil, fmt.Errorf("driver cannot handle request from %s", backendKey)
	}

	return storageBackend, nil
}

func (s *Server) getBackendFromVolumeID(id string) (backend.StorageBackend, error) {
	backendKey, err := backendKeyFromVolumeID(id)
	if err != nil {
		return nil, err
	}

	storageBackend := s.backends[backendKey]
	if storageBackend == nil {
		return nil, fmt.Errorf("driver cannot handle request from %s", backendKey)
	}

	return storageBackend, nil
}

// backendKeyFromVolumeID convert given volume ID into storage backend name and internal ID.
func backendKeyFromVolumeID(inputID string) (backend string, err error) {
	volumeInfo := strings.Split(inputID, "_")
	if len(volumeInfo) < 2 {
		return "", fmt.Errorf("input volume ID is not in format of '<backend>_<ID>' but %s", inputID)
	}

	return volumeInfo[0], nil
}

// doUnmount tries to unmount given path,
// and remove the path directly if it's not a mountpoint.
func (s *Server) doUnmount(mountPath string) error {
	pathExists, pathErr := volumeutil.PathExists(mountPath)
	if !pathExists {
		log.Printf("Unmount skipped because path does not exist: %v", mountPath)
		return nil
	}
	corruptedMnt := isCorruptedMnt(pathErr)
	if pathErr != nil && !corruptedMnt {
		return fmt.Errorf("Error checking path: %v", pathErr)
	}

	if !corruptedMnt {
		var notMnt bool
		var err error
		notMnt, err = s.mounter.IsNotMountPoint(mountPath)

		if err != nil {
			return err
		}

		if notMnt {
			// This can happen when the path is of block.
			log.Printf("%s is not a mountpoint, deleting", mountPath)
			return os.RemoveAll(mountPath)
		}
	}

	// Unmount the mount path
	if err := s.mounter.Unmount(mountPath); err != nil {
		return err
	}
	notMnt, mntErr := s.mounter.IsLikelyNotMountPoint(mountPath)
	if mntErr != nil {
		return mntErr
	}
	if notMnt {
		return os.RemoveAll(mountPath)
	}
	return fmt.Errorf("failed to unmount path %v", mountPath)
}

// isCorruptedMnt return true if err is about corrupted mount point
func isCorruptedMnt(err error) bool {
	if err == nil {
		return false
	}
	var underlyingError error
	switch pe := err.(type) {
	case nil:
		return false
	case *os.PathError:
		underlyingError = pe.Err
	case *os.LinkError:
		underlyingError = pe.Err
	case *os.SyscallError:
		underlyingError = pe.Err
	}
	return underlyingError == syscall.ENOTCONN || underlyingError == syscall.ESTALE
}

// volume not found error that returned from the backends should follow the format.
func isVolumeNotFoundError(err error) bool {
	suffix := "volume not found"
	if err != nil && strings.HasSuffix(err.Error(), suffix) {
		return true
	}

	return false
}
