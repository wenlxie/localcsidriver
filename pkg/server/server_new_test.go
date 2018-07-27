package server

import (
	"context"
	"io/ioutil"
	stdlog "log"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/google/uuid"
	"github.com/kubernetes-csi/localcsidriver/pkg/cleanup"
	"github.com/kubernetes-csi/localcsidriver/pkg/config"
	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
)

// The size of the source devices we create in our tests.
const devsize = 100 << 20 // 100MiB

func setupServer() (client *Client, cleanupFn func()) {
	vgName := "test-group"

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
		DefaultVolumeSize: 100 << 20,
		LvmConfig: []config.VolumeGroupConfig{
			{
				Name: "test-group",
				DiscoveryDir: discoveryDir,
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
		vg, err := lvm.LookupVolumeGroup(vgName)
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
		vg, err := lvm.LookupVolumeGroup(vgName)
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
	return client, clean.Unwind
}

func testGetPluginInfoRequest() *csi.GetPluginInfoRequest {
	req := &csi.GetPluginInfoRequest{}
	return req
}

func TestGetPluginInfo(t *testing.T) {
	client, clean := setupServer()
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
