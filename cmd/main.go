package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/kubernetes-csi/localcsidriver/pkg/config"
	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
	"github.com/kubernetes-csi/localcsidriver/pkg/server"
)

func main() {
	// Configure flags
	configPathF := flag.String("config-path", "/etc/config/local-driver.conf", "The file path of dirver config")
	endpointF := flag.String("endpoint", "unix:///var/lib/kubelet/plugins/kubernetes.io.csi.local/csi.sock", "The path to the listening unix socket file")
	flag.Parse()
	// Setup logging
	logprefix := fmt.Sprintf("[%s]", "CSI local driver")
	logflags := log.LstdFlags | log.Lshortfile
	logger := log.New(os.Stderr, logprefix, logflags)
	server.SetLogger(logger)
	lvm.SetLogger(logger)
	// Determine listen address.
	sock := *endpointF
	if strings.HasPrefix(sock, "unix://") {
		sock = sock[len("unix://"):]
	}
	// Setup socket listener
	addr := "/" + sock
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove %s, error: %s", addr, err.Error())
	}
	lis, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	// Setup server
	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts,
		grpc.UnaryInterceptor(
			server.ChainUnaryServer(
				server.LoggingInterceptor(),
			),
		),
	)
	grpcServer := grpc.NewServer(grpcOpts...)

	driverConfig, err := config.LoadDriverConfig(*configPathF)
	if err != nil {
		log.Fatalf("Error loading driver config: %v", err)
	}

	s, err := server.New(driverConfig)
	if err != nil {
		log.Fatalf("Error initializing localvolume plugin: %v", err)
	}

	stopCh := make(chan struct{})

	if err := s.Setup(stopCh); err != nil {
		log.Fatalf("Error initializing localvolume plugin: %v", err)
	}
	csi.RegisterIdentityServer(grpcServer, s)
	csi.RegisterControllerServer(grpcServer, s)
	csi.RegisterNodeServer(grpcServer, s)
	grpcServer.Serve(lis)
}
