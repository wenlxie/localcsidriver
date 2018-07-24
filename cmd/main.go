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
	socketFileF := flag.String("unix-addr", "/run/csi/local-driver.sock", "The path to the listening unix socket file")
	socketFileEnvF := flag.String("unix-addr-env", "", "An optional environment variable from which to read the unix-addr")
	flag.Parse()
	// Setup logging
	logprefix := fmt.Sprintf("[%s]", "CSI local driver")
	logflags := log.LstdFlags | log.Lshortfile
	logger := log.New(os.Stderr, logprefix, logflags)
	server.SetLogger(logger)
	lvm.SetLogger(logger)
	// Determine listen address.
	if *socketFileF != "" && *socketFileEnvF != "" {
		log.Fatalf("Cannot specify -unix-addr and -unix-addr-env")
	}
	sock := *socketFileF
	if *socketFileEnvF != "" {
		sock = os.Getenv(*socketFileEnvF)
	}
	if strings.HasPrefix(sock, "unix://") {
		sock = sock[len("unix://"):]
	}
	// Setup socket listener
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
