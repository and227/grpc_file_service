package app

import (
	"fmt"
	"net"
	"os"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"

	pb "file_service/pkg/file_proto"

	config "file_service/config/server"

	"file_service/internal/server/service"
)

var (
	filesStorage string
)

func init() {
	filesStorage = os.Getenv("FILES_STORAGE")
	if filesStorage == "" {
		log.Fatal("Empty 'FILES_STORAGE'")
	}
	fmt.Printf("filesStorage: %s\n", filesStorage)

}

func Run() {
	serverSock, err := net.Listen("tcp", config.GRPC_ADDRESS)
	if err != nil {
		log.Errorf("Cannot open port for listening: %s (%e)", config.GRPC_ADDRESS, err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	logger := log.New()
	logger.SetLevel(log.DebugLevel)
	logger.SetFormatter(&log.JSONFormatter{})
	pb.RegisterFileServiceServer(
		grpcServer,
		service.NewFileGRPCServer(
			logger,
			filesStorage,
			map[string]int{
				"download":  config.DOWNLOAD_LIMIT,
				"upload":    config.UPLOAD_LIMIT,
				"read_list": config.READ_LIST_LIMIT,
			},
		),
	)
	log.Infof("Run grpc server on %s\n", config.GRPC_ADDRESS)
	log.Fatalf("Erro during grpc server: %s", grpcServer.Serve(serverSock))
}
