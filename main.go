package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "file_service/file_proto"
)

type FileGRPCServer struct {
	pb.UnimplementedFileServiceServer
	mutex         sync.Mutex
	l             *log.Logger
	downloadCount int32
	readListCount int32
	mu            sync.Mutex
}

// type rateLimitInterceptor struct {
// 	requestCount int32
// 	limitValue   int32
// }

// func (limiter *rateLimitInterceptor) Inc() {
// 	atomic.AddInt32(&limiter.requestCount, 1)
// }

// func (limiter *rateLimitInterceptor) Dec() {
// 	atomic.AddInt32(&limiter.requestCount, -1)
// }

// func (limiter rateLimitInterceptor) IsLimited() bool {
// 	return limiter.requestCount > limiter.limitValue
// }

// func (limiter *rateLimitInterceptor) Limit() bool {
// 	limiter.Inc()
// 	defer limiter.Dec()

// 	return limiter.IsLimited()
// }

var (
	downloadLimiter = make(chan struct{}, UP_DOWNLOAD_LIMIT)
	readListLimiter = make(chan struct{}, READ_LIST_LIMIT)
)

func NewFileGRPCServer(l *log.Logger) *FileGRPCServer {
	return &FileGRPCServer{
		l: l,
	}
}

func (g *FileGRPCServer) logError(err error) error {
	if err != nil {
		g.l.Debug(err)
	}
	return err
}

func (g *FileGRPCServer) contextError(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return g.logError(status.Error(codes.Canceled, "request is canceled"))
	case context.DeadlineExceeded:
		return g.logError(status.Error(codes.DeadlineExceeded, "deadline is exceeded"))
	default:
		return nil
	}
}

func (g *FileGRPCServer) Upload(stream pb.FileService_UploadServer) error {
	// atomic.AddInt32(&g.downloadCount, 1)
	// defer atomic.AddInt32(&g.downloadCount, -1)

	// g.mu.Lock()
	// defer g.mu.Unlock()

	// if g.downloadCount > UP_DOWNLOAD_LIMIT {
	// 	return status.Errorf(codes.ResourceExhausted, "Download limit hit")
	// }

	select {
	case downloadLimiter <- struct{}{}:
		defer func() {
			<-downloadLimiter
		}()
	default:
		return status.Errorf(codes.ResourceExhausted, "Download limit hit")
	}

	file := NewFile()
	var fileSize uint32
	fileSize = 0
	defer func() {
		if err := file.OutputFile.Close(); err != nil {
			g.l.Error("Close file error", err)
		}
	}()
	for {
		req, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			return g.logError(status.Error(codes.Internal, "GRPC Error: "+err.Error()))
		}
		if file.FilePath == "" {
			file.SetFile(req.GetFileName(), FILES_STORAGE)
		}
		chunk := req.GetChunk()
		fileSize += uint32(len(chunk))
		g.l.WithFields(log.Fields{
			"size": fileSize,
		}).Debug("received a chunk")
		if err := file.Write(chunk); err != nil {
			g.l.Error("Write file error")
		}
	}
	fileName := filepath.Base(file.FilePath)
	g.l.WithFields(log.Fields{
		"file": fileName,
		"size": fileSize,
	}).Debug("saved file")
	return stream.SendAndClose(&pb.FileUploadResponse{FileName: fileName, Size: fileSize})
}

func (g *FileGRPCServer) Download(request *pb.FileDownloadRequest, stream pb.FileService_DownloadServer) error {
	// atomic.AddInt32(&g.downloadCount, 1)
	// defer atomic.AddInt32(&g.downloadCount, -1)

	// if g.downloadCount > UP_DOWNLOAD_LIMIT {
	// 	return status.Errorf(codes.ResourceExhausted, "Download limit hit")
	// }

	select {
	case downloadLimiter <- struct{}{}:
		defer func() {
			<-downloadLimiter
		}()
	default:
		return status.Errorf(codes.ResourceExhausted, "Download limit hit")
	}

	fileName := request.GetFileName()

	file, err := os.Open(filepath.Join(FILES_STORAGE, fileName))
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	buffer := make([]byte, request.GetChankSize())

	for {
		num, err := file.Read(buffer)

		if err == io.EOF {
			fmt.Println(err)
			break
		}
		if err != nil {
			return nil
		}

		chunk := buffer[:num]

		stream.Send(&pb.FileDownloadResponse{
			Chunk: chunk,
		})
	}

	return nil
}

func statTimes(name string) (atime, mtime, ctime time.Time, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return
	}
	mtime = fi.ModTime()
	stat := fi.Sys().(*syscall.Stat_t)
	atime = time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
	ctime = time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec))
	return
}

func (g *FileGRPCServer) List(ctx context.Context, reqv *pb.FileListRequest) (*pb.FileListResponse, error) {
	// atomic.AddInt32(&g.readListCount, 1)
	// defer atomic.AddInt32(&g.readListCount, -1)

	// fmt.Println("read list count: ", g.readListCount)
	// if g.readListCount > READ_LIST_LIMIT {
	// 	return &pb.FileListResponse{}, status.Errorf(codes.ResourceExhausted, "Read list limit hit")
	// }

	select {
	case readListLimiter <- struct{}{}:
		defer func() {
			<-readListLimiter
		}()
	default:
		return &pb.FileListResponse{}, status.Errorf(codes.ResourceExhausted, "Read list limit hit")
	}

	var files []*pb.FileInfo

	entries, err := os.ReadDir(FILES_STORAGE)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		_, mtime, ctime, err := statTimes(path.Join(FILES_STORAGE, e.Name()))
		if err != nil {
			log.Fatal(err)
		}
		files = append(files, &pb.FileInfo{
			FileName: e.Name(),
			Created:  timestamppb.New(ctime),
			Updated:  timestamppb.New(mtime),
		})
	}

	return &pb.FileListResponse{Files: files}, nil
}

func main() {
	serverSock, err := net.Listen("tcp", GRPC_ADDRESS)
	if err != nil {
		log.Errorf("Cannot open port for listening: %s (%e)", GRPC_ADDRESS, err)
	}

	var opts []grpc.ServerOption
	// limiter := &rateLimitInterceptor{limitValue: UP_DOWNLOAD_LIMIT}

	grpcServer := grpc.NewServer(
		opts...,
	// // init the Ratelimiting middleware
	// grpc_middleware.WithUnaryServerChain(
	// 	grpc_ratelimit.UnaryServerInterceptor(limiter),
	// ),
	// grpc_middleware.WithStreamServerChain(
	// 	grpc_ratelimit.StreamServerInterceptor(limiter),
	// ),
	)
	logger := log.New()
	logger.SetLevel(log.DebugLevel)
	logger.SetFormatter(&log.JSONFormatter{})
	pb.RegisterFileServiceServer(grpcServer, NewFileGRPCServer(logger))
	log.Infof("Run grpc server on %s\n", GRPC_ADDRESS)
	log.Fatalf("Erro during grpc server: %s", grpcServer.Serve(serverSock))
}
