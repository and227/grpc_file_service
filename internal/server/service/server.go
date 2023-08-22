package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	pb "file_service/pkg/file_proto"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LimiterType int32

const (
	DOWNLOAD_LIMITER LimiterType = iota
	UPLOAD_LIMITER
	READ_LIST_LIMITER
)

const (
	DEFAULT_DOWNLOAD_LIMIT  = 10
	DEFAULT_UPLOAD_LIMIT    = 10
	DEFAULT_READ_LIST_LIMIT = 100
)

type void struct{}

type FileGRPCServer struct {
	pb.UnimplementedFileServiceServer
	logger       *log.Logger
	filesStorage string
	limiters     map[LimiterType]chan void
}

func GetOrDefault(m map[string]int, key string, default_ int) int {
	val, ex := m[key]
	if !ex {
		val = default_
	}
	return val
}

func NewFileGRPCServer(logger *log.Logger, filesStorage string, limits map[string]int) *FileGRPCServer {
	downloadLimit := GetOrDefault(limits, "download", DEFAULT_DOWNLOAD_LIMIT)
	uploadLimit := GetOrDefault(limits, "upload", DEFAULT_UPLOAD_LIMIT)
	readListLimit := GetOrDefault(limits, "read_list", DEFAULT_READ_LIST_LIMIT)
	limiters := map[LimiterType]chan void{
		DOWNLOAD_LIMITER:  make(chan void, downloadLimit),
		UPLOAD_LIMITER:    make(chan void, uploadLimit),
		READ_LIST_LIMITER: make(chan void, readListLimit),
	}
	return &FileGRPCServer{
		logger:       logger,
		filesStorage: filesStorage,
		limiters:     limiters,
	}
}

func (s *FileGRPCServer) Upload(stream pb.FileService_UploadServer) error {
	limiter := s.limiters[UPLOAD_LIMITER]
	select {
	case limiter <- void{}:
		defer func() {
			<-limiter
		}()
	default:
		return status.Errorf(codes.ResourceExhausted, "Limit hit")
	}

	file := NewFile()

	defer func() {
		if err := file.Close(); err != nil {
			s.logger.Error("Close file error", err)
		}
	}()

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			return s.logGrpcError(status.Error(codes.Internal, err.Error()))
		}

		if !file.IsSet() {
			file.SetFile(req.GetFileName(), s.filesStorage)
		}
		chunk := req.GetChunk()
		if err := file.Write(chunk); err != nil {
			s.logger.Error("Write file error")
		}
		s.logger.WithFields(log.Fields{
			"size": file.fileSize,
		}).Debug("write a chunk")
	}

	fileName := filepath.Base(file.FilePath)

	s.logger.WithFields(log.Fields{
		"file": fileName,
		"size": file.fileSize,
	}).Debug("saved file")

	return stream.SendAndClose(&pb.FileUploadResponse{FileName: fileName, Size: file.fileSize})
}

func (s *FileGRPCServer) Download(request *pb.FileDownloadRequest, stream pb.FileService_DownloadServer) error {
	limiter := s.limiters[DOWNLOAD_LIMITER]
	select {
	case limiter <- void{}:
		defer func() {
			<-limiter
		}()
	default:
		return status.Errorf(codes.ResourceExhausted, "Limit hit")
	}

	fileName := request.GetFileName()

	file, err := os.Open(filepath.Join(s.filesStorage, fileName))
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

func (s *FileGRPCServer) List(ctx context.Context, reqv *pb.FileListRequest) (*pb.FileListResponse, error) {
	limiter := s.limiters[READ_LIST_LIMITER]
	select {
	case limiter <- void{}:
		defer func() {
			<-limiter
		}()
	default:
		return &pb.FileListResponse{}, status.Errorf(codes.ResourceExhausted, "Limit hit")
	}

	var files []*pb.FileInfo

	entries, err := os.ReadDir(s.filesStorage)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range entries {
		_, mtime, ctime, err := statTimes(path.Join(s.filesStorage, e.Name()))
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
