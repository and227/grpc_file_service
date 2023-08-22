package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	pb "file_service/pkg/file_proto"

	"google.golang.org/grpc"
)

type ClientService struct {
	readFolder  string
	writeFolder string
	addr        string
	batchSize   int
	client      pb.FileServiceClient
	conn        *grpc.ClientConn
}

func New(readFolder string, writeFolder string, addr string, batchSize int) *ClientService {
	return &ClientService{
		readFolder:  readFolder,
		writeFolder: writeFolder,
		addr:        addr,
		batchSize:   batchSize,
	}
}

func (s *ClientService) Connect() error {
	var err error
	s.conn, err = grpc.Dial(s.addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	s.client = pb.NewFileServiceClient(s.conn)
	return nil
}

func (s *ClientService) Close() {
	s.conn.Close()
}

func (s *ClientService) SendFile(filename string) error {
	interrupt := make(chan os.Signal, 1)
	shutdownSignals := []os.Signal{
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
	}
	signal.Notify(interrupt, shutdownSignals...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(s *ClientService) {
		if err := s.upload(ctx, cancel, filename); err != nil {
			log.Print(err)
			cancel()
		}
	}(s)

	select {
	case killSignal := <-interrupt:
		log.Println("Got ", killSignal)
		cancel()
	case <-ctx.Done():
	}

	return nil
}

func (s *ClientService) upload(ctx context.Context, cancel context.CancelFunc, filename string) error {
	stream, err := s.client.Upload(ctx)
	if err != nil {
		fmt.Printf("RPC call error: %s\n", err)
		return err
	}
	file, err := os.Open(path.Join(s.readFolder, filename))
	if err != nil {
		return err
	}
	buf := make([]byte, s.batchSize)
	batchNumber := 1
	for {
		num, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		chunk := buf[:num]

		if err := stream.Send(&pb.FileUploadRequest{FileName: filename, Chunk: chunk}); err != nil {
			fmt.Printf("Send chank error: %s\n ", err)
			return err
		}
		log.Printf("Sent - batch #%v - size - %v\n", batchNumber, len(chunk))
		batchNumber += 1

	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		fmt.Printf("RPC CloseAndRecv error: %s\n", err)
		return err
	}
	log.Printf("Sent - %v bytes - %s\n", res.GetSize(), res.GetFileName())
	cancel()
	return nil
}

func (s *ClientService) DownloadFile(procnum int, filename string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return s.download(ctx, filename, cancel, procnum)
}

func (s *ClientService) download(
	ctx context.Context,
	filename string,
	cancel context.CancelFunc,
	procnum int,
) error {
	stream, err := s.client.Download(
		ctx,
		&pb.FileDownloadRequest{
			FileName:  filename,
			ChankSize: int32(s.batchSize),
		},
	)

	if err != nil {
		return err
	}

	file, err := os.OpenFile(
		path.Join(s.writeFolder, filename),
		os.O_APPEND|os.O_WRONLY|os.O_CREATE,
		0644,
	)
	defer file.Close()

	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		chank := resp.GetChunk()
		_, err = file.Write(chank)
		if err != nil {
			return err
		}
	}

	cancel()

	fmt.Printf(
		"%d) Downloaded file: filename=%s, chank_size=%d\n",
		procnum, filename, s.batchSize,
	)

	return nil
}

func (s *ClientService) listfiles(ctx context.Context, cancel context.CancelFunc, procnum int) error {
	resp, err := s.client.List(ctx, &pb.FileListRequest{})
	if err != nil {
		return err
	}
	for _, file := range resp.GetFiles() {
		fmt.Printf(
			"%d) name: %s, created: %s, updated: %s\n",
			procnum, file.GetFileName(), file.GetCreated().String(), file.GetUpdated().String(),
		)
	}

	cancel()

	return nil
}

func (s *ClientService) ListFiles(procnum int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return s.listfiles(ctx, cancel, procnum)
}
