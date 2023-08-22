package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"

	pb "file_service/file_proto"

	"google.golang.org/grpc"
)

const (
	WRITE_FOLDER = "write"
	READ_FOLDER  = "read"
)

type ClientService struct {
	addr      string
	filePath  string
	batchSize int
	client    pb.FileServiceClient
	conn      *grpc.ClientConn
}

func New(addr string, filePath string, batchSize int) *ClientService {
	return &ClientService{
		addr:      addr,
		filePath:  filePath,
		batchSize: batchSize,
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
	log.Println(s.addr, s.filePath)
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
	file, err := os.Open(path.Join(READ_FOLDER, filename))
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
			ChankSize: BATCH_SIZE,
		},
	)

	if err != nil {
		return err
	}

	file, err := os.OpenFile(
		path.Join(WRITE_FOLDER, filename),
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

	fmt.Printf("%d) Downloaded file: filename=%s, chank_size=%d\n", procnum, filename, BATCH_SIZE)

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

func getRandomFile() string {
	n := len(sendFiles)
	return sendFiles[rand.Intn(n)]
}

func getFilename(num int) string {
	n := len(sendFiles)
	return sendFiles[num%n]
}

var sendFiles []string

func init() {
	files, err := os.ReadDir(READ_FOLDER)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		sendFiles = append(sendFiles, file.Name())
	}
	fmt.Printf("%d files to send\n", len(sendFiles))
}

func main() {
	clientService := New(SERVER_ADDR, FILE_PATH, BATCH_SIZE)
	clientService.Connect()
	defer clientService.Close()

	// if err := clientService.SendFile(); err != nil {
	// 	log.Fatal(err)
	// }
	// if err := clientService.DownloadFile(FILE_NAME); err != nil {
	// 	log.Fatal(err)
	// }
	// if err := clientService.ListFiles(); err != nil {
	// 	log.Fatal(err)
	// }

	var wg sync.WaitGroup

	// wg.Add(20)
	// for i := 0; i < 20; i++ {
	// 	go func(num int, client *ClientService, wg *sync.WaitGroup) {
	// 		if err := clientService.SendFile(getRandomFile()); err != nil {
	// 			log.Fatal(err)
	// 		}
	// 		wg.Done()
	// 	}(i, clientService, &wg)
	// }

	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(num int, client *ClientService, wg *sync.WaitGroup) {
			if err := clientService.DownloadFile(num, getFilename(num)); err != nil {
				log.Printf("%d) %s\n", num, err)
			}
			wg.Done()
		}(i, clientService, &wg)
	}

	// wg.Add(20)
	// for i := 0; i < 20; i++ {
	// 	go func(procnum int, client *ClientService, wg *sync.WaitGroup) {
	// 		if err := client.ListFiles(procnum); err != nil {
	// 			log.Printf("%d) %s\n", procnum, err)
	// 		}
	// 		wg.Done()
	// 	}(i, clientService, &wg)
	// }

	wg.Wait()

	fmt.Println("Done!")
}
