package app

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"

	config "file_service/config/client"

	client "file_service/internal/client/service"
)

const (
	UPLOAD    = "upload"
	DOWNLOAD  = "download"
	READ_LIST = "read_list"
)

var (
	sendFiles   []string
	readFolder  string
	writeFolder string
)

func init() {
	readFolder = os.Getenv("READ_FILES_FOLDER")
	if readFolder == "" {
		log.Fatal("Empty 'READ_FILES_FOLDER'")
	}
	fmt.Printf("readFolder: %s\n", readFolder)

	writeFolder = os.Getenv("WRITE_FILES_FOLDER")
	if writeFolder == "" {
		log.Fatal("Empty 'WRITE_FILES_FOLDER'")
	}
	fmt.Printf("writeFolder: %s\n", readFolder)

	files, err := os.ReadDir(readFolder)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		sendFiles = append(sendFiles, file.Name())
	}
	fmt.Printf("%d files to send\n", len(sendFiles))
}

func getRandomFile() string {
	n := len(sendFiles)
	return sendFiles[rand.Intn(n)]
}

func getFilename(num int) string {
	n := len(sendFiles)
	return sendFiles[num%n]
}

func Run(command string, requestsNum int) {
	clientService := client.New(
		readFolder, writeFolder,
		config.SERVER_ADDR, config.BATCH_SIZE,
	)
	clientService.Connect()
	defer clientService.Close()

	var wg sync.WaitGroup

	switch command {
	case UPLOAD:
		wg.Add(requestsNum)
		for i := 0; i < requestsNum; i++ {
			go func(num int, client *client.ClientService, wg *sync.WaitGroup) {
				if err := clientService.SendFile(getRandomFile()); err != nil {
					log.Printf("%d) %s\n", num, err)
				}
				wg.Done()
			}(i, clientService, &wg)
		}
	case DOWNLOAD:
		wg.Add(requestsNum)
		for i := 0; i < requestsNum; i++ {
			go func(num int, client *client.ClientService, wg *sync.WaitGroup) {
				if err := clientService.DownloadFile(num, getFilename(num)); err != nil {
					log.Printf("%d) %s\n", num, err)
				}
				wg.Done()
			}(i, clientService, &wg)
		}
	case READ_LIST:
		wg.Add(requestsNum)
		for i := 0; i < requestsNum; i++ {
			go func(procnum int, client *client.ClientService, wg *sync.WaitGroup) {
				if err := client.ListFiles(procnum); err != nil {
					log.Printf("%d) %s\n", procnum, err)
				}
				wg.Done()
			}(i, clientService, &wg)
		}
	default:
		log.Fatalf("Wrong command: %s\n", command)
	}

	wg.Wait()

	fmt.Println("Done!")
}
