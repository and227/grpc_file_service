package main

import (
	"file_service/internal/client/app"
	_ "file_service/internal/client/app"
	"flag"
	"fmt"
)

var (
	command     string
	requestsNum int
)

const DEFAULT_REQUEST_NUM = 20
const DEFAULT_COMMAND = "read_list"

func init() {
	flag.StringVar(&command, "command", DEFAULT_COMMAND, "Test command type")
	flag.IntVar(&requestsNum, "requestNum", DEFAULT_REQUEST_NUM, "Number of requests")
	flag.Parse()
}

func main() {
	fmt.Printf("Run command %s\n", command)
	app.Run(command, requestsNum)
}
