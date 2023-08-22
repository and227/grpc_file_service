package service

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
)

type File struct {
	fileSize   uint32
	FilePath   string
	buffer     *bytes.Buffer
	OutputFile *os.File
}

func NewFile() *File {
	return &File{}
}

func (f *File) IsSet() bool {
	return f.FilePath != ""
}

func (f *File) Close() error {
	if f.OutputFile == nil {
		return nil
	}
	return f.OutputFile.Close()
}

func (f *File) SetFile(fileName, path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	f.FilePath = filepath.Join(path, fileName)
	file, err := os.Create(f.FilePath)
	if err != nil {
		return err
	}
	f.OutputFile = file
	return nil
}

func (f *File) Write(chunk []byte) error {
	if f.OutputFile == nil {
		return nil
	}

	f.fileSize += uint32(len(chunk))
	_, err := f.OutputFile.Write(chunk)
	return err
}
