package util

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"

)

// ReadDir returns a list of full path
// of the files under given directory
func ReadDir(fullPath string) ([]string, error) {
	dir, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	files, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	fileList := []string{}

	for _, file := range files {
		fileList = append(fileList, path.Join(fullPath, file))
	}

	return fileList, nil
}

// ZeroPartitionTable is the go equivalent of
// `dd if=/dev/zero of=devicePath bs=512 count=1`.
func ZeroPartitionTable(devicePath string) error {
	file, err := os.OpenFile(devicePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(bytes.Repeat([]byte{0}, 512)); err != nil {
		return err
	}
	return nil
}

// This method is the go equivalent of
// `dd if=/dev/zero of=devicePath`.
func CleanupDataOnDevice(devicePath string) error {
	file, err := os.OpenFile(devicePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	devzero, err := os.Open("/dev/zero")
	if err != nil {
		return err
	}
	defer devzero.Close()
	if _, err := io.Copy(file, devzero); err != nil {
		// We expect to stop when we get ENOSPC.
		if perr, ok := err.(*os.PathError); ok && perr.Err == syscall.ENOSPC {
			return nil
		}
		return err
	}
	return fmt.Errorf("failed to see expected ENOSPC when erasing data")
}