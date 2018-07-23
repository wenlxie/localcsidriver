package util

import (
	"os"

	"golang.org/x/sys/unix"
)

// ReadDir returns a list all the files under the given directory
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
	return files, nil
}

// IsDir checks if the given path is a directory
func IsDir(fullPath string) bool {
	dir, err := os.Open(fullPath)
	if err != nil {
		return false
	}
	defer dir.Close()

	stat, err := dir.Stat()
	if err != nil {
		return false
	}

	return stat.IsDir()
}

// IsBlock checks if the given path is a block device
func IsBlock(fullPath string) bool {
	var st unix.Stat_t
	err := unix.Stat(fullPath, &st)
	if err != nil {
		return false
	}

	return (st.Mode & unix.S_IFMT) == unix.S_IFBLK
}
