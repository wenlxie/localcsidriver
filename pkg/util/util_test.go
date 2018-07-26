package util

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/kubernetes-csi/localcsidriver/pkg/lvm"
)

func TestReadDir(t *testing.T) {
	dirName, err := ioutil.TempDir(os.TempDir(), "util-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	expectedOutput := map[string]string{}
	input := []string{"test-file1", "test-file2", "test-file3"}
	for _, file := range input {
		filePath := path.Join(dirName, file)
		os.Create(filePath)

		expectedOutput[filePath] = ""
	}

	output, err := ReadDir(dirName)
	if err != nil {
		t.Fatal(err)
	}

	if len(expectedOutput) != len(output) {
		t.Fatalf("Expected %d files, but got %d", len(expectedOutput), len(output))
	}

	for _, file := range output {
		if _, ok := expectedOutput[file]; !ok {
			t.Fatalf("Unexpected file %s", file)
		}
	}
}

func TestCleanupDataOnDevice(t *testing.T) {
	var volsize uint64 = 100 << 20
	// Create loop device for test.
	loop, err := lvm.CreateLoopDevice(volsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	err = CleanupDataOnDevice(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetDiskFormat(t *testing.T) {
	var volsize uint64 = 100 << 20
	// Create loop device for test.
	loop, err := lvm.CreateLoopDevice(volsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	// Should get empty fstype and nil error for unformatted disk
	fstype, err := GetDiskFormat(loop.Path())
	if err != nil {
		t.Fatal(err)
	}

	if fstype != "" {
		t.Fatalf("Unexpected fstype: %s", fstype)
	}

	// Format the device
	args := []string{"-F", "-m0", loop.Path()}
	_, err = exec.Command("mkfs.ext4", args...).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}

	// Should get expected fstype
	fstype, err = GetDiskFormat(loop.Path())
	if err != nil {
		t.Fatal(err)
	}

	if fstype != "ext4" {
		t.Fatalf("Expected fstype %s, but got %s", "ext4", fstype)
	}

}
