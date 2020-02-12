package foldermgr

import (
	"io"
	"os"
	"path/filepath"
)

func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func fileExists(filename string) bool {
	stat, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !stat.IsDir()
}

//Gets the size (non-recursively) of all files in the directory.
func getDirFilesSize(path string) (uint64, error) {
	var size uint64
	dir, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	
	files, err := dir.Readdirnames(-1)
	for _, file := range files {
		stat, err := os.Stat(filepath.Join(path, file) )
		if err != nil {
			return 0, err
		}
		if !stat.IsDir() {
			size += uint64(stat.Size())
		}
	}

	return size, err
}