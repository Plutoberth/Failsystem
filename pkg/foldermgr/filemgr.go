package foldermgr

import (
	"github.com/pkg/errors"
	"log"
	"os"
	"path"
)

type managedFile struct {
	folder *managedFolder
	file *os.File
	entry allocationEntry
	bytesWritten int64
	filePath string
}

func (m *managedFile) Write(p []byte) (n int, err error) {
	newSize := m.bytesWritten + int64(len(p))
	if newSize > m.entry.size {
		return 0, errors.Errorf("Write exceeded allocation size: %v > %v", newSize, m.entry.size)
	}
	m.bytesWritten = newSize
	//Relay back writes and number written from write function
	return m.file.Write(p)
}

func (m *managedFile) Close() error {
	//Free the allocation, as the file is now an actual file (or invalid, so it's discarded)
	if err := m.folder.freeAllocation(m.entry); err != nil {
		log.Printf("%v when freeing %v", err, m.entry.UUID)
	}

	if err := m.file.Close(); err != nil {
		return err
	}

	if m.bytesWritten != m.entry.size {
		//Contract states that changes are not saved for
		_ = os.Remove(m.filePath)
		return errors.Errorf("Size written != Size allocated: %v != %v", m.bytesWritten, m.entry.size)
	}

	//Move file to storage folder (essentially its parent)
	if err := os.Rename(m.filePath, path.Join(m.folder.folderPath, m.entry.UUID)); err != nil {
		return err
	}

	return m.folder.registerFile(m.entry)
}


