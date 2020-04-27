package foldermgr

import (
	"fmt"
	"log"
	"os"
	"path"
)

type managedFile struct {
	folder       *managedFolder
	file         *os.File
	entry        allocationEntry
	bytesWritten int64
	filePath     string
}

func (m *managedFile) Write(p []byte) (n int, err error) {
	newSize := m.bytesWritten + int64(len(p))
	if newSize > m.entry.size {
		return 0, fmt.Errorf("Write exceeded allocation size: %v > %v", newSize, m.entry.size)
	}
	actualBytesWritten, err := m.file.Write(p)
	m.bytesWritten += int64(actualBytesWritten) //Only update the actual number that was written
	return actualBytesWritten, err
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
		//The contract states that an unfulfilled quota will discard the file.
		_ = os.Remove(m.filePath)
		return fmt.Errorf("size written != size allocated: %v != %v", m.bytesWritten, m.entry.size)
	}

	//Move file to storage folder (essentially its parent)
	if err := os.Rename(m.filePath, path.Join(m.folder.folderPath, m.entry.UUID)); err != nil {
		return err
	}

	return m.folder.registerFile(m.entry)
}
