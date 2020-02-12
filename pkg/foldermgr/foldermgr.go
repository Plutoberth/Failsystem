//Package foldermgr defines a way to interact with a folder, with quota management
//and allocs for new files.
package foldermgr

import (
	"context"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const UNLIMITED = math.MaxUint64
const dataFolderPerms = 600 //only r/w for the owning user

//ManagedFolder exposes an interface to control a folder, with a quota for sizes, and a simple io.WriteCloser interface.
//On most io operations, it's not much more than a thin wrapper except enforcing sizes on writes.
type ManagedFolder interface {
	//Get the maximum number of bytes the folder can store.
	GetQuota() uint64

	//Get the remaining number of bytes the folder can store. Normally, the user shouldn't care about this method
	//and use AllocateSpace instead.
	GetRemainingQuota() uint64

	//Get the path of the underlying folder.
	GetFolderPath() string

	//Try to allocate space for the specified UUID. Returns true if the allocation succeeded, and false if
	//there wasn't enough space for it.
	//The function will return an error in cases where the folder can't be fetched, or the user tried to allocate
	//space for an existing allocation or file.
	//Cancelling the context is a valid operation as long as WriteToFile wasn't called for the specified UUID,
	//otherwise, the operation is a no-op.
	AllocateSpace(UUID string, allocSize uint64, ctx context.Context) (bool, error)

	//Receive an io.ReadCloser for the specified UUID.
	//ReadFile will fail if the UUID hasn't been closed.
	ReadFile(UUID string) (io.ReadCloser, error)

	//Receive an io.WriteCloser for the specified UUID.
	//This operation will succeed only if there is an unused allocation for the specified UUID.
	//The returned io.WriteCloser will fail (and delete the file) if bytesWritten != bytesAllocated. It may fail
	//when writing (for writing too much) or when closing the file (for writing too little). This way, the caller
	//may terminate the write and free resources if it unexpectedly runs out of data.
	//Using ReadFile on the UUID is only valid after the returned io.WriteCloser has been closed.
	WriteToFile(UUID string) (io.WriteCloser, error)

	//TODO: Add DeleteFile
}

type allocationEntry struct {
	mtx *sync.Mutex
	UUID string
	size uint64
	//If we already started writing to the mutex, the cancel op is a no-op.
	startedWriting bool
}

type managedFolder struct {
	//quota, in bytes.
	quota uint64
	//used amount, in bytes
	usedBytes uint64
	//The path to the actual managed folder.
	folderPath string

	mtx    *sync.RWMutex
	allocs *map[string]allocationEntry
}



//NewManagedFolder creates a new managed folder. nonEmptyOK defines whether it's ok for the folder to not be empty.
//TODO: Add detection for a previously used folder, and use it regardless of nonEmpty
func NewManagedFolder(quota uint64, folderPath string, nonEmptyOK bool) (ManagedFolder, error) {
	folderPath, err := filepath.Abs(folderPath)
	if err != nil {
		return nil, errors.Errorf("\"%v\" is not a valid path", folderPath)
	}

	folder := managedFolder{
		quota:      quota,
		folderPath: folderPath,
		mtx:        new(sync.RWMutex),
		allocs:     new(map[string]allocationEntry),
	}

	if stat, err := os.Stat(folderPath); os.IsNotExist(err) {
		if err := os.MkdirAll(folderPath, dataFolderPerms); err != nil {
			log.Printf("Couldn't create dir for \"%v\"", err)
			return nil, errors.Wrap(err, "Couldn't create data folder")
		}
	} else {
		if !stat.IsDir() {
			return nil, errors.Errorf("\"%v\" is not a directory", folderPath)
		}

		//Check if dir is empty
		empty, err := isDirEmpty(folderPath)
		if err != nil {
			return nil, errors.Errorf("Couldn't open \"%v\"", folderPath)
		}
		if !empty && !nonEmptyOK {
			return nil, errors.Errorf("Tried to mount on a non-empty folder \"%v\"." +
				"Enable the nonEmptyOK flag or try with an empty folder", folderPath)
		}
	}

	return &folder, nil
}

func (m *managedFolder) GetQuota() uint64 {
	return m.quota
}

func (m *managedFolder) GetRemainingQuota() uint64 {
	return m.quota - m.usedBytes
}

func (m *managedFolder) GetFolderPath() string {
	return m.folderPath
}

func (m *managedFolder) AllocateSpace(UUID string, allocSize uint64, ctx context.Context) (bool, error) {
	panic("implement me")
}

func (m *managedFolder) ReadFile(UUID string) (io.ReadCloser, error) {
	panic("implement me")
}

func (m *managedFolder) WriteToFile(UUID string) (io.WriteCloser, error) {
	panic("implement me")
}



