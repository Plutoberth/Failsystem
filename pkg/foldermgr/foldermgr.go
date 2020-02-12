//Package foldermgr defines a way to interact with a folder, with quota management
//and allocs for new files.
package foldermgr

import (
	"context"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
)

const UNLIMITED = math.MaxUint64
const dataFolderPerms = 600 //only r/w for the owning user
const transfersDataFolder = "transfers"

//ManagedFolder exposes a thread-safe interface to control a folder, with a quota for sizes, and a simple io.WriteCloser interface.
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
	//It's guaranteed that the UUID is freed for reallocation immediately after cancellation.
	AllocateSpace(ctx context.Context, UUID string, allocSize uint64) (bool, error)

	//Receive a non thread-safe io.ReadCloser for the specified UUID.
	//ReadFile will fail if the UUID hasn't been closed.
	ReadFile(UUID string) (io.ReadCloser, error)

	//Receive a non thread-safe io.WriteCloser for the specified UUID.
	//This operation will succeed only if there is an unused allocation for the specified UUID.
	//The returned io.WriteCloser will fail (and delete the file) if bytesWritten != bytesAllocated. It may fail
	//when writing (for writing too much) or when closing the file (for writing too little). This way, the caller
	//may terminate the write and free resources if it unexpectedly runs out of data.
	//Using ReadFile on the UUID is only valid after the returned io.WriteCloser has been closed.
	WriteToFile(UUID string) (io.WriteCloser, error)

	//TODO: Add DeleteFile
}

type allocationEntry struct {
	UUID string
	size uint64
	//If writtenTo is closed, WriteToFile has been called for the entry. This serves as a simple flag.
	writtenTo chan struct{}
}

type fileEntry struct {
	UUID string
	size uint64
}

type managedFolder struct {
	//quota, in bytes.
	quota uint64
	//The path to the actual managed folder.
	folderPath string

	//The mutex must be held to access any fields defined below it.
	mtx    *sync.RWMutex
	allocs *map[string]allocationEntry

	//used amount by allocations, in bytes
	usedAllocationBytes uint64

	//used amount by finalized files, in bytes
	usedFileBytes uint64
}

//NewManagedFolder creates a new managed folder. nonEmptyOK defines whether it's ok for the folder to not be empty.
//TODO: Add detection for a previously used folder, and use it regardless of nonEmpty
func NewManagedFolder(quota uint64, folderPath string, nonEmptyOK bool) (ManagedFolder, error) {
	folderPath, err := filepath.Abs(folderPath)
	if err != nil {
		return nil, errors.Errorf("\"%v\" is not a valid path", folderPath)
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
			return nil, errors.Errorf("Tried to mount on a non-empty folder \"%v\"."+
				"Enable the nonEmptyOK flag or try with an empty folder", folderPath)
		}
	}

	usedBytes, err := getDirFilesSize(folderPath)
	if err != nil {
		return nil, err
	}

	folder := managedFolder{
		quota:               quota,
		folderPath:          folderPath,
		mtx:                 new(sync.RWMutex),
		allocs:              new(map[string]allocationEntry),
		usedAllocationBytes: 0,
		usedFileBytes:       usedBytes,
	}

	return &folder, nil
}

func (m *managedFolder) GetQuota() uint64 {
	return m.quota
}

func (m *managedFolder) GetRemainingQuota() uint64 {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	return m.quota - m.usedAllocationBytes - m.usedFileBytes
}

func (m *managedFolder) GetFolderPath() string {
	return m.folderPath
}

func (m *managedFolder) AllocateSpace(ctx context.Context, UUID string, allocSize uint64) (bool, error) {
	if m.GetRemainingQuota() < allocSize {
		return false, nil
	}

	if _, err := uuid.Parse(UUID); err != nil {
		return false, errors.Errorf("\"%v\" is not a valid UUID", UUID)
	}

	if fileExists(path.Join(m.folderPath, UUID)) {
		return false, errors.Errorf("\"%v\" already exists")
	}

	entry := allocationEntry{
		UUID:      UUID,
		size:      allocSize,
		writtenTo: make(chan struct{}),
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := (*m.allocs)[UUID]; ok {
		return false, errors.Errorf("Tried to reallocate an existing allocation")
	}

	m.usedAllocationBytes += entry.size
	(*m.allocs)[UUID] = entry
	go m.waitForCancellation(ctx, entry)

	return true, nil
}

func (m *managedFolder) ReadFile(UUID string) (io.ReadCloser, error) {
	panic("implement me")
}

func (m *managedFolder) WriteToFile(UUID string) (io.WriteCloser, error) {
	if _, err := uuid.Parse(UUID); err != nil {
		return nil, errors.Errorf("\"%v\" is not a valid UUID", UUID)
	}

	//this also serves as a lock for entry, so that two functions wouldn't try to close it together after both of them
	//checked that the channel wasn't closed
	m.mtx.RLock()
	entry, ok := (*m.allocs)[UUID]
	if !ok {
		return nil, errors.Errorf("%v not found in allocations")
	}
	select {
	case <-entry.writtenTo:
		return nil, errors.Errorf("%v is already being written to", UUID)
	default:
	}
	close(entry.writtenTo)

	//We can unlock it now, because entry is exclusive for us and we no longer need map access.
	m.mtx.RUnlock()

	if fileExists(path.Join(m.folderPath, UUID)) {
		return nil, errors.Errorf("\"%v\" already exists... somehow. This shouldn't happen.")
	}

	//Create it in the temporary data folder.
	fpath := path.Join(m.folderPath, transfersDataFolder, UUID)

	f, err := os.Create(fpath)
	if err != nil {
		return nil, errors.Errorf("Couldn't open \"%v\" for writing", UUID)
	}

	fileToWrite := managedFile{
		folder:       m,
		file:         f,
		entry:        entry,
		bytesWritten: 0,
		filePath:     fpath,
	}

	return &fileToWrite, nil
}

func (m *managedFolder) freeAllocation(entry allocationEntry) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, ok := (*m.allocs)[entry.UUID]; ok {
		m.usedAllocationBytes -= entry.size
		delete(*m.allocs, entry.UUID)
		return nil
	} else {
		return errors.Errorf("%v not found in the allocation list", entry.UUID)
	}
}

//Used to notify the managed folder that a new file was added. For extra speed, this can be reworked
//to keep a list of files in memory instead of scanning each time and only keeping size in memory.
func (m *managedFolder) registerFile(entry allocationEntry) error {
	m.mtx.RLock()
	m.usedFileBytes += entry.size
	m.mtx.RUnlock()
	return nil
}

func (m *managedFolder) waitForCancellation(ctx context.Context, entry allocationEntry) {
	select {
	case <-ctx.Done():
		_ = m.freeAllocation(entry)
	case <-entry.writtenTo:
		//If they started writing to it, we no longer need to listen for the context.
		return
	}
}
