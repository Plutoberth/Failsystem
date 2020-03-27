//Package foldermgr defines a way to interact with a folder, with quota management
//and allocs for new files.
package foldermgr

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
)

const dataFolderPerms = 600 //only r/w for the owning user
const transfersDataFolder = "transfers"
const managedFolderSentinel = ".managedFolder"

//ManagedFolder exposes a thread-safe interface to control a folder, with a quota for sizes, and a simple io.WriteCloser interface.
//On most io operations, it's not much more than a thin wrapper except enforcing sizes on writes.
type ManagedFolder interface {

	//Get the maximum number of bytes the folder can store.
	GetQuota() int64

	//Get the remaining number of bytes the folder can store. Normally, the user shouldn't care about this method
	//and use AllocateSpace instead.
	GetRemainingSpace() int64

	//Get the path of the underlying folder.
	GetFolderPath() string

	//Try to allocate space for the specified UUID. Returns true if the allocation succeeded, and false if
	//there wasn't enough space for it.
	//The function will return an error in cases where the folder can't be fetched, or the user tried to allocate
	//space for an existing allocation or file.
	//Cancelling the context is a valid operation as long as WriteToFile wasn't called for the specified UUID,
	//otherwise, the operation is a no-op. Note that the freed space will not be available immediately due to an
	//implementation detail.
	//It's guaranteed that the UUID is freed for reallocation immediately after cancellation.
	AllocateSpace(ctx context.Context, UUID string, allocSize int64) (bool, error)

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

	//Check if the UUID has an allocation.
	CheckIfAllocated(UUID string) bool

	//List files in the directory.
	ListFiles() ([]os.FileInfo, error)

	//Get the path of the managed folder.
	GetPath() string

	//TODO: Add DeleteFile
}

type allocationEntry struct {
	UUID string
	size int64
	//If writtenTo is closed, WriteToFile has been called for the entry. This serves as a simple flag.
	writtenTo chan struct{}
}

type fileEntry struct {
	UUID string
	size int64
}

type managedFolder struct {
	//quota, in bytes.
	quota int64
	//The path to the actual managed folder.
	folderPath string

	//The mutex must be held to access any fields defined below it.
	mtx    *sync.RWMutex
	allocs *map[string]allocationEntry

	//used amount by allocations, in bytes
	usedAllocationBytes int64

	//used amount by finalized files, in bytes
	usedFileBytes int64
}

//NewManagedFolder creates a new managed folder.
func NewManagedFolder(quota int64, folderPath string) (ManagedFolder, error) {
	folderPath, err := filepath.Abs(folderPath)

	if err != nil {
		return nil, fmt.Errorf("\"%v\" is not a valid path", folderPath)
	}

	if stat, err := os.Stat(folderPath); os.IsNotExist(err) {
		//Create all dirs required for the operation
		if err := os.MkdirAll(filepath.Join(folderPath, transfersDataFolder), dataFolderPerms); err != nil {
			log.Printf("Couldn't create dir for \"%v\"", err)
			return nil, fmt.Errorf("couldn't create data folder: %w", err)
		}
	} else {
		if !stat.IsDir() {
			return nil, fmt.Errorf("\"%v\" is not a directory", folderPath)
		}

		//Check if dir is empty
		empty, err := isDirEmpty(folderPath)
		if err != nil {
			return nil, err
		}
		if !empty {
			//If the folder isn't empty, but the sentinel exists, we can write to the folder.
			if !fileExists(filepath.Join(folderPath, managedFolderSentinel)) {
				return nil, fmt.Errorf("Tried to mount on a non-empty folder \"%v\"", folderPath)
			}
		} else {
			// Make sure that the transfer folder is empty
			transferPath := filepath.Join(folderPath, transfersDataFolder)

			if err = os.RemoveAll(transferPath); err != nil {
				return nil, err
			}
			if err = os.Mkdir(transferPath, dataFolderPerms); err != nil {
				log.Printf("Couldn't create dir for \"%v\"", err)
				return nil, fmt.Errorf("couldn't create transfers folder: %w", err)
			}
		}
	}

	f, err := os.Create(filepath.Join(folderPath, managedFolderSentinel))
	if err != nil {
		return nil, fmt.Errorf("Couldn't create sentinel file")
	}
	f.Close()

	usedBytes, err := getDirFilesSize(folderPath)
	if err != nil {
		return nil, err
	}

	folder := managedFolder{
		quota:               quota,
		folderPath:          folderPath,
		mtx:                 new(sync.RWMutex),
		allocs:              &map[string]allocationEntry{},
		usedAllocationBytes: 0,
		usedFileBytes:       usedBytes,
	}

	return &folder, nil
}

func (m *managedFolder) GetQuota() int64 {
	return m.quota
}

func (m *managedFolder) GetRemainingSpace() int64 {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	return m.quota - m.usedAllocationBytes - m.usedFileBytes
}

func (m *managedFolder) GetFolderPath() string {
	return m.folderPath
}

func (m *managedFolder) CheckIfAllocated(UUID string) bool {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	if _, ok := (*m.allocs)[UUID]; ok {
		return true
	}
	return false
}

func (m *managedFolder) AllocateSpace(ctx context.Context, UUID string, allocSize int64) (bool, error) {
	if m.GetRemainingSpace() < allocSize {
		return false, nil
	}

	if _, err := uuid.Parse(UUID); err != nil {
		return false, fmt.Errorf("\"%v\" is not a valid UUID", UUID)
	}

	if fileExists(path.Join(m.folderPath, UUID)) {
		return false, fmt.Errorf("\"%v\" already exists", UUID)
	}

	entry := allocationEntry{
		UUID:      UUID,
		size:      allocSize,
		writtenTo: make(chan struct{}),
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := (*m.allocs)[UUID]; ok {
		return false, fmt.Errorf("Tried to reallocate an existing allocation")
	}

	m.usedAllocationBytes += entry.size
	(*m.allocs)[UUID] = entry
	go m.waitForCancellation(ctx, entry)

	return true, nil
}

func (m *managedFolder) ReadFile(UUID string) (io.ReadCloser, error) {
	//Security note: if the UUID constraint is removed, you must check for path traversal.
	if _, err := uuid.Parse(UUID); err != nil {
		return nil, fmt.Errorf("\"%v\" is not a valid UUID", UUID)
	}
	return os.Open(filepath.Join(m.folderPath, UUID))
}

func (m *managedFolder) WriteToFile(UUID string) (io.WriteCloser, error) {
	//Security note: if the UUID constraint is removed, you must check for path traversal.
	if _, err := uuid.Parse(UUID); err != nil {
		return nil, fmt.Errorf("\"%v\" is not a valid UUID", UUID)
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()
	entry, ok := (*m.allocs)[UUID]
	if !ok {
		return nil, fmt.Errorf("%v not found in allocations", UUID)
	}
	entry.writtenTo <- struct{}{}
	close(entry.writtenTo)

	folderPath := path.Join(m.folderPath, transfersDataFolder)
	fpath := path.Join(folderPath, UUID)

	if fileExists(fpath) {
		return nil, fmt.Errorf("%v is already being written to", UUID)
	}

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		//Create dir
		if err := os.MkdirAll(folderPath, dataFolderPerms); err != nil {
			log.Printf("Couldn't create dir for \"%v\"", err)
			return nil, fmt.Errorf("couldn't create transfers folder: %w", err)
		}
	}

	//Create it in the temporary data folder.
	f, err := os.Create(fpath)

	if err != nil {
		log.Println(err)
		return nil, fmt.Errorf("couldn't open \"%v\" for writing", UUID)
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
		return fmt.Errorf("%v not found in the allocation list", entry.UUID)
	}
}

//Used to notify the managed folder that a new file was added. For extra speed, this can be reworked
//to keep a list of files in memory instead of scanning each time and only keeping size in memory.
func (m *managedFolder) registerFile(entry allocationEntry) error {
	m.mtx.Lock()
	m.usedFileBytes += entry.size
	m.mtx.Unlock()
	return nil
}

func (m *managedFolder) waitForCancellation(ctx context.Context, entry allocationEntry) {
	select {
	case <-ctx.Done():
		_ = m.freeAllocation(entry)
	case <-entry.writtenTo:
		//If they started writing, freeing the allocation is no longer possible so we don't need to listen
		return
	}
}

func (m *managedFolder) ListFiles() ([]os.FileInfo, error) {
	var files []os.FileInfo
	entries, err := ioutil.ReadDir(m.folderPath)
	files = make([]os.FileInfo, 0, len(entries))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && entry.Name() != managedFolderSentinel {
			files = append(files, entry)
		}
	}

	return files, nil
}

func (m *managedFolder) GetPath() string {
	return m.folderPath
}
