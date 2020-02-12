//Package foldermgr defines a way to interact with a folder, with quota management
//and allocations for new files.
package foldermgr

import (
	"context"
	"io"
	"math"
)

const UNLIMITED = math.MaxUint64

//ManagedFolder exposes an interface to control a folder, with a quota for sizes, and a simple io.WriteCloser interface.
//On most io operations, it's not much more than a thin wrapper except enforcing sizes on writes.
type ManagedFolder interface {
	//Get the maximum number of bytes the folder can store.
	GetQuota() uint64

	//Get the path of the underlying folder.
	GetFolderPath() string

	//Get the remaining number of bytes the folder can store. Normally, the user shouldn't care about this method
	//and use AllocateSpace instead.
	GetRemainingQuota() uint64

	//Try to allocate space for the specified UUID. Returns bool if the allocation was successful or not.
	//The function will return an error in cases where the folder can't be fetched, or the user tried to allocate
	//space for an existing allocation or file.
	//Cancelling the context is a valid operation as long as WriteToFile wasn't called for the specified UUID,
	//otherwise, the operation is a no-op.
	AllocateSpace(UUID string, allocSize uint64, ctx context.Context) (bool, error)

	//Receive an io.ReadCloser for the specified UUID.
	//ReadFile will fail if the UUID is still being written to.
	ReadFile(UUID string) (io.ReadCloser, error)

	//Receive an io.WriteCloser for the specified UUID.
	//This operation will succeed only if there is an unused allocation for the specified UUID.
	//The returned io.WriteCloser will fail if more bytes were written to it than were allocated by AllocateSpace.
	//Using ReadFile on the UUID is only valid after the returned io.WriteCloser has been closed.
	WriteToFile(UUID string) (io.WriteCloser, error)

	//TODO: Add DeleteFile
}

type managedFolder struct {
	//quota, in bytes.
	quota uint64
	//The path to the actual managed folder.
	folderPath string
}