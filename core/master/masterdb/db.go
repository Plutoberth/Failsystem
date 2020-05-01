package masterdb

import (
	"context"
	pb "github.com/plutoberth/Failsystem/model"
	"time"
)

//Datastore defines the database interface for the master.
type Datastore interface {
	//LastUpdate shall be automatically updated.
	UpdateServerEntry(ctx context.Context, entry ServerEntry) error
	//Get the server's entry
	GetServerEntry(ctx context.Context, UUID string) (*ServerEntry, error)
	GetServersWithEnoughSpace(ctx context.Context, requestedSize int64) ([]ServerEntry, error)
	//Create a still-unavailable file entry, to be finalized later.
	CreateFileEntry(ctx context.Context, entry FileEntry) error
	//Finalize the file, marking it as available
	FinalizeFileEntry(ctx context.Context, fileUUID string, hash DbHash) error
	//Update servers that host the file.
	UpdateFileHosts(ctx context.Context, fileUUID string, serverUUID string) error
	//Get a file with that UUID, only if it is available.
	GetFileEntry(ctx context.Context, UUID string) (*FileEntry, error)
	ListFiles(ctx context.Context) ([]FileEntry, error)
}

//The bson tags specify which names the MongoDB serializer should use.
type ServerEntry struct {
	UUID           string    `bson:"_id"`
	Ip             string    `bson:"Ip"`
	AvailableSpace int64     `bson:"AvailableSpace"`
	LastUpdate     time.Time `bson:"LastUpdate"`
}

type FileEntry struct {
	UUID        string   `bson:"_id"`
	Name        string   `bson:"Name"`
	Size        int64    `bson:"Size"`
	ServerUUIDs []string `bson:"ServerUUIDs"`
	Available   bool     `bson:"Available"`
	Hash        DbHash   `bson:"Hash"`
}

type DbHash struct {
	Type    pb.HashType
	HexHash string
}

func (h *DbHash) ToPb() pb.DataHash {
	return pb.DataHash{
		Type:    h.Type,
		HexHash: h.HexHash,
	}
}

func PbToDbHash(hash pb.DataHash) DbHash {
	return DbHash{
		Type:    hash.Type,
		HexHash: hash.HexHash,
	}
}
