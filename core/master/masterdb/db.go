package masterdb

import (
	"context"
	pb "github.com/plutoberth/Failsystem/model"
	"time"
)

type Datastore interface {
	//LastUpdate shall be automatically updated.
	UpdateServerEntry(ctx context.Context, entry ServerEntry) error
	GetServerEntry(ctx context.Context, UUID string) (*ServerEntry, error)
	GetServersWithEnoughSpace(ctx context.Context, requestedSize int64) ([]ServerEntry, error)
	CreateFileEntry(ctx context.Context, entry FileEntry) error
	UpdateFileHosts(ctx context.Context, fileUUID string, serverUUID string) error
	FinalizeFileEntry(ctx context.Context, fileUUID string, hash pb.DataHash) error
	GetFileEntry(ctx context.Context, UUID string) (*FileEntry, error)
	ListFiles(ctx context.Context) ([]FileEntry, error)
}

type ServerEntry struct {
	UUID           string    `bson:"_id"`
	Ip             string    `bson:"Ip"`
	AvailableSpace int64     `bson:"AvailableSpace"`
	LastUpdate     time.Time `bson:"LastUpdate"`
}

type FileEntry struct {
	UUID        string      `bson:"_id"`
	Name        string      `bson:"Name"`
	Size        int64       `bson:"Size"`
	ServerUUIDs []string    `bson:"ServerUUIDs"`
	Available   bool        `bson:"Available"`
	Hash        pb.DataHash `bson:"Hash"`
}

