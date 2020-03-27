package master

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Datastore interface {
	//LastUpdate shall be automatically updated.
	UpdateServerEntry(ctx context.Context, entry ServerEntry) error
	GetServerEntry(ctx context.Context, UUID string) (*ServerEntry, error)
	UpdateFileEntry(ctx context.Context, FileUUID string, Size int64, ServerUUID string) error
	GetFileEntry(ctx context.Context, UUID string) (*FileEntry, error)
}

type ServerEntry struct {
	UUID           string `bson:"_id"`
	Ip             string `bson:"Ip"`
	AvailableSpace int64 `bson:"AvailableSpace"`
	LastUpdate     time.Time `bson:"LastUpdate"`
}

type FileEntry struct {
	UUID        string `bson:"_id"`
	Size        int64 `bson:"Size"`
	ServerUUIDs []byte `bson:"ServerUUIDs"`
}

type Lease struct {
	FileUUID  string `bson:"_id"`
	GrantedTo string `bson:"GrantedTo"`
}

type Operation int

const (
	Upload      Operation = 1
	Download    Operation = 2
	Delete      Operation = 3
	Replication Operation = 4
)

type mongoDataStore struct {
	db *mongo.Database
}

const (
	serverCollection = "servers"
	fileCollection   = "files"
	expiryIndex = "expiryIndex"
	serverTTL = 30
)

func NewMongoDatastore(ctx context.Context, address string) (Datastore, error) {
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", address)).
		SetAuth(options.Credential{Username: "root", Password: "example"})

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	database := client.Database("failnet")
	_, _ = database.Collection(serverCollection).Indexes().DropOne(ctx, expiryIndex, options.DropIndexes())
	_, err = database.Collection(serverCollection).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.M{"LastUpdate": 1},
			Options: options.Index().SetExpireAfterSeconds(serverTTL).SetName(expiryIndex)},
		options.CreateIndexes())
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	return &mongoDataStore{database}, nil
}

//TODO:Fix contexts

func (m *mongoDataStore) UpdateServerEntry(ctx context.Context, entry ServerEntry) error {
	entry.LastUpdate = time.Now()
	_, err := m.db.Collection(serverCollection).UpdateOne(ctx, bson.M{"_id": entry.UUID},
		bson.M{"$set": entry}, options.Update().SetUpsert(true))
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update server failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetServerEntry(ctx context.Context, UUID string) (*ServerEntry, error) {
	var res = new(ServerEntry)
	findResult := m.db.Collection(serverCollection).FindOne(ctx, bson.M{"_id": UUID}, options.FindOne())
	if findResult.Err() != nil {
		return nil, fmt.Errorf("find server failed: %v", findResult.Err())
	}
	if err := findResult.Decode(&res); err != nil {
		return nil, fmt.Errorf("find server failed: %v", err)
	}
	return res, nil
}

func (m *mongoDataStore) UpdateFileEntry(ctx context.Context, FileUUID string, Size int64, ServerUUID string) error {
	_, err := m.db.Collection(fileCollection).UpdateOne(ctx, bson.M{"_id": FileUUID},
		bson.M{"$set": bson.M{"_id": FileUUID, "Size": Size}, "$addToSet": bson.M{"ServerUUIDs": ServerUUID}}, options.Update().SetUpsert(true))
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update file failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetFileEntry(ctx context.Context, UUID string) (*FileEntry, error) {
	var res = new(FileEntry)
	findResult := m.db.Collection(fileCollection).FindOne(ctx, bson.M{"_id": UUID}, options.FindOne())
	if findResult.Err() != nil {
		return nil, fmt.Errorf("find file failed: %v", findResult.Err())
	}
	if err := findResult.Decode(&res); err != nil {
		return nil, fmt.Errorf("find file failed: %v", err)
	}
	return res, nil
}
