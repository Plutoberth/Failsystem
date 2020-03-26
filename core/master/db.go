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
	UpdateServerEntry(entry ServerEntry) error
	GetServerEntry(UUID string) (*ServerEntry, error)
	UpdateFileEntry(entry FileEntry) error
}

type ServerEntry struct {
	UUID           string `bson:"_id"`
	Ip             string
	AvailableSpace string
	LastUpdate     time.Time
}

type FileEntry struct {
	UUID        string `bson:"_id"`
	Size        int64
	ServerUUIDs []byte
}

type mongoDataStore struct {
	db *mongo.Database
}

const (
	serverCollection = "servers"
	fileCollection   = "files"

	serverTTL = 360
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
	_, err = database.Collection(serverCollection).Indexes().CreateOne(context.Background(),
		mongo.IndexModel{Keys: bson.M{"LastUpdate": 1}, Options: options.Index().SetExpireAfterSeconds(serverTTL)},
		options.CreateIndexes())
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	return &mongoDataStore{database}, nil
}

//TODO:Fix contexts

func (m *mongoDataStore) UpdateServerEntry(entry ServerEntry) error {
	entry.LastUpdate = time.Now()
	_, err := m.db.Collection(serverCollection).UpdateOne(context.Background(), bson.M{"_id": entry.UUID},
		bson.M{"$set": entry}, options.Update().SetUpsert(true))
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update server failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetServerEntry(UUID string) (*ServerEntry, error) {
	var res = new(ServerEntry)
	findResult := m.db.Collection(serverCollection).FindOne(context.Background(), bson.M{"_id": UUID}, options.FindOne())
	if findResult.Err() != nil {
		return nil, fmt.Errorf("find failed: %v", findResult.Err())
	}
	if err := findResult.Decode(&res); err != nil {
		return nil, fmt.Errorf("find failed: %v", err)
	}
	return res, nil
}
}
