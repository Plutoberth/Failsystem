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
	//Date shall be automatically updated.
	UpdateServerEntry(entry ServerEntry) error
	GetServerEntry(UUID string) (*ServerEntry, error)
}

type ServerEntry struct {
	UUID           string `bson:"_id"`
	LastIp         string
	AvailableSpace string
	Date           time.Time
}

type mongoDataStore struct {
	db *mongo.Database
}

const (
	serverCollection = "servers"
	fileCollection   = "files"
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
	return &mongoDataStore{client.Database("failnet")}, nil
}

func (m *mongoDataStore) UpdateServerEntry(entry ServerEntry) error {
	_, err := m.db.Collection(serverCollection).UpdateOne(context.Background(), bson.M{"_id": entry.UUID},
		bson.M{"$set": entry}, options.Update().SetUpsert(true))
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update failed: %v", err)
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
