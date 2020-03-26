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

func NewMongoDatastore(client *mongo.Client) Datastore {
	return &mongoDataStore{client.Database("failnet")}
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
