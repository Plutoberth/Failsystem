package masterdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/plutoberth/Failsystem/core/minion"
	pb "github.com/plutoberth/Failsystem/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"time"
)

type mongoDataStore struct {
	db *mongo.Database
}



const (
	dbName           = "failsystemdb"
	serverCollection = "servers"
	fileCollection   = "files"
	expiryIndex      = "expiryIndex"
	serverTTL        = minion.HeartbeatInterval * 3
)

func getEnvs() (username string, password string, err error) {
	username = os.Getenv("MONGO_INITDB_ROOT_USERNAME")
	password = os.Getenv("MONGO_INITDB_ROOT_PASSWORD")
	if username == "" || password == "" {
		return "", "", errors.New("MONGO_INITDB_ROOT_USERNAME and MONGO_INITDB_ROOT_PASSWORD must be set")
	}
	return username, password, nil
}

func NewMongoDatastore(ctx context.Context, address string) (Datastore, error) {
	username, password, err := getEnvs()
	if err != nil {
		return nil, err
	}
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", address)).
		SetAuth(options.Credential{Username: username, Password: password})

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

	database := client.Database(dbName)
	_, _ = database.Collection(serverCollection).Indexes().DropOne(ctx, expiryIndex, options.DropIndexes())
	_, err = database.Collection(serverCollection).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.M{"LastUpdate": 1},
			Options: options.Index().SetExpireAfterSeconds(int32(serverTTL.Seconds())).SetName(expiryIndex)},
		options.CreateIndexes())
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	return &mongoDataStore{database}, nil
}

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

func (m *mongoDataStore) CreateFileEntry(ctx context.Context, entry FileEntry) error {
	entry.Available = false
	entry.Hash = pb.DataHash{}
	_, err := m.db.Collection(fileCollection).InsertOne(ctx, bson.M{"$set": entry}, options.InsertOne())
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("create file failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) FinalizeFileEntry(ctx context.Context, fileUUID string, hash pb.DataHash) error {
	_, err := m.db.Collection(fileCollection).UpdateOne(ctx, bson.M{"_id": fileUUID},
		bson.M{"$set": bson.M{"Available": true, "Hash": hash}}, options.Update())
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


func (m *mongoDataStore) ListFiles(ctx context.Context, UUID string) ([]FileEntry, error) {
	var results = make([]FileEntry, 0)
	cursor, err := m.db.Collection(fileCollection).Find(ctx, bson.M{"Available": bson.M{"$eq": true}}, options.Find())
	if err != nil {
		return nil, fmt.Errorf("list files failed: %v", err)
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decoding listed files failed: %v", err)
	}
	return results, nil
}

func (m *mongoDataStore) UpdateFileHosts(ctx context.Context, fileUUID string, serverUUID string) error {
	_, err := m.db.Collection(fileCollection).UpdateOne(ctx, bson.M{"_id": fileUUID},
		bson.M{"$addToSet": bson.M{"ServerUUIDs": serverUUID}}, options.Update())
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update file failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetServersWithEnoughSpace(ctx context.Context, requestedSize int64) ([]ServerEntry, error) {
	var results = make([]ServerEntry, 0)
	cursor, err := m.db.Collection(serverCollection).Find(ctx, bson.M{"AvailableSpace": bson.M{"$gt": requestedSize}}, options.Find())
	if err != nil {
		return nil, fmt.Errorf("find available servers failed: %v", err)
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}
