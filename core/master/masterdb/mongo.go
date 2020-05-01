package masterdb

import (
	"context"
	"fmt"
	"github.com/plutoberth/Failsystem/core/minion"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"time"
)

type mongoDataStore struct {
	db *mongo.Database
}

const (
	dbName                 = "failsystemdb"
	serverCollection       = "servers"
	fileCollection         = "files"
	serverExpiryIndex      = "serverExpiryIndex"
	uniqueNameIndex        = "uniqueNameIndex"
	duplicateKeyCode       = 11000
	collectionNotFoundCode = 26
	serverTTL              = minion.HeartbeatInterval * 3
	defaultUser            = "failnet"
	defaultPassword        = "failnet"
)

func getEnvs() (username string, password string) {
	username = os.Getenv("MONGO_INITDB_ROOT_USERNAME")
	password = os.Getenv("MONGO_INITDB_ROOT_PASSWORD")
	//If the contents were not found, we're probably running in a debug env.
	if username == "" || password == "" {
		log.Printf("Didn't find Mongo credentials in MONGO_INITDB_ROOT_USERNAME and MONGO_INITDB_ROOT_PASSWORD, using defaults")
		return defaultUser, defaultPassword
	}
	return username, password
}

func NewMongoDatastore(ctx context.Context, address string) (Datastore, error) {
	username, password := getEnvs()
	//Configure username and password
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
		return nil, fmt.Errorf("failed to ping mongodb: %w", err)
	}

	//Open the failnet database within the server
	database := client.Database(dbName)
	if err := createIndexes(ctx, database); err != nil {
		return nil, err
	}

	return &mongoDataStore{database}, nil
}

func createIndexes(ctx context.Context, database *mongo.Database) error {
	_, err := database.Collection(serverCollection).Indexes().DropAll(ctx, options.DropIndexes())
	if err != nil {
		mongoCommandErr, ok := err.(mongo.CommandError)
		if !(ok && mongoCommandErr.Code == collectionNotFoundCode) {
			return err
		}
	}

	//Reconstruct the automatic dead server expiration index
	_, err = database.Collection(serverCollection).Indexes().CreateOne(ctx,
		mongo.IndexModel{Keys: bson.M{"LastUpdate": 1},
			Options: options.Index().SetExpireAfterSeconds(int32(serverTTL.Seconds())).SetName(serverExpiryIndex)},
		options.CreateIndexes())

	if err != nil {
		return fmt.Errorf("failed to create expiration index: %v", err)
	}

	_, err = database.Collection(fileCollection).Indexes().DropAll(ctx, options.DropIndexes())
	if err != nil {
		mongoCommandErr, ok := err.(mongo.CommandError)
		if !(ok && mongoCommandErr.Code == collectionNotFoundCode) {
			return err
		}
	}

	//.SetPartialFilterExpression(bson.M{"Available": bson.M{"$eq": true}})},
	_, err = database.Collection(fileCollection).Indexes().CreateMany(ctx,
		[]mongo.IndexModel{{
			Keys: bson.M{"Name": 1}, Options: options.Index().SetUnique(true).SetName(uniqueNameIndex),
		}, {
			Keys: bson.M{"UUID": 1}, Options: options.Index(),
		},
		},
		options.CreateIndexes())

	if err != nil {
		return fmt.Errorf("failed to create name uniqueness index: %v", err)
	}

	return nil
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
	entry.Hash = DbHash{}

	//if (it exists, but it wasn't finalized) update the document, else (just insert a new one)
	_, err := m.db.Collection(fileCollection).
		UpdateOne(ctx,
			bson.M{"Available": bson.M{"$eq": false}, "Name": entry.Name},
			bson.M{"$set": entry}, options.Update().SetUpsert(true))
	if err != nil {
		var errstring string
		switch e := err.(type) {
		case mongo.WriteException:
			if len(e.WriteErrors) >= 1 {
				writeErr := e.WriteErrors[0]
				if writeErr.Code == duplicateKeyCode {
					errstring = fmt.Sprintf("name %s already exists", entry.Name)
				} else {
					log.Println(writeErr.Message)
					errstring = writeErr.Message
				}
			} else {
				errstring = e.Error()
			}

		default:
			errstring = e.Error()
		}
		return fmt.Errorf("create file failed: %v", errstring)
		//Intentionally not wrapping so callers wouldn't depend on error
	}

	return nil
}

func (m *mongoDataStore) FinalizeFileEntry(ctx context.Context, fileUUID string, hash DbHash) error {
	_, err := m.db.Collection(fileCollection).UpdateOne(ctx, bson.M{"UUID": fileUUID},
		bson.M{"$set": bson.M{"Available": true, "Hash": hash}}, options.Update())
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update file failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetFileEntry(ctx context.Context, UUID string) (*FileEntry, error) {
	var res = new(FileEntry)
	//Get a file with that UUID, only if it's available.
	findResult := m.db.Collection(fileCollection).FindOne(ctx,
		bson.M{"UUID": UUID, "Available": bson.M{"$eq": true}},
		options.FindOne())
	if findResult.Err() != nil {
		return nil, fmt.Errorf("find file failed: %v", findResult.Err())
	}
	if err := findResult.Decode(&res); err != nil {
		return nil, fmt.Errorf("find file failed: %v", err)
	}
	return res, nil
}

func (m *mongoDataStore) ListFiles(ctx context.Context) ([]FileEntry, error) {
	var results = make([]FileEntry, 0)
	//Get all files that are available
	cursor, err := m.db.Collection(fileCollection).Find(ctx, bson.M{"Available": bson.M{"$eq": true}}, options.Find())
	if err != nil {
		return nil, fmt.Errorf("list files failed: %v", err)
	}
	//Try to decode into the array
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decoding listed files failed: %v", err)
	}
	return results, nil
}

func (m *mongoDataStore) UpdateFileHosts(ctx context.Context, fileUUID string, serverUUID string) error {
	//Add the new file host to a the set of hosts. As per the set data structure, if it is already there, it will not
	//be added again.
	_, err := m.db.Collection(fileCollection).UpdateOne(ctx, bson.M{"UUID": fileUUID},
		bson.M{"$addToSet": bson.M{"ServerUUIDs": serverUUID}}, options.Update())
	if err != nil {
		//Intentionally not wrapping so callers wouldn't depend on error
		return fmt.Errorf("update file failed: %v", err)
	}

	return nil
}

func (m *mongoDataStore) GetServersWithEnoughSpace(ctx context.Context, requestedSize int64) ([]ServerEntry, error) {
	var results = make([]ServerEntry, 0)
	//Find all servers whose available space is greater than the requested size.
	cursor, err := m.db.Collection(serverCollection).Find(ctx, bson.M{"AvailableSpace": bson.M{"$gt": requestedSize}}, options.Find())
	if err != nil {
		return nil, fmt.Errorf("find available servers failed: %v", err)
	}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}
