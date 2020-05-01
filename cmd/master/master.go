package main

import (
	"context"
	"flag"
	"github.com/google/uuid"
	"github.com/plutoberth/Failsystem/core/master"
	"github.com/plutoberth/Failsystem/core/master/masterdb"
	pb "github.com/plutoberth/Failsystem/model"
	"log"
	"math/rand"
	"os"
	"time"
)

var port = flag.Uint("port", 1337, "The Server's port.")

var names = []string{"Correct", "Horse", "Battery", "Staple", "Dog", "Monkey", "Zoo", "Iron", "Rust", "Gopher", "Go",
	"Bjarne", "Joy", "Nehama", "Dan", "Danny", "Randal", "Linus", "Bill"}

func CreateRandomName() string {
	var name string
	for i := 0; i < 4; i++ {
		name += names[rand.Intn(len(names))]
	}
	return name
}

func CreateDummyFile(datastore masterdb.Datastore) {
	rand.Seed(time.Now().UTC().UnixNano())
	entry := masterdb.FileEntry{
		UUID:        uuid.New().String(),
		Name:        CreateRandomName(),
		Size:        int64(rand.Intn(1337)),
		ServerUUIDs: []string{uuid.New().String(), uuid.New().String(), uuid.New().String()},
	}
	if err := datastore.CreateFileEntry(context.Background(), entry); err != nil {
		log.Printf("Skipped %s due to %s", entry.Name, err.Error())
		return
	}
	if err := datastore.FinalizeFileEntry(context.Background(), entry.UUID, masterdb.DbHash{
		Type:    pb.HashType_SHA1,
		HexHash: "abcdefg_Ihatecpp",
	}); err != nil {
		log.Printf("Skipped %s due to %s", entry.Name, err.Error())
	}
}

func main() {
	// Write the filename and line number in the logs. Useful for debugging.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()
	var address string
	if os.Getenv("CONTAINER") != "" {
		address = "mongo:27017"
	} else {
		//This address should be the address of the mongo host
		address = "192.168.99.100:27017"
	}
	mongoDal, err := masterdb.NewMongoDatastore(context.Background(), address)

	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	for i := 0; i < 20; i++ {
		CreateDummyFile(mongoDal)
	}


	server, err := master.NewServer(*port, mongoDal)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	err = server.Serve()
	if err != nil {
		log.Fatalf("Failed afrer opening server: %v", err)
	}
}
