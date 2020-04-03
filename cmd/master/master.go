package main

import (
	"context"
	"flag"
	"github.com/plutoberth/Failsystem/core/master"
	"github.com/plutoberth/Failsystem/core/master/masterdb"
	"log"
)

var port = flag.Uint("port", 1337, "The Server's port.")

func main() {
	flag.Parse()
	mongoDal, err := masterdb.NewMongoDatastore(context.Background(), "mongo:27017")
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
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
