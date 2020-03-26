package main

import (
	"context"
	"flag"
	"github.com/plutoberth/Failsystem/core/master"
	"log"
)

var port = flag.Uint("port", 1337, "The Server's port.")

func main() {
	flag.Parse()
	mongoDal, err := master.NewMongoDatastore(context.Background(), "192.168.99.100:27017")
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
