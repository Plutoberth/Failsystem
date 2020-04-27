package main

import (
	"context"
	"flag"
	"github.com/plutoberth/Failsystem/core/master"
	"github.com/plutoberth/Failsystem/core/master/masterdb"
	"log"
	"os"
)

var port = flag.Uint("port", 1337, "The Server's port.")

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
	server, err := master.NewServer(*port, mongoDal)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	err = server.Serve()
	if err != nil {
		log.Fatalf("Failed afrer opening server: %v", err)
	}
}
