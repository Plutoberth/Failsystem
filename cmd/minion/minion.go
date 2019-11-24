package main

import (
	"flag"
	"log"

	"github.com/plutoberth/Failsystem/core"
)

var port = flag.Uint("port", 1337, "The MinionServer's port.")

func main() {
	flag.Parse()
	server, err := core.NewMinionServer(*port)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	server.Serve()
}
