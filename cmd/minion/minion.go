package main

import (
	"flag"
	"log"

	"github.com/plutoberth/Failsystem/core"
)

var port = flag.Uint("port", 1337, "The MinionServer's port.")
var quota = flag.Int64("quota", 13371337, "The data folder's quota.")
var folderPath = flag.String("folder", "./dataFolder", "The data folder's path.")

func main() {
	flag.Parse()
	server, err := core.NewMinionServer(*port, *folderPath, *quota)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	_ = server.Serve()
}
