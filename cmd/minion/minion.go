package main

import (
	"flag"
	"github.com/plutoberth/Failsystem/core/minion"
	"log"
)

var port = flag.Uint("port", 1337, "The Server's port.")
var quota = flag.Int64("quota", 13371337, "The data folder's quota.")
var folderPath = flag.String("folder", "./dataFolder", "The data folder's path.")

func main() {
	flag.Parse()
	server, err := minion.NewServer(*port, *folderPath, *quota)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	_ = server.Serve()
}
