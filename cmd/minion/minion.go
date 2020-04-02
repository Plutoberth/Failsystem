package main

import (
	"flag"
	"github.com/plutoberth/Failsystem/core/minion"
	"log"
)

var port = flag.Uint("port", 31337, "The Server's port.")
var quota = flag.Int64("quota", 10000, "The data folder's quota.")
var folderPath = flag.String("folder", "./dataFolder", "The data folder's path.")

func main() {
	flag.Parse()
	server, err := minion.NewServer(*port, *folderPath, *quota, "192.168.99.100:1337")
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	_ = server.Serve()
}
