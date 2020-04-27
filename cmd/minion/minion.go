package main

import (
	"flag"
	"github.com/plutoberth/Failsystem/core/minion"
	"log"
	"os"
)

var port = flag.Uint("port", 31337, "The Server's port.")
var quota = flag.Int64("quota", 10000, "The data folder's quota.")
var folderPath = flag.String("folder", "./dataFolder", "The data folder's path.")

func main() {
	// Write the filename and line number in the logs. Useful for debugging.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()
	var masterAddress string
	if masterAddress = os.Getenv("MASTER_ADDRESS"); masterAddress == "" {
		log.Printf("Running in local mode, master is on localhost")
		masterAddress = "127.0.0.1:1337"
	}
	server, err := minion.NewServer(*port, *folderPath, *quota, masterAddress)
	if err != nil {
		log.Fatalf("Failed while establishing server: %v", err)
	}

	_ = server.Serve()
}
