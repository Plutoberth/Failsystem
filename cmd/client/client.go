package main

import (
	"flag"
	"fmt"
	"github.com/plutoberth/Failsystem/core/master"
	"log"
)

var port = flag.Uint("port", 1337, "The Master server's port.")

func main() {
	// Write the filename and line number in the logs. Useful for debugging.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	address := fmt.Sprintf("localhost:%v", *port)

	client, err := master.NewClient(address)

	if err != nil {
		log.Fatalf("Dial Failure: %v", err)
	}
	defer client.Close()

}
