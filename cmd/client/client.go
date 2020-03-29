package main

import (
	"flag"
	"fmt"
	"github.com/plutoberth/Failsystem/core/master"
	"log"
)

var port = flag.Uint("port", 1337, "The Master server's port.")

func main() {
	flag.Parse()

	address := fmt.Sprintf("localhost:%v", *port)

	client, err := master.NewClient(address)

	if err != nil {
		log.Fatalf("Dial Failure: %v", err)
	}
	defer client.Close()

}
