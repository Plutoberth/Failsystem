package main

import (
	"flag"
	"fmt"
	"github.com/plutoberth/Failsystem/core/minion"
	"log"
)

var port = flag.Uint("port", 1337, "The Server's port.")

func main() {
	flag.Parse()

	address := fmt.Sprintf("localhost:%v", *port)

	client, err := minion.NewClient(address)

	if err != nil {
		log.Fatalf("Dial Failure: %v", err)
	}
	defer client.Close()
}
