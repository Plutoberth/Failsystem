package main

import (
	"context"
	"fmt"
	"github.com/plutoberth/Failsystem/core/master"
	"log"
	"time"
)

func main() {
	fmt.Println("Connected to MongoDB!")
	mongoDal, err := master.NewMongoDatastore(context.Background(), "192.168.99.100:27017")
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}

	if err := mongoDal.UpdateServerEntry(master.ServerEntry{
		UUID:           "notauid45",
		LastIp:         "129.231.234.12",
		AvailableSpace: "30000000",
		LastUpdate:     time.Time{},
	}); err != nil {
		log.Fatal(err)
	}

	if res, err := mongoDal.GetServerEntry("notauid45"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("%+v", *res)
	}
}
