package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/plutoberth/Failsystem/core"
)



var filename = flag.String("filename", "file.test", "The file that you want to upload.")
var port = flag.Uint("port", 1337, "The MinionServer's port.")

func main() {
	flag.Parse()

	address := fmt.Sprintf("localhost:%v", *port)

	minion, err := core.NewMinionClient(address)

	if err != nil {
		log.Fatalf("Dial Failure: %v", err)
	}
	defer minion.Close()

	//if err := minion.UploadFile(*filename); err != nil {
	//	fmt.Println(err)
	//}

	//if err := minion.DownloadFile("25acfdd3-0e00-4923-87a2-37e33766d680", "testme"); err != nil {
	//	fmt.Println(err)
	//}
}
