package foldermgr

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

const testFolder = "./testFolder"

const fileSize = 100
const quota = 1000

func getRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func getRandomUUID() string {
	id, _ := uuid.NewUUID()
	return id.String()
}

//Here, passing means not having any errors.
func TestFolderClean(t *testing.T)  {

	m, err := NewManagedFolder(quota, testFolder)
	if err != nil {
		t.Fatal(err)
	}

	idStr := getRandomUUID()

	success, err := m.AllocateSpace(context.Background(), idStr, fileSize)
	if err != nil {
		t.Fatal(err)
	}
	if !success {
		t.Fatalf("Couldn't allocate %v bytes on a %v byte managed folder", fileSize, quota)
	}

	writeFile, err := m.WriteToFile(idStr)
	if err != nil {
		t.Fatal(err)
	}
	data := getRandomData(fileSize)
	readData := make([]byte, fileSize)

	if _, err := writeFile.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writeFile.Close(); err != nil {
		t.Fatal(err)
	}
	readFile, err := m.ReadFile(idStr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readFile.Read(readData); err != nil {
		t.Fatal(err)
	}

	if err := readFile.Close(); err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(readData, data) != 0 {
		t.Fatal("Data from read and write weren't the same")
	}
}


//Test whether the folder reuse feature works correctly, both on a random folder and a good folder.
func TestFolderReuse (t *testing.T) {
	_, err := NewManagedFolder(quota, "./")
	log.Print(err)
	if err == nil {
		t.Fatal("Trying to create a folder for the current dir was successful, even though it contains code files.")
	}

	//Both operations should work thanks to the sentinel file

	_, err = NewManagedFolder(quota, testFolder)
	if err != nil {
		t.Fatal(err)
	}

	_ , err = NewManagedFolder(quota, testFolder)
	if err != nil {
		t.Fatal(err)
	}
}

//Test whether the file limits enforced by filemgr work correctly.
func TestFileLimits(t *testing.T) {
	m, err := NewManagedFolder(quota, testFolder)
	if err != nil {
		t.Fatal(err)
	}

	idStr := getRandomUUID()

	success, err := m.AllocateSpace(context.Background(), idStr, fileSize)
	if err != nil {
		t.Fatal(err)
	}
	if !success {
		t.Fatalf("Couldn't allocate %v bytes on a %v byte managed folder", fileSize, quota)
	}

	writeFile, err := m.WriteToFile(idStr)
	if err != nil {
		t.Fatal(err)
	}

	oversizedData := getRandomData(fileSize*2)
	undersizedData := getRandomData(fileSize*0.5)

	// If it failed correctly, this write should just be discarded.
	if _, err := writeFile.Write(oversizedData); err == nil {
		t.Fatalf("Writing %v bytes to an %v byte file was successful", len(oversizedData), fileSize)
	}

	if _, err := writeFile.Write(undersizedData); err != nil {
		t.Fatal(err)
	}

	if err := writeFile.Close(); err == nil {
		t.Fatalf("Closing a %v bytes file with only %v bytes written was successful", fileSize, len(undersizedData))
	}
}

func TestQuotaLimits(t *testing.T) {
	m, err := NewManagedFolder(quota, testFolder)
	if err != nil {
		t.Fatal(err)
	}

	success, err := m.AllocateSpace(context.Background(), getRandomUUID(), quota*2)
	if err != nil {
		t.Fatal(err)
	}
	if success {
		t.Fatalf("Allocated %v bytes on a %v byte managed folder", fileSize, quota)
	}

	ctx, cancel := context.WithCancel(context.Background())
	uuidToCancel := getRandomUUID()

	success, err = m.AllocateSpace(ctx, uuidToCancel, quota*0.7)

	if err != nil {
		t.Fatal(err)
	}
	if !success {
		t.Fatalf("Couldn't allocate %v bytes on a %v byte managed folder", fileSize, quota)
	}

	//Should fail due to exceeding quota
	success, err = m.AllocateSpace(context.Background(), getRandomUUID(), quota*0.5)
	if err != nil {
		t.Fatal(err)
	}

	if success {
		t.Fatalf("Exceeded quota by allocating %v bytes on a %v byte managed folder", quota * 1.2, quota)
	}

	cancel()

	//Give the goroutine some time to complete
	time.Sleep(time.Millisecond * 100)

	//Should succeed after cancelling because the allocation was freed
	success, err = m.AllocateSpace(context.Background(), getRandomUUID(), quota*0.5)
	if err != nil {
		t.Fatal(err)
	}

	if !success {
		t.Fatalf("Couldn't allocate %v bytes on an empty %v byte shared folder", quota * 0.5, quota)
	}

	_, err = m.WriteToFile(uuidToCancel)
	if err == nil {
		t.Fatal("Opening a write to a file was successful after cancelling its context")
	}

}

func TestMain(m *testing.M) {
	code := m.Run()
	err := os.RemoveAll(testFolder)
	if err != nil {
		log.Print(err)
	}

	os.Exit(code)
}