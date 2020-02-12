package foldermgr

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"testing"
)

//This function shouldn't fail
func TestFolderClean(t *testing.T)  {
	const TestFolder = "./TestFolder"
	const fileSize = 100
	const quota = 1000
	//Cleanup
	defer os.RemoveAll(TestFolder)
	m, err := NewManagedFolder(quota, TestFolder)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := uuid.NewUUID()
	idStr := id.String()

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
	data := make([]byte, fileSize)
	readData := make([]byte, fileSize)
	rand.Read(data)

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

//TODO: Add a proper setup and teardown phase before writing the second test
//TODO: Add tests for
//* Reentrance (sentinel feature)
//* Writing too much (for a file)
//* Writing too little (for a file)
//* Exceeding quota (for the folder)
//* Cancelling the context (including reusing quota)
//* More?