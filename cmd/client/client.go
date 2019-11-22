package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/plutoberth/Failsystem/core"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

const (
	address   = "localhost:1337"
	timeout   = time.Millisecond * 3000
	chunksize = 1024
)

func initiateFileRead(client pb.MasterClient, req *pb.FileReadRequest) (resp *pb.FileReadResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err = client.InitiateFileRead(ctx, req)
	return
}

func initiateFileUpload(client pb.MasterClient, req *pb.FileUploadRequest) (resp *pb.FileUploadResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err = client.InitiateFileUpload(ctx, req)
	return
}

func testMinion(conn *grpc.ClientConn) (err error) {
	var (
		file *os.File
	)

	c := pb.NewMinionClient(conn)

	if file, err = os.Open("file.test"); err != nil {
		return
	}
	defer file.Close()

	stream, err := c.UploadFile(context.Background())
	if err != nil {
		return
	}
	defer stream.CloseSend()

	if err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_UUID{UUID: uuid.New().String()}}); err != nil {
		return errors.Wrapf(err, "Failed when sending headers.")
	}

	buf := make([]byte, chunksize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrapf(err, "Failed while copying file to buf")
		}
		err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_Chunk{Chunk: &pb.FileChunk{Content: buf[:n]}}})

		if err != nil {
			err = errors.Wrapf(err, "Failed while uploading data to the server")
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return errors.Wrapf(err, "Failed while closing stream")
	}

	valid, err := core.VerifyUploadResponse(resp, file)
	if err != nil {
		return errors.Wrapf(err, "Failed to hash")
	}

	if valid == false {
		return errors.New("data corrupted during transmission")
	}

	return nil
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Dial Failure: %v", err)
	}
	defer conn.Close()

	if err := testMinion(conn); err != nil {
		fmt.Println(err)
	}
}
