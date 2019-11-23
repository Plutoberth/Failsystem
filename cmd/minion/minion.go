package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/google/uuid"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	port = flag.Int("port", 1337, "The MinionServer's port.")
)

type minionServer struct {
	pb.UnimplementedMinionServer
}

func sha256Hasher(data <-chan []byte, res chan<- []byte) {
	h := sha256.New()
	for elem := range data {
		h.Write(elem)
	}
	res <- h.Sum(nil)
}

func (s *minionServer) UploadFile(stream pb.Minion_UploadFileServer) (err error) {
	var (
		file     *os.File
		filename string
	)

	if req, err := stream.Recv(); err == nil {
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_Chunk:
			return status.Errorf(codes.InvalidArgument, "Error! First Upload Request must be a UUID.")

		case *pb.UploadRequest_UUID:
			filename = data.UUID
		}
	} else {
		return status.Errorf(codes.Internal, "Error while receiving from stream.")
	}

	if _, err := uuid.Parse(filename); err != nil {
		fmt.Println(err.Error())
		return status.Errorf(codes.InvalidArgument, "Error! Invalid UUID")

	}

	if file, err = os.Create(filename); err != nil {
		//TODO: Add code to check if UUID is in queue and valid for the authed user.
		fmt.Println(filename, err)
		return status.Errorf(codes.Internal, "Error: Couldn't open file the file with that UUID.")
	}

	defer file.Close()

	input, res := make(chan []byte, 1), make(chan []byte)

	go sha256Hasher(input, res)

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return status.Errorf(codes.Unknown, "Error! Failed while reading from stream.")
			}
		}
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_UUID:
			return status.Errorf(codes.InvalidArgument, "Error! Subsequent upload requests must be chunks.")

		case *pb.UploadRequest_Chunk:
			c := data.Chunk.GetContent()
			if c == nil {
				return status.Errorf(codes.InvalidArgument, "Error! Content field must be populated.")
			}
			input <- c
			file.Write(c)
		}
	}

	close(input)

	hexHash := hex.EncodeToString(<-res)
	close(res)

	if err := stream.SendAndClose(&pb.UploadResponse{Type: pb.HashType_SHA256, HexHash: hexHash}); err != nil {
		return status.Errorf(codes.Internal, "Failed while sending response.")
	}
	return nil
}

func (s *minionServer) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) error {
	panic("not implemented")
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMinionServer(grpcServer, &minionServer{})
	grpcServer.Serve(lis)
}
