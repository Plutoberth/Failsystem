package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 1337, "The server's port.")
)

type server struct {
	pb.UnimplementedMasterServer
}

func (s *server) InitiateFileUpload(ctx context.Context, in *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
	return &pb.FileUploadResponse{}, nil
}

func (s *server) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	fmt.Println("Got message: ", in.GetUUID())
	return &pb.FileReadResponse{}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &server{})
	grpcServer.Serve(lis)
}
