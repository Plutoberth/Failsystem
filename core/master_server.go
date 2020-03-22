package core

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"net"
	"sync"
)

//MasterServer interface defines methods that the caller may use to host the Master grpc service.
type MasterServer interface {
	Serve() error
	Close()
}

type masterServer struct {
	pb.UnimplementedMasterServer
	pb.UnimplementedMinionToMasterServer
	address string
	server  *grpc.Server

	mtx *sync.RWMutex
}

//NewMasterServer - Initializes a new master server.
func NewMasterServer(port uint, quota int64) (MasterServer, error) {
	s := new(masterServer)
	if port >= maxPort {
		return nil, errors.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)

	return s, nil
}

func (s *masterServer) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMasterServer(s.server, s)
	err = s.server.Serve(lis)
	return err
}

func (s *masterServer) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}


func (s *masterServer) InitiateFileUpload(ctx context.Context, in *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
	panic("implement me")
}

func (s *masterServer) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	panic("implement me")
}

func (s *masterServer) Announce(ctx context.Context, in *pb.Announcement, opts ...grpc.CallOption) (*pb.AnnounceResponse, error) {

	panic("implement me")
}
