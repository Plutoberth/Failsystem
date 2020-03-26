package master

import (
	"context"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"net"
	"sync"
)

//Server interface defines methods that the caller may use to host the Master grpc service.
type Server interface {
	Serve() error
	Close()
}

type server struct {
	pb.UnimplementedMasterServer
	pb.UnimplementedMinionToMasterServer
	address string
	server  *grpc.Server

	mtx *sync.RWMutex
}

const maxPort uint = 2 << 16 // 65536

//NewServer - Initializes a new master Server.
func NewServer(port uint, quota int64) (Server, error) {
	s := new(server)
	if port >= maxPort {
		return nil, fmt.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)

	return s, nil
}

func (s *server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMasterServer(s.server, s)
	err = s.server.Serve(lis)
	return err
}

func (s *server) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}

func (s *server) InitiateFileUpload(ctx context.Context, in *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
	panic("implement me")
}

func (s *server) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	panic("implement me")
}

func (s *server) Announce(ctx context.Context, in *pb.Announcement, opts ...grpc.CallOption) (*pb.AnnounceResponse, error) {

	panic("implement me")
}
