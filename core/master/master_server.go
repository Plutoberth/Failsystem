package master

import (
	"context"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
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
	db      Datastore
}

const (
	maxPort           uint = 2 << 16 // 65536
	replicationFactor uint = 3
)

//NewServer - Initializes a new master Server.
func NewServer(port uint, db Datastore) (Server, error) {
	s := new(server)
	if port >= maxPort {
		return nil, fmt.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)
	s.db = db

	return s, nil
}

func (s *server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMasterServer(s.server, s)
	pb.RegisterMinionToMasterServer(s.server, s)
	fmt.Println("Master is running at: ", s.address)
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

	return &pb.FileUploadResponse{}, nil
}

func (s *server) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	panic("implement me")
}

func (s *server) updateServerDetails(ctx context.Context, UUID string, availableSpace int64) error {
	caller, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "Couldn't fetch IP address for peer")
	}

	err := s.db.UpdateServerEntry(ctx, ServerEntry{
		UUID:           UUID,
		Ip:             caller.Addr.(*net.TCPAddr).IP.String(),
		AvailableSpace: availableSpace,
	})
	if err != nil {
		log.Printf("Failed when writing heartbeat to db: %v", err)
		return status.Errorf(codes.Internal, "Failed to update database")
	}
	return nil
}

func (s *server) Announce(ctx context.Context, in *pb.Announcement) (*pb.AnnounceResponse, error) {
	if in.GetEntries() != nil {
		for _, file := range in.GetEntries() {
			if err := s.db.UpdateFileHosts(ctx, file.GetUUID(), in.GetUUID()); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to update database")
			}
		}
	}
	return &pb.AnnounceResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace())
}

func (s *server) Beat(ctx context.Context, in *pb.Heartbeat) (*pb.HeartBeatResponse, error) {
	return &pb.HeartBeatResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace())
}
