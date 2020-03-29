package master

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/plutoberth/Failsystem/core/minion/internal_minion"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"time"
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
	replicationFactor      = 3
	minionTimeout          = 3 * time.Second
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


func (s *server) allocateAndEmpower(ctx context.Context, servers []ServerEntry, size int64, fileuuid string) (empoweredServer *ServerEntry, err error) {
	//TODO: randomize order
	//TODO: Improve concurrency
	//This section could heavily benefit from connecting to the servers and allocating concurrently.
	//Doing this properly, especially with db updates, is extremely difficult, so it will be kept for a future version.
	for _, server := range servers {
		c, err := internal_minion.NewClient(ctx, server.Ip)
		if err != nil {
			if err == ctx.Err() {
				return nil, status.Errorf(codes.DeadlineExceeded, ctx.Err().Error())
			}
			continue
		}
		resp, err := c.Allocate(ctx, &pb.AllocationRequest{UUID: fileuuid, FileSize:size})
		if err != nil {
			if err == ctx.Err() {
				return nil, status.Errorf(codes.DeadlineExceeded, ctx.Err().Error())
			}
			_ = c.Close()
			continue
		}
		server.AvailableSpace = resp.GetAvailableSpace()
		if err := s.db.UpdateServerEntry(ctx, server); err != nil {
			log.Println("Failed to update db on initiate upload: ", err.Error())
			return nil, status.Errorf(codes.Internal, "Failed to update db")
		}
		if !resp.Allocated {
			_ = c.Close()
			continue
		}

	}
}

func (s *server) InitiateFileUpload(ctx context.Context, in *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {

	if in.GetFileSize() <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "File size must be greater than zero")
	}

	fileUUID := in.GetFileName()
	if _, err := uuid.Parse(fileUUID); err != nil {
		return nil, fmt.Errorf("\"%v\" is not a valid UUID", fileUUID)
	}
	servers, err := s.db.GetServersWithEnoughSpace(ctx, in.FileSize)

	if err != nil {
		log.Printf("Error when fetching servers with enough space: %v", err)
		return nil, status.Errorf(codes.Internal, "Database fetch failed")
	}

	if len(servers) < replicationFactor {
		return nil, status.Errorf(codes.ResourceExhausted, "Not enough servers for replication")
	}

	newuuid, err := uuid.NewUUID()
	if err != nil {
		log.Println("Failed to create UUID: ", err.Error())
		return nil, status.Errorf(codes.Internal, "Failed to create UUID")
	}
	fileuuid := newuuid.String()



}





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

func (s *server) FinalizeUpload(ctx context.Context, in *pb.FinalizeUploadMessage) (*pb.FinalizeUploadResponse, error) {
	panic("not implemented")
}
