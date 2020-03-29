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
	"math/rand"
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
	port   uint
	server *grpc.Server
	db     Datastore
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
	s.port = port
	s.db = db

	return s, nil
}

func (s *server) Serve() error {
	address := fmt.Sprintf("0.0.0.0:%v", s.port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMasterServer(s.server, s)
	pb.RegisterMinionToMasterServer(s.server, s)
	fmt.Println("Master is running at: ", address)
	err = s.server.Serve(lis)
	return err
}

func (s *server) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}

func (s *server) allocateAndEmpower(ctx context.Context, servers []ServerEntry, size int64, fileUUID string) (empoweredServer *ServerEntry, allocatedServers []ServerEntry, err error) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	allocatedServers = make([]ServerEntry, 0, replicationFactor)

	//TODO: Separate into functions
	//TODO: Improve concurrency
	//This section could heavily benefit from connecting to the servers and allocating concurrently.
	//Doing this properly, especially with db updates, is extremely difficult, so it will be kept for a future version.
	for _, server := range servers {
		c, err := internal_minion.NewClient(ctx, server.Ip)
		if err != nil {
			if err == ctx.Err() {
				return nil, nil, err
			}
			continue
		}
		resp, err := c.Allocate(ctx, &pb.AllocationRequest{UUID: fileUUID, FileSize: size})
		if err != nil {
			if err == ctx.Err() {
				return nil, nil, err
			}
			_ = c.Close()
			continue
		}
		server.AvailableSpace = resp.GetAvailableSpace()
		if err := s.db.UpdateServerEntry(ctx, server); err != nil {
			log.Println("Failed to update db on initiate upload: ", err.Error())
			return nil, nil, status.Errorf(codes.Internal, "Failed to update db")
		}

		if !resp.Allocated {
			_ = c.Close()
			continue
		}

		allocatedServers = append(allocatedServers, server)
		//If we already have enough servers, try to empower the last one
		if len(allocatedServers) == replicationFactor {
			serverIps := make([]string, 0, replicationFactor-1)
			for _, server := range allocatedServers[:replicationFactor-1] {
				serverIps = append(serverIps, server.Ip)
			}
			if _, err := c.Empower(ctx, &pb.EmpowermentRequest{
				UUID:         fileUUID,
				Subordinates: serverIps,
			}); err != nil {
				_ = c.Close()
				return &server, nil, nil
			} else {
				log.Printf("Error when empowering: %v", err)
				_ = c.Close()
				//Return the allocated server and exit the for loop
				return &server, allocatedServers, nil
			}
		}

		_ = c.Close()
	}
	return nil, nil, status.Errorf(codes.ResourceExhausted,
		"couldn't find enough servers, only %d/%d available", len(allocatedServers), replicationFactor)
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

	empoweredServer, allocatedServers, err := s.allocateAndEmpower(ctx, servers, in.GetFileSize(), fileuuid)
	if err != nil {
		return nil, err
	}

	serverUUIDs := make([]string, len(allocatedServers))
	for _, server := range allocatedServers {
		serverUUIDs = append(serverUUIDs, server.UUID)
	}

	if err := s.db.CreateFileEntry(ctx, FileEntry{
		UUID:        fileuuid,
		Name:        in.GetFileName(),
		Size:        in.GetFileSize(),
		ServerUUIDs: serverUUIDs,
		Available:   false,
	}); err != nil {
		log.Printf("Failed to create file entry on db: %v", err)
		return nil, status.Errorf(codes.Internal, "Failed to create file entry on db: %v", err)
	}

	return &pb.FileUploadResponse{
		UUID:              fileUUID,
		EmpoweredMinionIp: empoweredServer.Ip,
	}, nil
}

func (s *server) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	panic("implement me")
}

func (s *server) updateServerDetails(ctx context.Context, UUID string, availableSpace int64, port int32) error {
	caller, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "Couldn't fetch IP address for peer")
	}

	err := s.db.UpdateServerEntry(ctx, ServerEntry{
		UUID:           UUID,
		Ip:             fmt.Sprintf("%s:%d", caller.Addr.(*net.TCPAddr).IP.String(), port),
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
	return &pb.AnnounceResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace(), in.GetPort())
}

func (s *server) Beat(ctx context.Context, in *pb.Heartbeat) (*pb.HeartBeatResponse, error) {
	return &pb.HeartBeatResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace(), in.GetPort())
}

func (s *server) FinalizeUpload(ctx context.Context, in *pb.FinalizeUploadMessage) (*pb.FinalizeUploadResponse, error) {
	panic("not implemented")
}
