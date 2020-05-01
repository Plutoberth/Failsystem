package master

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/plutoberth/Failsystem/core/master/masterdb"
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
	//these structs automatically implement new methods with dummy ones
	pb.UnimplementedMasterServer
	pb.UnimplementedMinionToMasterServer
	port   uint
	server *grpc.Server
	db     masterdb.Datastore
}

const (
	maxPort           uint = 2 << 16 // 65536
	replicationFactor      = 1
)

var UpdateDbFailed = status.Errorf(codes.Internal, "Failed to update database")
var AccessDbFailed = status.Errorf(codes.Internal, "Failed to access database")

//NewServer - Initializes a new master Server.
func NewServer(port uint, db masterdb.Datastore) (Server, error) {
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

func (s *server) allocateAndEmpower(ctx context.Context, servers []masterdb.ServerEntry, size int64, fileUUID string) (empoweredServer *masterdb.ServerEntry, allocatedServers []masterdb.ServerEntry, err error) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	allocatedServers = make([]masterdb.ServerEntry, 0, replicationFactor)

	//TODO: Separate into functions
	//TODO: Improve concurrency
	//This section could heavily benefit from connecting to the servers and allocating concurrently.
	//Doing this properly, especially with masterdb updates, is extremely difficult, so it will be kept for a future version.
	//Iterate over the servers, connecting to each one and making an allocation. If we finished allocating data, try to
	//empower the last server.
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
			log.Println("Failed to update masterdb on initiate upload: ", err.Error())
			return nil, nil, UpdateDbFailed
		}

		if !resp.Allocated {
			_ = c.Close()
			continue
		}

		allocatedServers = append(allocatedServers, server)
		//If we already have enough servers, try to empower the last one
		if len(allocatedServers) == replicationFactor {
			serverIps := make([]string, 0, replicationFactor-1)
			//Append all previous server ips for the empowered server
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
				//Exit early and return the allocated server and exit the for loop
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

	newuuid, err := uuid.NewUUID()
	if err != nil {
		log.Println("Failed to create UUID: ", err.Error())
		return nil, status.Errorf(codes.Internal, "Failed to create UUID")
	}
	fileUUID := newuuid.String()

	servers, err := s.db.GetServersWithEnoughSpace(ctx, in.FileSize)

	if err != nil {
		log.Printf("Error when fetching servers with enough space: %v", err)
		return nil, AccessDbFailed
	}

	if len(servers) < replicationFactor {
		return nil, status.Errorf(codes.ResourceExhausted, "Not enough servers for replication")
	}



	//Allocate space on servers and empower one of them
	empoweredServer, allocatedServers, err := s.allocateAndEmpower(ctx, servers, in.GetFileSize(), fileUUID)
	if err != nil {
		return nil, err
	}

	serverUUIDs := make([]string, len(allocatedServers))
	for _, server := range allocatedServers {
		serverUUIDs = append(serverUUIDs, server.UUID)
	}

	//Create a file entry in the db for book-keeping.
	if err := s.db.CreateFileEntry(ctx, masterdb.FileEntry{
		UUID:        fileUUID,
		Name:        in.GetFileName(),
		Size:        in.GetFileSize(),
		ServerUUIDs: serverUUIDs,
		Available:   false,
	}); err != nil {
		log.Printf("Failed to create file entry on masterdb: %v", err)
		return nil, UpdateDbFailed
	}

	return &pb.FileUploadResponse{
		UUID:              fileUUID,
		EmpoweredMinionIp: empoweredServer.Ip,
	}, nil
}

func (s *server) InitiateFileRead(ctx context.Context, in *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	fileUUID := in.GetUUID()
	if _, err := uuid.Parse(fileUUID); err != nil {
		return nil, fmt.Errorf("\"%v\" is not a valid UUID", fileUUID)
	}
	file, err := s.db.GetFileEntry(ctx, fileUUID)
	if err != nil {
		log.Printf("Failed to access masterdb on InitiateFileRead: %v", err)
		return nil, AccessDbFailed
	}

	//Simply get one of the alive servers that hold the file, and return it as a server to read from.
	pbHash := file.Hash.ToPb()
	return &pb.FileReadResponse{
		MinionServerIp: file.ServerUUIDs[rand.Intn(len(file.ServerUUIDs))],
		Hash:           &pbHash,
	}, nil
}

//A unified function to update the details in the database about a server.
func (s *server) updateServerDetails(ctx context.Context, UUID string, availableSpace int64, port int32) error {
	caller, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "Couldn't fetch IP address for peer")
	}

	err := s.db.UpdateServerEntry(ctx, masterdb.ServerEntry{
		UUID:           UUID,
		Ip:             fmt.Sprintf("%s:%d", caller.Addr.(*net.TCPAddr).IP.String(), port),
		AvailableSpace: availableSpace,
	})
	if err != nil {
		log.Printf("Failed when writing heartbeat to masterdb: %v", err)
		return UpdateDbFailed
	}
	return nil
}

func (s *server) Announce(ctx context.Context, in *pb.Announcement) (*pb.AnnounceResponse, error) {
	//UpdateFileHosts for all files, then update the rest of its details
	if in.GetEntries() != nil {
		for _, file := range in.GetEntries() {
			if err := s.db.UpdateFileHosts(ctx, file.GetUUID(), in.GetUUID()); err != nil {
				return nil, UpdateDbFailed
			}
		}
	}
	return &pb.AnnounceResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace(), in.GetPort())
}

func (s *server) Beat(ctx context.Context, in *pb.Heartbeat) (*pb.HeartBeatResponse, error) {
	return &pb.HeartBeatResponse{}, s.updateServerDetails(ctx, in.GetUUID(), in.GetAvailableSpace(), in.GetPort())
}

func (s *server) FinalizeUpload(ctx context.Context, in *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	err := s.db.FinalizeFileEntry(ctx, in.GetFileUUID(), masterdb.PbToDbHash(*in.GetHash()))
	if err != nil {
		return nil, UpdateDbFailed
	}
	return &pb.FinalizeUploadResponse{}, nil
}

func (s *server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	files, err := s.db.ListFiles(ctx)
	if err != nil {
		return nil, AccessDbFailed
	}
	respArr := make([]*pb.MasterFileEntry, 0, len(files))
	for _, file := range files {
		respArr = append(respArr, &pb.MasterFileEntry{
			UUID: file.UUID,
			Name: file.Name,
			Size: file.Size,
		})
	}
	return &pb.ListFilesResponse{
		Entries: respArr,
	}, nil
}
