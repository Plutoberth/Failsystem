package minion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/plutoberth/Failsystem/core/master"
	"github.com/plutoberth/Failsystem/core/streams"
	"github.com/plutoberth/Failsystem/pkg/foldermgr"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"errors"
	"github.com/google/uuid"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//Server interface defines methods that the caller may use to host the Minion grpc service.
type Server interface {
	Serve() error
	Close()
}

type server struct {
	pb.UnimplementedMinionServer
	pb.UnimplementedMasterToMinionServer
	address       string
	uuid          string
	folder        foldermgr.ManagedFolder
	server        *grpc.Server
	masterAddress string

	mtx *sync.RWMutex

	//The empowerments field represents a very low severity memory leak. The empowerment never expires, so the application
	//will not delete the values if the UploadFile method is never called.
	empowerments map[string][]string
}

const (
	defaultChunkSize  uint32 = 2 << 16
	maxPort           uint   = 2 << 16 // 65536
	delayTime                = time.Second * 10
	uuidFile                 = "uuid"
	HeartbeatInterval        = time.Second * 4
)

//NewServer - Initializes a new minion server.
func NewServer(port uint, folderPath string, quota int64, masterAddress string) (Server, error) {
	s := new(server)
	if port >= maxPort {
		return nil, fmt.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)

	folder, err := foldermgr.NewManagedFolder(quota, folderPath)
	if err != nil {
		return nil, err
	}
	s.folder = folder
	s.masterAddress = masterAddress

	s.empowerments = make(map[string][]string)
	s.mtx = new(sync.RWMutex)

	if readUuid, err := ioutil.ReadFile(uuidFile); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		} else {
			log.Printf("Creating a new UUID file")
			uu, _ := uuid.NewUUID()
			if err = ioutil.WriteFile(uuidFile, []byte(uu.String()), 0600); err != nil {
				return nil, err
			}
			s.uuid = uu.String()
		}
	} else {
		if _, err := uuid.Parse(string(readUuid)); err == nil {
			s.uuid = string(readUuid)
		} else {
			return nil, fmt.Errorf("couldn't parse uuid file: %v", err)
		}
	}
	log.Printf("Server created on UUID: %s", s.uuid)
	return s, nil
}

func (s *server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMinionServer(s.server, s)
	pb.RegisterMasterToMinionServer(s.server, s)

	go s.heartbeatLoop()
	err = s.server.Serve(lis)

	return err
}

func (s *server) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}

func (s *server) announceToMaster() error {
	var announcement pb.Announcement
	announcement.UUID = s.uuid
	announcement.AvailableSpace = s.folder.GetRemainingSpace()
	resp, err := s.folder.ListFiles()
	if err != nil {
		return fmt.Errorf("CRITICAL: Failed to list files in folder: %v", err)
	}
	announcement.Entries = make([]*pb.FileEntry, 0, len(resp))
	for _, entry := range resp {
		announcement.Entries = append(announcement.Entries, &pb.FileEntry{
			UUID:     entry.Name(),
			FileSize: entry.Size(),
		})
	}

	ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
	mtmClient, err := master.NewInternalClient(ctx, s.masterAddress)
	if err != nil {
		return fmt.Errorf("CRITICAL: Failed to connect to the master (%v): %v", s.masterAddress, err)

	}
	defer func() {
		if err := mtmClient.Close(); err != nil {
			log.Printf("CRITICAL: Failed to close connection to master (%v): %v", s.masterAddress, err)
		}
	}()

	if err := mtmClient.Announce(context.Background(), &announcement); err != nil {
		return fmt.Errorf("CRITICAL: Failed to send an announcement to the master (%v): %v", s.masterAddress, err)
	}
	return nil
}

func (s *server) heartbeatLoop() {
	//Announce first
	if err := s.announceToMaster(); err != nil {
		log.Printf("%v", err)
	}
	for range time.Tick(HeartbeatInterval) {
		if s.server == nil {
			break
		}
		ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
		mtmClient, err := master.NewInternalClient(ctx, s.masterAddress)
		if err != nil {
			log.Printf("CRITICAL: Failed to connect to the master (%v): %v", s.masterAddress, err)
			continue
		}
		if err = mtmClient.Heartbeat(context.Background(), s.uuid, s.folder.GetRemainingSpace()); err != nil {
			log.Printf("CRITICAL: Failed to send a heartbeat to the master (%v): %v", s.masterAddress, err)
		}
		if err := mtmClient.Close(); err != nil {
			log.Printf("CRITICAL: Failed to close connection to master (%v): %v", s.masterAddress, err)
		}
	}
}

//Create the subordinate uploads
func (s *server) createSubUploads(uuid string) (subWriters []io.WriteCloser, minionClients []Client, err error) {
	s.mtx.RLock()
	subs := s.empowerments[uuid]
	delete(s.empowerments, uuid)
	s.mtx.RUnlock()
	if subs != nil {
		subWriters = make([]io.WriteCloser, len(subs))
		minionClients = make([]Client, len(subs))
		for i, sub := range subs {
			minionClient, err := NewClient(sub)
			minionClients[i] = minionClient
			if err != nil {
				log.Printf("Error when connecting to subordinate in UploadFile: %v", err)
				return nil, nil, status.Errorf(codes.Internal, "Couldn't connect to minion server")
			}

			subWriters[i], err = minionClient.Upload(uuid)
			if err != nil {
				return nil, nil, status.Errorf(codes.Internal, "Failed during replication process")
			}
		}
	}
	return
}

func (s *server) finalizeSubUploads(subWriters []io.WriteCloser, clients []Client) error {
	for _, subWriter := range subWriters {
		if err := subWriter.Close(); err != nil {
			if errors.Is(err, HashError) {
				return status.Errorf(codes.DataLoss, "Data corrupted among servers")
			} else {
				log.Printf("finalizeSubUploads: Failed when closing Minion upload - %v", err)
				return status.Errorf(codes.DataLoss, "Failed when finalizing replication")
			}
		}
	}

	for _, client := range clients {
		if err := client.Close(); err != nil {
			log.Printf("finalizeSubUploads: Close client - %v", err)
			return status.Errorf(codes.Internal, "Failed when finalizing replication")
		}
	}
	return nil
}

func (s *server) UploadFile(stream pb.Minion_UploadFileServer) (err error) {
	var (
		file          io.WriteCloser
		filename      string
		minionClients []Client
		subWriters    []io.WriteCloser
	)

	if req, err := stream.Recv(); err != nil {
		log.Printf("Error receving in UploadFile: %v", err.Error())
		return status.Errorf(codes.Internal, "Failed while receiving from stream")
	} else {
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_Chunk:
			return status.Errorf(codes.InvalidArgument, "First upload request must be a UUID")

		case *pb.UploadRequest_UUID:
			//TODO: Using this technique is inefficient CPU-wise. The hash is performed n times.
			//In practice, 99% of the time is in network IO, so it doesn't really matter.
			filename = data.UUID

			if _, err := uuid.Parse(filename); err != nil {
				return status.Errorf(codes.InvalidArgument, "Invalid UUID")
			}

			subWriters, minionClients, err = s.createSubUploads(filename)
			if err != nil {
				return err
			}
		}
	}


	if file, err = s.folder.WriteToFile(filename); err != nil {
		log.Printf("Failed to open file, %v", err)
		return status.Errorf(codes.Internal, "Couldn't open a file with that UUID")
	}

	hasher := sha256.New()
	//Internally, only one multiwriter will be created, concatenating previous multiwriters.
	w := io.MultiWriter(hasher, file)
	for _, subWriter := range subWriters {
		w = io.MultiWriter(w, subWriter)
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return status.Errorf(codes.Unknown, "Failed while reading from stream")
			}
		}
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_UUID:
			return status.Errorf(codes.InvalidArgument, "Subsequent upload requests must be chunks")

		case *pb.UploadRequest_Chunk:
			c := data.Chunk.GetContent()
			if c == nil {
				return status.Errorf(codes.InvalidArgument, "Content field must be populated")
			}

			if _, err := w.Write(c); err != nil {
				return status.Errorf(codes.Internal, "Unable to write to file")
			}
		default:
			log.Printf("Error when type switching on Server UploadFile, swithced on unexpected type. Value: %v", req.GetData())
			return status.Errorf(codes.InvalidArgument, "Unrecognized request data type")
		}
	}

	if err := stream.SendAndClose(&pb.DataHash{Type: pb.HashType_SHA256, HexHash: hex.EncodeToString(hasher.Sum(nil))}); err != nil {
		return status.Errorf(codes.Internal, "Failed while sending response")
	}

	if err := file.Close(); err != nil {
		log.Printf("UploadFile: Failed when closing - %v", err)
		return status.Error(codes.Internal, "Failed when finalizing the upload")
	}

	err = s.finalizeSubUploads(subWriters, minionClients)
	if err != nil {
		return err
	}

	//TODO: Notify master that the upload was successful

	return err
}

func (s *server) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) (err error) {
	var (
		file io.ReadCloser
	)

	if _, err := uuid.Parse(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if file, err = s.folder.ReadFile(req.GetUUID()); err != nil {
		return status.Errorf(codes.NotFound, "UUID not present")
	}

	chunkWriter := streams.NewFileChunkSenderWrapper(stream)
	buf := make([]byte, defaultChunkSize)
	bytesWritten, err := io.CopyBuffer(chunkWriter, file, buf)
	log.Printf("Wrote %v bytes to chunkWriter", bytesWritten)
	return err
}

func (s *server) Allocate(ctx context.Context, in *pb.AllocationRequest) (*pb.AllocationResponse, error) {
	//TODO: Add an option to specify a selected context in the request
	if _, err := uuid.Parse(in.GetUUID()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Filename must be a UUID")
	}

	allocationContext, _ := context.WithTimeout(context.Background(), delayTime)
	success, err := s.folder.AllocateSpace(allocationContext, in.GetUUID(), in.GetFileSize())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to allocate space in internal storage")
	} else {
		return &pb.AllocationResponse{
			Allocated:      success,
			AvailableSpace: s.folder.GetRemainingSpace(),
		}, nil
	}
}

func (s *server) Empower(ctx context.Context, in *pb.EmpowermentRequest) (*pb.EmpowermentResponse, error) {
	_, _ = ctx, in

	if _, err := uuid.Parse(in.GetUUID()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if !s.folder.CheckIfAllocated(in.GetUUID()) {
		return nil, status.Errorf(codes.FailedPrecondition, "The UUID doesn't have an allocation on the server")
	}

	subs := in.GetSubordinates()
	if subs == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Subordinates not sent")
	}

	for _, ip := range subs {
		if net.ParseIP(ip) == nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v is not a valid IP address", ip)
		}
	}

	s.mtx.Lock()
	s.empowerments[in.GetUUID()] = subs
	s.mtx.Unlock()

	return &pb.EmpowermentResponse{}, nil
}
