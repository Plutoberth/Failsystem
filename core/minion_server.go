package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/plutoberth/Failsystem/pkg/foldermgr"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"errors"
	"github.com/google/uuid"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//MinionServer interface defines methods that the caller may use to host the Minion grpc service.
type MinionServer interface {
	Serve() error
	Close()
}

type minionServer struct {
	pb.UnimplementedMinionServer
	pb.UnimplementedMasterToMinionServer
	address string
	folder  foldermgr.ManagedFolder
	server  *grpc.Server

	mtx *sync.RWMutex

	//The empowerments field represents a very low severity memory leak. The empowerment never expires, so the application
	//will not delete the values if the UploadFile method is never called.
	empowerments map[string][]string
}

const (
	defaultChunkSize uint32 = 2 << 16
	maxPort          uint   = 2 << 16 // 65536
	delayTime               = time.Second * 10
)

//NewMinionServer - Initializes a new minion server.
func NewMinionServer(port uint, folderPath string, quota int64) (MinionServer, error) {
	s := new(minionServer)
	if port >= maxPort {
		return nil, fmt.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)

	folder, err := foldermgr.NewManagedFolder(quota, folderPath)
	if err != nil {
		return nil, err
	}
	s.folder = folder

	s.empowerments = make(map[string][]string)
	s.mtx = new(sync.RWMutex)

	return s, nil
}

func (s *minionServer) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMinionServer(s.server, s)
	pb.RegisterMasterToMinionServer(s.server, s)
	err = s.server.Serve(lis)
	return err
}

func (s *minionServer) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}

//Create the subordinate uploads
func (s *minionServer) createSubUploads(uuid string) (subWriters []io.WriteCloser, minionClients []MinionClient, err error) {
	s.mtx.RLock()
	subs := s.empowerments[uuid]
	delete(s.empowerments, uuid)
	s.mtx.RUnlock()
	if subs != nil {
		subWriters = make([]io.WriteCloser, len(subs))
		minionClients = make([]MinionClient, len(subs))
		for i, sub := range subs {
			minionClient, err := NewMinionClient(sub)
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

func (s *minionServer) finalizeSubUploads(subWriters []io.WriteCloser, clients []MinionClient) error {
	for _, subWriter := range subWriters  {
		if err := subWriter.Close(); err != nil {
			if errors.Is(err, HashError) {
				return status.Errorf(codes.Internal, "Data corrupted among servers")
			} else {
				log.Printf("finalizeSubUploads: Failed when closing Minion upload - %v", err)
				return status.Errorf(codes.Internal, "Failed when finalizing replication")
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

func (s *minionServer) UploadFile(stream pb.Minion_UploadFileServer) (err error) {
	var (
		file     io.WriteCloser
		filename string
		minionClients []MinionClient
		subWriters     []io.WriteCloser
	)

	if req, err := stream.Recv(); err != nil {
		log.Printf("Error receving in UploadFile: %v", err.Error())
		return status.Errorf(codes.Internal, "Failed while receiving from stream")
	} else {
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_Chunk:
			return status.Errorf(codes.InvalidArgument, "First Upload Request must be a UUID")

		case *pb.UploadRequest_UUID:
			//TODO: Using this technique is inefficient CPU-wise. The hash is performed n times.
			//In practice, 99% of the time is in network IO, so it doesn't really matter.
			subWriters, minionClients, err = s.createSubUploads(data.UUID)
			if err != nil {
				return err
			}
			filename = data.UUID
		}
	}


	if _, err := uuid.Parse(filename); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if file, err = s.folder.WriteToFile(filename); err != nil {
		log.Printf("Failed to open file, %v", err)
		return status.Errorf(codes.Internal, "Couldn't open a file with that UUID")
	}

	hasher := sha256.New()
	//Internally, only one multiwriter will be created, concatenating previous multiwriters.
	w := io.MultiWriter(hasher, file)
	for _, subWriter := range subWriters  {
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
			log.Printf("Error when type switching on MinionServer UploadFile, swithced on unexpected type. Value: %v", req.GetData())
			return status.Errorf(codes.Internal, "Unrecognized request data type")
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

func (s *minionServer) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) (err error) {
	var (
		file io.ReadCloser
	)

	if _, err := uuid.Parse(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if file, err = s.folder.ReadFile(req.GetUUID()); err != nil {
		return status.Errorf(codes.NotFound, "UUID not present")
	}

	chunkWriter := NewFileChunkSenderWrapper(stream)
	buf := make([]byte, defaultChunkSize)
	bytesWritten, err := io.CopyBuffer(chunkWriter, file, buf)
	log.Printf("Wrote %v bytes to chunkWriter", bytesWritten)
	return err
}

func (s *minionServer) Allocate(ctx context.Context, in *pb.AllocationRequest) (*pb.AllocationResponse, error) {
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

func (s *minionServer) Empower(ctx context.Context, in *pb.EmpowermentRequest) (*pb.EmpowermentResponse, error) {
	_, _ = ctx, in

	if _, err := uuid.Parse(in.GetUUID()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if !s.folder.CheckIfAllocated(in.GetUUID()) {
		return nil, status.Errorf(codes.InvalidArgument, "The UUID doesn't have an allocation on the server")
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
