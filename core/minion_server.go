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
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	folder foldermgr.ManagedFolder
	server  *grpc.Server
}

const (
	defaultChunkSize uint32 = 2 << 16
	maxPort      uint   = 2 << 16 // 65536
)

//NewMinionServer - Initializes a new minion server.
func NewMinionServer(port uint, folderPath string, quota int64) (MinionServer, error) {
	s := new(minionServer)
	if port >= maxPort {
		return nil, errors.Errorf("port must be between 0 and %v", maxPort)
	}
	s.address = fmt.Sprintf("0.0.0.0:%v", port)

	folder, err  := foldermgr.NewManagedFolder(quota, folderPath)
	if err != nil {
		return nil, err
	}
	s.folder = folder

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

func (s *minionServer) UploadFile(stream pb.Minion_UploadFileServer) (err error) {
	var (
		file     *os.File
		filename string
		req      *pb.UploadRequest
	)

	if req, err = stream.Recv(); err != nil {
		return status.Errorf(codes.Internal, "Failed while receiving from stream")
	}
	switch data := req.GetData().(type) {
	case *pb.UploadRequest_Chunk:
		return status.Errorf(codes.InvalidArgument, "First Upload Request must be a UUID.")

	case *pb.UploadRequest_UUID:
		filename = data.UUID
	}

	if _, err := uuid.Parse(filename); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if file, err = os.Create(filename); err != nil {
		//TODO: Add code to check if UUID is in queue and valid for the user.
		return status.Errorf(codes.Internal, "Couldn't open a file with that UUID.")
	}

	defer file.Close()

	hasher := sha256.New()

	w := io.MultiWriter(hasher, file)

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return status.Errorf(codes.Unknown, "Failed while reading from stream.")
			}
		}
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_UUID:
			return status.Errorf(codes.InvalidArgument, "Subsequent upload requests must be chunks.")

		case *pb.UploadRequest_Chunk:
			c := data.Chunk.GetContent()
			if c == nil {
				return status.Errorf(codes.InvalidArgument, "Content field must be populated.")
			}

			if _, err := w.Write(c); err != nil {
				return status.Errorf(codes.Internal, "Unable to write to file")
			}
		}
	}

	hexHash := hex.EncodeToString(hasher.Sum(nil))

	if err := stream.SendAndClose(&pb.DataHash{Type: pb.HashType_SHA256, HexHash: hexHash}); err != nil {
		return status.Errorf(codes.Internal, "Failed while sending response.")
	}
	return nil
}

func (s *minionServer) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) (err error) {
	var (
		file *os.File
	)

	if _, err := uuid.Parse(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid UUID")
	}

	if file, err = os.Open(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "UUID not present")
	}

	chunkWriter := NewFileChunkSenderWrapper(stream)
	buf := make([]byte, defaultChunkSize)
	bytesWritten, err := io.CopyBuffer(chunkWriter, file, buf)
	log.Printf("Wrote %v bytes to chunkWriter", bytesWritten)
	return err
}

func (s *minionServer) Allocate(ctx context.Context, in *pb.AllocationRequest) (*pb.AllocationResponse, error) {
	//TODO: Add an option to specify a selected context
	allocationContext, _ := context.WithTimeout(context.Background(), time.Second * 10)
	success, err := s.folder.AllocateSpace(allocationContext, in.UUID, int64(in.FileSize))
	if err != nil {
		return nil, errors.Wrap(err, "Failed when allocating space")
	} else {
		return &pb.AllocationResponse{
			Allocated:            success,
			AvailableSpace:       s.folder.GetRemainingSpace(),
		}, nil
	}
}

