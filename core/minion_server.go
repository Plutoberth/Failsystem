package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
	. "github.com/plutoberth/Failsystem/pkg/omissions"
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
	address string
	server  *grpc.Server
}

const (
	maxChunkSize uint32 = 2 << 8  //256 B
	minChunkSize uint32 = 2 << 18 //256 KiB
)

//NewMinionServer - Initializes a new minion server.
func NewMinionServer(port uint) (MinionServer, error) {
	s := new(minionServer)
	if port >= 65536 {
		return s, errors.New("port must be between 0 and 65536")
	}
	s.address = fmt.Sprintf("localhost:%v", port)
	return s, nil
}

func (s *minionServer) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMinionServer(s.server, s)
	s.server.Serve(lis)
	return nil
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
		fmt.Println(err.Error())
		return status.Errorf(codes.InvalidArgument, "Error! Invalid UUID")

	}

	if file, err = os.Create(filename); err != nil {
		//TODO: Add code to check if UUID is in queue and valid for the user.
		fmt.Println(filename, err)
		return status.Errorf(codes.Internal, "Error: Couldn't open file the file with that UUID.")
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

	if err := stream.SendAndClose(&pb.UploadResponse{Type: pb.HashType_SHA256, HexHash: hexHash}); err != nil {
		return status.Errorf(codes.Internal, "Failed while sending response.")
	}
	return nil
}

func (s *minionServer) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) (err error) {
	var (
		file *os.File
	)

	if _, err := uuid.Parse(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "Download Request must contain a valid UUID")
	}

	if file, err = os.Open(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "uuid not present")
	}

	chunkSize := req.GetChunkSize()
	chunkSize = Max(minChunkSize, Min(chunkSize, maxChunkSize))
	buf := make([]byte, chunkSize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "Failed while copying file to buf")
		}
		err = stream.Send(&pb.FileChunk{Content: buf[:n]})

		if err != nil {
			err = errors.Wrap(err, "Failed while sending data to the client")
		}
	}

	return nil
}
