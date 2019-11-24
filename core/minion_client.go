package core

import (
	"context"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//MinionClient interface defines methods that the caller may use to use the Minion grpc service.
type MinionClient interface {
	UploadFile(filepath string, chunksize int) error
	Close()
}

type minionClient struct {
	conn   *grpc.ClientConn
	client pb.MinionClient
}

//NewMinionClient - Returns a MinionClient struct initialized with the string.
func NewMinionClient(address string) (MinionClient, error) {
	c := new(minionClient)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionClient(conn)
	return c, nil
}

func (c *minionClient) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil //prevent double calls
	}
}

func (c *minionClient) UploadFile(filepath string, chunksize int) (err error) {
	var (
		file *os.File
	)

	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()

	stream, err := c.client.UploadFile(context.Background())
	if err != nil {
		return
	}
	defer stream.CloseSend()

	if err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_UUID{UUID: uuid.New().String()}}); err != nil {
		return errors.Wrap(err, "Failed when sending headers")
	}

	buf := make([]byte, chunksize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "Failed while copying file to buf")
		}
		err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_Chunk{Chunk: &pb.FileChunk{Content: buf[:n]}}})

		if err != nil {
			err = errors.Wrap(err, "Failed while uploading data to the server")
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "Failed while closing stream")
	}

	valid, err := VerifyUploadResponse(resp, file)
	if err != nil {
		return errors.Wrap(err, "Failed to hash")
	}

	if valid == false {
		return errors.New("data corrupted during transmission")
	}

	return nil
}
