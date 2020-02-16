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
	UploadFile(filepath string) error
	DownloadFile(uuid string, targetFile string) error
	Close() error
}

type minionClient struct {
	conn      *grpc.ClientConn
	client    pb.MinionClient
	chunkSize int
}

const chunkSize = 4096

//NewMinionClient - Returns a MinionClient struct initialized with the string.
func NewMinionClient(address string, chunkSize int) (MinionClient, error) {
	c := new(minionClient)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionClient(conn)
	c.chunkSize = chunkSize
	return c, nil
}

func (c *minionClient) Close() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil //prevent double calls
	} else {
		err = errors.New("Connection has already been closed, or it was never initialized.")
	}
	return err
}

func (c *minionClient) UploadFile(filepath string) (err error) {
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

	buf := make([]byte, c.chunkSize)
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

func (c *minionClient) DownloadFile(uuid string, targetFile string) (err error) {
	var (
		file *os.File
	)

	if file, err = os.Create(targetFile); err != nil {
		return errors.Wrapf(err, "Failed to open %v", targetFile)
	}

	defer file.Close()

	stream, err := c.client.DownloadFile(context.Background(), &pb.DownloadRequest{
		UUID: uuid,
	})

	if err != nil {
		return errors.Wrap(err, "Failed while sending download request")
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.Wrap(err, "Failed while reading from stream")
			}
		}
		c := req.Content
		if _, err = file.Write(c); err != nil {
			return errors.Wrapf(err, "Failed while writing to %v", targetFile)
		}
	}

	return nil
}
