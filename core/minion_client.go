package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"

	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//MinionClient interface defines methods that the caller may use to use the Minion grpc service.
type MinionClient interface {
	UploadData(data io.Reader, uuid string) error
	UploadByFilename(filepath string, uuid string) error
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

func (c *minionClient) UploadData(data io.Reader, uuid string) (err error) {
	var hasher hash.Hash = sha256.New()
	stream, err := c.client.UploadFile(context.Background())
	if err != nil {
		return
	}
	defer stream.CloseSend()

	if err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_UUID{UUID: uuid}}); err != nil {
		return errors.Wrap(err, "Failed when sending headers")
	}

	buf := make([]byte, c.chunkSize)
	for {
		n, err := data.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "Failed while copying data to buf")
		}

		hasher.Write(buf[:n])
		err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_Chunk{Chunk: &pb.FileChunk{Content: buf[:n]}}})


		if err != nil {
			err = errors.Wrap(err, "Failed while uploading data to the server")
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "Failed while closing stream")
	}

	hexHash := hex.EncodeToString(hasher.Sum(nil))
	if hexHash != resp.GetHexHash() && resp.GetType() == pb.HashType_SHA256 {
		return errors.New("data corrupted during transmission")
	} else if resp.GetType() != pb.HashType_SHA256 {
		return errors.New("Dev didn't properly support different hashes")
	}

	return nil
}

func (c *minionClient) UploadByFilename(filepath string, uuid string) (err error) {
	var (
		file *os.File
	)

	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()

	return c.UploadData(file, uuid)
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
