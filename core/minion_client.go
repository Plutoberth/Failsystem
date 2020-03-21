package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//MinionClient interface defines methods that the caller may use to use the Minion grpc service.
type MinionClient interface {
	Upload(uuid string) (io.WriteCloser, error)
	UploadByFilename(filepath string, uuid string) error
	DownloadFile(uuid string, targetFile string) error
	Close() error
}

type minionClient struct {
	conn      *grpc.ClientConn
	client    pb.MinionClient
	chunkSize int
}

type uploadManager struct {
	hasher hash.Hash
	stream pb.Minion_UploadFileClient
}



type HashError string

func (f HashError) Error() string {
	return string(f)
}

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

func (u *uploadManager) Write(buf []byte) (n int, err error) {
	u.hasher.Write(buf)
	err = u.stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_Chunk{Chunk: &pb.FileChunk{Content: buf}}})

	if err != nil {
		err = errors.Wrap(err, "Failed while uploading data to the server")
	} else {
		n = len(buf)
	}
	return n, err
}

func (u *uploadManager) Close() error {
	if err := u.stream.CloseSend(); err != nil {
		return err
	}

	resp, err := u.stream.CloseAndRecv()
	if err != nil {
		return errors.Wrap(err, "Failed while closing stream")
	}

	hexHash := hex.EncodeToString(u.hasher.Sum(nil))
	if hexHash != resp.GetHexHash() && resp.GetType() == pb.HashType_SHA256 {
		log.Printf("UploadFile hashing: %v != %v", hexHash, resp.GetHexHash())
		return HashError("data corrupted during transmission")
	} else if resp.GetType() != pb.HashType_SHA256 {
		log.Printf("UploadFile hashing: Requested check for type %v", resp.GetType().String())
		return errors.New("dev didn't properly support different hashes")
	}

	return nil
}

func (c *minionClient) Upload(uuid string) (io.WriteCloser, error) {
	stream, err := c.client.UploadFile(context.Background())
	if err != nil {
		return nil, err
	}

	if err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_UUID{UUID: uuid}}); err != nil {
		stream.CloseSend()
		return nil, errors.Wrap(err, "Failed when sending headers")
	}

	return &uploadManager{
		hasher: sha256.New(),
		stream: stream,
	}, nil
}

func (c *minionClient) UploadByFilename(filepath string, uuid string) (err error) {
	var (
		file *os.File
	)

	if file, err = os.Open(filepath); err != nil {
		return
	}
	defer file.Close()

	uploadWriter, err := c.Upload(uuid)
	if err != nil {
		return err
	}
	if _, err := io.CopyBuffer(uploadWriter, file, make([]byte, c.chunkSize)); err != nil {
		return err
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
