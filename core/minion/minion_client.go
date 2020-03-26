package minion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"

	"errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//Client interface defines methods that the caller may use to use the Minion grpc service.
type Client interface {
	Upload(uuid string) (io.WriteCloser, error)
	UploadByFilename(filepath string, uuid string) error
	DownloadFile(uuid string, targetFile string) error
	Close() error
}

type client struct {
	conn      *grpc.ClientConn
	client    pb.MinionClient
}

type uploadManager struct {
	hasher hash.Hash
	stream pb.Minion_UploadFileClient
}

var HashError = errors.New("data corrupted during transmission")

const chunkSize = 4096

//NewClient - Returns a Client struct initialized with the string.
func NewClient(address string) (Client, error) {
	c := new(client)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionClient(conn)
	return c, nil
}

func (c *client) Close() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil //prevent double calls
	} else {
		err = errors.New("the connection was already closed, or it was never initialized")
	}
	return err
}

func (u *uploadManager) Write(buf []byte) (n int, err error) {
	u.hasher.Write(buf)
	err = u.stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_Chunk{Chunk: &pb.FileChunk{Content: buf}}})

	if err != nil {
		err = fmt.Errorf("failed while uploading data to the server: %w", err)
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
		return fmt.Errorf("failed while closing stream: %w", err)
	}

	hexHash := hex.EncodeToString(u.hasher.Sum(nil))
	if hexHash != resp.GetHexHash() && resp.GetType() == pb.HashType_SHA256 {
		log.Printf("UploadFile hashing: %v != %v", hexHash, resp.GetHexHash())
		return HashError
	} else if resp.GetType() != pb.HashType_SHA256 {
		log.Printf("UploadFile hashing: Requested check for type %v", resp.GetType().String())
		return errors.New("dev didn't properly support different hashes")
	}

	return nil
}

func (c *client) Upload(uuid string) (io.WriteCloser, error) {
	stream, err := c.client.UploadFile(context.Background())
	if err != nil {
		return nil, err
	}

	if err = stream.Send(&pb.UploadRequest{Data: &pb.UploadRequest_UUID{UUID: uuid}}); err != nil {
		stream.CloseSend()
		return nil, fmt.Errorf( "failed when sending headers: %w", err)
	}

	return &uploadManager{
		hasher: sha256.New(),
		stream: stream,
	}, nil
}

func (c *client) UploadByFilename(filepath string, uuid string) (err error) {
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
	if _, err := io.CopyBuffer(uploadWriter, file, make([]byte, chunkSize)); err != nil {
		return err
	}

	return uploadWriter.Close()
}

func (c *client) DownloadFile(uuid string, targetFile string) (err error) {
	var (
		file *os.File
	)

	if file, err = os.Create(targetFile); err != nil {
		return fmt.Errorf("failed to open %v: %w", targetFile, err)
	}

	defer file.Close()

	stream, err := c.client.DownloadFile(context.Background(), &pb.DownloadRequest{
		UUID: uuid,
	})

	if err != nil {
		return fmt.Errorf( "failed while sending download request: %w", err)
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return fmt.Errorf( "failed while reading from stream: %w", err)
			}
		}
		c := req.Content
		if _, err = file.Write(c); err != nil {
			return fmt.Errorf("failed while writing to %v: %w", targetFile, err)
		}
	}

	return nil
}
