package master

import (
	"context"
	"errors"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//Client interface defines methods that the caller may use to use the Minion grpc service.
type Client interface {
	InitiateFileUpload(ctx context.Context, req *pb.FileUploadRequest) (*pb.FileUploadResponse, error)
	InitiateFileRead(ctx context.Context, req *pb.FileReadRequest) (*pb.FileReadResponse, error)
	ListFiles(ctx context.Context, in *pb.ListFilesRequest) (*pb.ListFilesResponse, error)
	Close() error
}

type client struct {
	conn   *grpc.ClientConn
	client pb.MasterClient
}


//NewClient - Returns a Client struct initialized with the string.
func NewClient(address string) (Client, error) {
	c := new(client)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMasterClient(conn)

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

func (c *client) InitiateFileUpload(ctx context.Context, req *pb.FileUploadRequest) (*pb.FileUploadResponse, error) {
	return c.client.InitiateFileUpload(ctx, req)
}

func (c *client) InitiateFileRead(ctx context.Context, req *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	return c.client.InitiateFileRead(ctx, req)
}

func (c *client) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	return c.client.ListFiles(ctx, req)
}
