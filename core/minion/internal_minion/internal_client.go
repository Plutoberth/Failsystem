package internal_minion

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//Client interface defines methods that the caller may use to use the MasterToMinion grpc service.
type Client interface {
	Allocate(ctx context.Context, req *pb.AllocationRequest) (*pb.AllocationResponse, error)
	Empower(ctx context.Context, req *pb.EmpowermentRequest) (*pb.EmpowermentResponse, error)
	Close() error
}

type client struct {
	conn   *grpc.ClientConn
	client pb.MasterToMinionClient
}

//NewClient - Returns a Client struct initialized with the string.
func NewClient(ctx context.Context, address string) (Client, error) {
	c := new(client)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMasterToMinionClient(conn)
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

func (c *client) Allocate(ctx context.Context, req *pb.AllocationRequest) (*pb.AllocationResponse, error) {
	resp, err := c.client.Allocate(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("allocation failed: %w", err)
	}
	return resp, nil
}

func (c *client) Empower(ctx context.Context, req *pb.EmpowermentRequest) (*pb.EmpowermentResponse, error) {
	resp, err := c.client.Empower(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("empowerment failed: %w", err)
	}
	return resp, nil
}
