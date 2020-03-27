package master

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//InternalClient interface defines methods that the caller may use to use the MinionToMaster grpc service.
type InternalClient interface {
	Heartbeat(ctx context.Context, uuid string, availableSpace int64) error
	Announce(ctx context.Context, announcement *pb.Announcement) error
	Close() error
}

type internalClient struct {
	conn   *grpc.ClientConn
	client pb.MinionToMasterClient
}

//NewInternalClient - Returns a InternalClient struct initialized with the string.
func NewInternalClient(ctx context.Context, address string) (InternalClient, error) {
	c := new(internalClient)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionToMasterClient(conn)
	return c, nil
}

func (c *internalClient) Close() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil //prevent double calls
	} else {
		err = errors.New("the connection was already closed, or it was never initialized")
	}
	return err
}

func (c *internalClient) Heartbeat(ctx context.Context, uuid string, availableSpace int64) error {
	_, err := c.client.Beat(ctx, &pb.Heartbeat{UUID: uuid, AvailableSpace: availableSpace})
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	return nil
}

func (c *internalClient) Announce(ctx context.Context, announcement *pb.Announcement) error {
	if _, err := c.client.Announce(ctx, announcement); err != nil {
		return fmt.Errorf("announcement failed: %w", err)
	}
	return nil
}
