package internal_master

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//Client interface defines methods that the caller may use to use the MinionToMaster grpc service.
type Client interface {
	Heartbeat(ctx context.Context, heartbeat *pb.Heartbeat) error
	Announce(ctx context.Context, announcement *pb.Announcement) error
	Close() error
}

type client struct {
	conn   *grpc.ClientConn
	client pb.MinionToMasterClient
}

//NewClient - Returns a Client struct initialized with the string.
func NewClient(ctx context.Context, address string) (Client, error) {
	c := new(client)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionToMasterClient(conn)
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

func (c *client) Heartbeat(ctx context.Context, heartbeat *pb.Heartbeat) error {
	_, err := c.client.Beat(ctx,heartbeat)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	return nil
}

func (c *client) Announce(ctx context.Context, announcement *pb.Announcement) error {
	if _, err := c.client.Announce(ctx, announcement); err != nil {
		return fmt.Errorf("announcement failed: %w", err)
	}
	return nil
}
