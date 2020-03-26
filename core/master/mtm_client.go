package master

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
)

//MTMClient interface defines methods that the caller may use to use the MinionToMaster grpc service.
type MTMClient interface {
	Heartbeat(uuid string, availableSpace int64) error
	Close() error
}

type mtmClient struct {
	conn   *grpc.ClientConn
	client pb.MinionToMasterClient
}


const chunkSize = 4096

//NewMTMClient - Returns a MTMClient struct initialized with the string.
func NewMTMClient(address string) (MTMClient, error) {
	c := new(mtmClient)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return c, err
	}
	c.conn = conn
	c.client = pb.NewMinionToMasterClient(conn)
	return c, nil
}

func (c *mtmClient) Close() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil //prevent double calls
	} else {
		err = errors.New("the connection was already closed, or it was never initialized")
	}
	return err
}

func (c *mtmClient) Heartbeat(uuid string, availableSpace int64) error {
	_, err := c.client.Beat(context.Background(), &pb.Heartbeat{UUID: uuid, AvailableSpace: availableSpace})
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	return nil
}

