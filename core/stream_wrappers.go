package core

import (
	"github.com/pkg/errors"

	pb "github.com/plutoberth/Failsystem/model"
)

//FileChunkSender is a more generalized interface for FileChunk senders.
type FileChunkSender interface {
	Send(*pb.FileChunk) error
}

//FileChunkSenderWrapper exposes an io.Writer wrapper for sending files to the client
type FileChunkSenderWrapper interface {
	Write(p []byte) (n int, err error)
	Close() error
}

type fileChunkSenderWrapper struct {
	Stream FileChunkSender
}

func NewFileChunkSenderWrapper(Stream FileChunkSender) FileChunkSenderWrapper {
	s := new(fileChunkSenderWrapper)
	s.Stream = Stream

	return s
}

func (s *fileChunkSenderWrapper) Close() error {

}

func (s *fileChunkSenderWrapper) Write(p []byte) (n int, err error) {
	if s.Stream == nil {
		return 0, errors.New("Wrapper must be initialized with stream")
	}
	if err := s.Stream.Send(&pb.FileChunk{Content: p}); err != nil {
		return 0, errors.Wrapf(err, "Failed to send data back to stream")
	}
	return len(p), nil
}
