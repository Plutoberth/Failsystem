package streams

import (
	"errors"

	pb "github.com/plutoberth/Failsystem/model"
)

//FileChunkSender is a more generalized interface for FileChunk senders.
type FileChunkSender interface {
	Send(*pb.FileChunk) error
}

//FileChunkSenderWrapper exposes an io.Writer wrapper for sending files to the client
type FileChunkSenderWrapper interface {
	Write(p []byte) (n int, err error)
}

type fileChunkSenderWrapper struct {
	stream FileChunkSender
}

func NewFileChunkSenderWrapper(Stream FileChunkSender) FileChunkSenderWrapper {
	s := new(fileChunkSenderWrapper)
	s.stream = Stream

	return s
}

func (s *fileChunkSenderWrapper) Write(p []byte) (n int, err error) {
	if s.stream == nil {
		return 0, errors.New("wrapper must be initialized with stream")
	}
	if err := s.stream.Send(&pb.FileChunk{Content: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
