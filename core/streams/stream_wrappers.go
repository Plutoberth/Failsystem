package streams

import (
	"errors"
	"io"

	pb "github.com/plutoberth/Failsystem/model"
)

//FileChunkSender is a more generalized interface for FileChunk senders.
type FileChunkSender interface {
	Send(*pb.FileChunk) error
}

type fileChunkSenderWrapper struct {
	stream FileChunkSender
}

//NewFileChunkSenderWrapper exposes an io.Writer wrapper for sending files to the client
func NewFileChunkSenderWrapper(Stream FileChunkSender) io.Writer {
	s := new(fileChunkSenderWrapper)
	s.stream = Stream

	return s
}

//Implemented for the io.Writer interface, this allows to easily send bytes to the stream
func (s *fileChunkSenderWrapper) Write(p []byte) (n int, err error) {
	if s.stream == nil {
		return 0, errors.New("wrapper must be initialized with stream")
	}
	if err := s.stream.Send(&pb.FileChunk{Content: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
