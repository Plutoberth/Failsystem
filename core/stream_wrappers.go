package core

import (
	"github.com/pkg/errors"
	pb "github.com/plutoberth/Failsystem/model"
)

//DownloadFileServerWrapper exposes an io.Writer wrapper for sending files to the client
type DownloadFileServerWrapper struct {
	Stream pb.Minion_DownloadFileServer
}

func (s *DownloadFileServerWrapper) Write(p []byte) (n int, err error) {
	if s.Stream == nil {
		return 0, errors.New("Wrapper must be initialized with stream")
	}
	if err := s.Stream.Send(&pb.FileChunk{Content: p}); err != nil {
		return 0, errors.Wrapf(err, "Failed to send data back to stream")
	}
	return len(p), nil
}
