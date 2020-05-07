package minion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/plutoberth/Failsystem/core/master/internal_master"
	"github.com/plutoberth/Failsystem/core/streams"
	"github.com/plutoberth/Failsystem/pkg/foldermgr"
	"github.com/plutoberth/Failsystem/pkg/minion"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"errors"
	"github.com/google/uuid"
	pb "github.com/plutoberth/Failsystem/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//Server interface defines methods that the caller may use to host the Minion grpc service.
type Server interface {
	Serve() error
	Close()
}

type server struct {
	//Implement new functions automatically for forwards compat
	pb.UnimplementedMinionServer
	pb.UnimplementedMasterToMinionServer
	port          uint
	uuid          string
	folder        foldermgr.ManagedFolder
	server        *grpc.Server
	masterAddress string

	//Protects all fields below the mutex
	mtx *sync.RWMutex

	//The empowerments field represents a very low severity memory leak. The empowerment never expires, so the application
	//will not delete the values if the UploadFile method is never called.
	empowerments map[string][]string
}

const (
	defaultChunkSize   uint32 = 1 << 16
	maxPort            uint   = 1 << 16 // 65536
	allocationLifetime        = time.Second * 10
	uuidFile                  = "uuid"
	HeartbeatInterval         = time.Second * 30
)

//NewServer - Initializes a new minion server.
func NewServer(port uint, folderPath string, quota int64, masterAddress string) (Server, error) {
	s := new(server)
	if port >= maxPort {
		return nil, fmt.Errorf("port must be between 0 and %v", maxPort)
	}
	s.port = port

	//Create a managed folder for file storage
	folder, err := foldermgr.NewManagedFolder(quota, folderPath)
	if err != nil {
		return nil, err
	}
	s.folder = folder
	s.masterAddress = masterAddress

	s.empowerments = make(map[string][]string)
	s.mtx = new(sync.RWMutex)

	//Check if we have a saved UUID file for server identification
	if readUuid, err := ioutil.ReadFile(uuidFile); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		} else {
			log.Printf("Creating a new UUID file")
			uu, _ := uuid.NewUUID()
			if err = ioutil.WriteFile(uuidFile, []byte(uu.String()), 0600); err != nil {
				return nil, err
			}
			s.uuid = uu.String()
		}
	} else {
		if _, err := uuid.Parse(string(readUuid)); err == nil {
			s.uuid = string(readUuid)
		} else {
			return nil, fmt.Errorf("couldn't parse uuid file: %v", err)
		}
	}
	log.Printf("Server created on UUID: %s", s.uuid)
	return s, nil
}

func (s *server) Serve() error {
	address := fmt.Sprintf("0.0.0.0:%v", s.port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMinionServer(s.server, s)
	pb.RegisterMasterToMinionServer(s.server, s)

	fmt.Println("Minion is running at: ", address)
	go s.heartbeatLoop()
	err = s.server.Serve(lis)

	return err
}

func (s *server) Close() {
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
}

//Announce our existence to the master.
//This includes our UUID and the list of current files.
func (s *server) announceToMaster() error {
	var announcement = pb.Announcement{
		UUID:           s.uuid,
		AvailableSpace: s.folder.GetRemainingSpace(),
		Port:           int32(s.port),
	}

	resp, err := s.folder.ListFiles()
	if err != nil {
		return fmt.Errorf("CRITICAL: Failed to list files in folder: %v", err)
	}
	announcement.Entries = make([]*pb.FileEntry, 0, len(resp))
	for _, entry := range resp {
		announcement.Entries = append(announcement.Entries, &pb.FileEntry{
			UUID:     entry.Name(),
			FileSize: entry.Size(),
		})
	}

	ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
	mtmClient, err := internal_master.NewClient(ctx, s.masterAddress)
	if err != nil {
		return fmt.Errorf("CRITICAL: Failed to connect to the master (%v): %v", s.masterAddress, err)
	}
	defer func() {
		if err := mtmClient.Close(); err != nil {
			log.Printf("CRITICAL: Failed to close connection to master (%v): %v", s.masterAddress, err)
		}
	}()

	if err := mtmClient.Announce(context.Background(), &announcement); err != nil {
		return fmt.Errorf("CRITICAL: Failed to send an announcement to the master (%v): %v", s.masterAddress, err)
	}
	return nil
}

//This loop notifies the master that we're still alive every so often. If we're not alive, the master can
//remove us from its lists.
func (s *server) heartbeatLoop() {
	//Announce first
	if err := s.announceToMaster(); err != nil {
		log.Printf("%v", err)
	}
	for range time.Tick(HeartbeatInterval) {
		if s.server == nil {
			break
		}
		ctx, _ := context.WithTimeout(context.Background(), HeartbeatInterval)
		mtmClient, err := internal_master.NewClient(ctx, s.masterAddress)
		if err != nil {
			log.Printf("CRITICAL: Failed to connect to the master (%v): %v", s.masterAddress, err)
			continue
		}
		if err = mtmClient.Heartbeat(ctx, &pb.Heartbeat{
			UUID:           s.uuid,
			AvailableSpace: s.folder.GetRemainingSpace(),
			Port:           int32(s.port),
		}); err != nil {
			log.Printf("CRITICAL: Failed to send a heartbeat to the master (%v): %v", s.masterAddress, err)
		}
		if err := mtmClient.Close(); err != nil {
			log.Printf("CRITICAL: Failed to close connection to master (%v): %v", s.masterAddress, err)
		}
	}
}

//Create the subordinate uploads.
//The system is designed so that the minion receives uploads from the users, and channels them to the replication
//minions.
func (s *server) createSubUploads(subs []string, uuid string) (subWriters []io.WriteCloser, minionClients []minion.Client, err error) {
	s.mtx.RLock()
	s.mtx.RUnlock()
	if subs != nil {
		subWriters = make([]io.WriteCloser, len(subs))
		minionClients = make([]minion.Client, len(subs))
		for i, sub := range subs {
			minionClient, err := minion.NewClient(sub)
			minionClients[i] = minionClient
			if err != nil {
				log.Printf("Failed to connect to subordinate: %v", err)
				return nil, nil, status.Errorf(codes.Internal, "Couldn't connect to minion server")
			}

			subWriters[i], err = minionClient.Upload(uuid)
			if err != nil {
				return nil, nil, status.Errorf(codes.Internal, "Failed during replication process")
			}
		}
	}
	return
}

//Finalizes the subuploads and verifies that their hashes match.
func (s *server) finalizeSubUploads(subWriters []io.WriteCloser, clients []minion.Client) error {
	for _, subWriter := range subWriters {
		if err := subWriter.Close(); err != nil {
			if errors.Is(err, minion.HashError) {
				return status.Errorf(codes.DataLoss, "Data corrupted among servers")
			} else {
				log.Printf("Failed when closing Minion upload: %v", err)
				return status.Errorf(codes.DataLoss, "Failed when finalizing replication")
			}
		}
	}

	for _, client := range clients {
		if err := client.Close(); err != nil {
			log.Printf("Failed when closing the Minion client: %v", err)
			return status.Errorf(codes.Internal, "Failed when finalizing replication")
		}
	}
	return nil
}

//Uploads a file. This function automatically checks whether the upload is for a file for which this server is
//empowered, and channels the upload if it is.
func (s *server) UploadFile(stream pb.Minion_UploadFileServer) (err error) {
	var (
		file          io.WriteCloser
		filename      string
		minionClients []minion.Client
		subWriters    []io.WriteCloser
		amEmpowered   = false
	)

	if req, err := stream.Recv(); err != nil {
		log.Printf("Recv error: %v", err.Error())
		return status.Errorf(codes.Internal, "Failed while receiving from stream")
	} else {
		//First upload request must be a UUID to mark the file.
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_Chunk:
			return status.Errorf(codes.InvalidArgument, "First upload request must be a UUID")

		case *pb.UploadRequest_UUID:
			//TODO: Using this technique is inefficient CPU-wise. The hash is performed n times.
			//In practice, 99% of the time is in network IO, so it doesn't really matter.
			filename = data.UUID

			if _, err := uuid.Parse(filename); err != nil {
				return status.Errorf(codes.InvalidArgument, "Filename must be a UUID")
			}

			//If we're empowered for that file, create the subuploads and remember it.
			subIps, ok := s.empowerments[filename]
			if ok {
				subWriters, minionClients, err = s.createSubUploads(subIps, filename)
				amEmpowered = true
			}

			if err != nil {
				return err
			}
		}
	}

	//Get a file handle to write to
	if file, err = s.folder.WriteToFile(filename); err != nil {
		log.Printf("Failed to open file, %v", err)
		return status.Errorf(codes.Internal, "Couldn't open a file with that UUID")
	}

	hasher := sha256.New()
	//Internally, all multiwriters will be concatenated into a single multiwriter.
	w := io.MultiWriter(hasher, file)
	for _, subWriter := range subWriters {
		w = io.MultiWriter(w, subWriter)
	}

	//Until the stream is over, receive all upload chunks and write them to the files simultaneously.
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return status.Errorf(codes.Unknown, "Failed while reading from stream")
			}
		}
		switch data := req.GetData().(type) {
		case *pb.UploadRequest_UUID:
			return status.Errorf(codes.InvalidArgument, "Subsequent upload requests must be chunks")

		case *pb.UploadRequest_Chunk:
			c := data.Chunk.GetContent()
			if c == nil {
				return status.Errorf(codes.InvalidArgument, "Content field must be populated")
			}

			if _, err := w.Write(c); err != nil {
				log.Printf("Failed to write to the multiwriter with error: %v", err)
				return status.Errorf(codes.Internal, "Unable to write to file")
			}
		default:
			log.Printf("Error when type switching, switched on unexpected type. value: %v", req.GetData())
			return status.Errorf(codes.InvalidArgument, "Unrecognized request data type")
		}
	}

	//get hash result
	hash := &pb.DataHash{Type: pb.HashType_SHA256, HexHash: hex.EncodeToString(hasher.Sum(nil))}
	if err := stream.SendAndClose(hash); err != nil {
		return status.Errorf(codes.Internal, "Failed while sending response")
	}

	if err := file.Close(); err != nil {
		log.Printf("Failed while closing the file: %v", err)
		return status.Error(codes.Internal, "Failed while closing the file")
	}

	if amEmpowered {
		err = s.finalizeSubUploads(subWriters, minionClients)
		if err != nil {
			return err
		}

		//Notify the master that the upload was successful
		master, err := internal_master.NewClient(stream.Context(), s.masterAddress)
		if err != nil {
			log.Printf("Failed when dialing to master: %v", err)
			return status.Errorf(codes.Internal, "Failed while finalizing upload")
		}
		defer master.Close()
		if err := master.FinalizeUpload(stream.Context(), &pb.FinalizeUploadRequest{
			ServerUUID: s.uuid,
			FileUUID:   filename,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *server) DownloadFile(req *pb.DownloadRequest, stream pb.Minion_DownloadFileServer) (err error) {
	var (
		file io.ReadCloser
	)

	if _, err := uuid.Parse(req.GetUUID()); err != nil {
		return status.Errorf(codes.InvalidArgument, "Filename must be a UUID")
	}

	//Check if we have this file in store
	if file, err = s.folder.ReadFile(req.GetUUID()); err != nil {
		return status.Errorf(codes.NotFound, "File not present")
	}

	//Use the wrapper to send the files chunks directly using io.copy
	chunkWriter := streams.NewFileChunkSenderWrapper(stream)
	buf := make([]byte, defaultChunkSize)
	bytesWritten, err := io.CopyBuffer(chunkWriter, file, buf)
	log.Printf("Wrote %v bytes to chunkWriter", bytesWritten)
	return err
}

func (s *server) Allocate(ctx context.Context, in *pb.AllocationRequest) (*pb.AllocationResponse, error) {
	//TODO: Add an option to specify a selected context in the request
	if _, err := uuid.Parse(in.GetUUID()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Filename must be a UUID")
	}

	//Define ample delay for the automatic deletion, as a context
	allocationContext, _ := context.WithTimeout(context.Background(), allocationLifetime)
	success, err := s.folder.AllocateSpace(allocationContext, in.GetUUID(), in.GetFileSize())
	if err != nil {
		log.Println(err.Error())
		return nil, status.Errorf(codes.Internal, "Failed to allocate space in internal storage")
	} else {
		log.Printf("Allocated %v bytes for %v", in.GetFileSize(), in.GetUUID())
		return &pb.AllocationResponse{
			Allocated:      success,
			AvailableSpace: s.folder.GetRemainingSpace(),
		}, nil
	}
}

func (s *server) Empower(ctx context.Context, in *pb.EmpowermentRequest) (*pb.EmpowermentResponse, error) {
	_, _ = ctx, in

	if _, err := uuid.Parse(in.GetUUID()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Filename must be a UUID")
	}

	if !s.folder.CheckIfAllocated(in.GetUUID()) {
		return nil, status.Errorf(codes.FailedPrecondition, "The UUID doesn't have an allocation on the server")
	}

	subs := in.GetSubordinates()
	if subs == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Subordinates not sent")
	}

	for _, ip := range subs {
		if net.ParseIP(ip) == nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v is not a valid IP address", ip)
		}
	}

	s.mtx.Lock()

	//If it was already empowered for that UUID
	if _, ok := s.empowerments[in.GetUUID()]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "this server is already empowered for this UUID")
	}
	//Add subordinates to empowerments list
	s.empowerments[in.GetUUID()] = subs
	s.mtx.Unlock()

	return &pb.EmpowermentResponse{}, nil
}
