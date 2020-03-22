package core

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/plutoberth/Failsystem/crypto/strlen"
	pb "github.com/plutoberth/Failsystem/model"
)

//VerifyDataHash checks if the response matches the file.
func VerifyDataHash(resp *pb.DataHash, file io.ReadSeeker) (bool, error) {
	var hasher hash.Hash

	switch resp.GetType() {

	case pb.HashType_SHA1:
		hasher = sha1.New()

	case pb.HashType_SHA256:
		hasher = sha256.New()

	case pb.HashType_STRLEN:
		hasher = strlen.New()

	default:
		return false, fmt.Errorf("hash type %v not supported", resp.GetType().String())
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	if _, err := io.Copy(hasher, file); err != nil {
		return false, err
	}

	//Is the hash valid?
	return hex.EncodeToString(hasher.Sum(nil)) == resp.GetHexHash(), nil
}

func CreateDataHash(file *os.File) (*pb.DataHash, error) {
	var hasher hash.Hash = sha256.New()

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	if _, err := io.Copy(hasher, file); err != nil {
		return nil, err
	}
	return &pb.DataHash{Type: pb.HashType_SHA256, HexHash: hex.EncodeToString(hasher.Sum(nil))}, nil
}
