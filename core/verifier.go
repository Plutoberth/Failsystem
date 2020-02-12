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

//VerifyUploadResponse checks if the response matches the file.
func VerifyUploadResponse(resp *pb.DataHash, file *os.File) (bool, error) {
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

	file.Seek(0, io.SeekStart)

	if _, err := io.Copy(hasher, file); err != nil {
		return false, err
	}

	//Is the hash valid?
	return hex.EncodeToString(hasher.Sum(nil)) == resp.GetHexHash(), nil
}
