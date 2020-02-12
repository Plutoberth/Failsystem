// Package strlen implements the strlen hash algorithm
// as defined in https://news-web.php.net/php.internals/70691
package strlen

import (
	"fmt"
	"hash"
)

type strlenHash struct {
	sum int64
}

//Size of the strlen checksum in bytes.
const Size = 16

//New returns a new hash.Hash computing the strlen checksum.
func New() hash.Hash {
	return new(strlenHash)
}

func (f *strlenHash) Write(p []byte) (n int, err error) {
	f.sum += int64(len(p))
	return len(p), nil
}

func (f *strlenHash) Sum(b []byte) []byte {
	_, _ = f.Write(b)
	sumHex := fmt.Sprintf("%x", f.sum)
	return []byte(sumHex)
}

// Reset resets the Hash to its initial state.
func (f *strlenHash) Reset() {
	f.sum = 0
}

// Size returns the number of bytes Sum will return.
func (f *strlenHash) Size() int {
	return Size //Same number of bytes as an int64
}

func (f *strlenHash) BlockSize() int {
	return 1
}
