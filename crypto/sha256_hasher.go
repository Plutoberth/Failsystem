package crypto

import "crypto/sha256"

//SHA256Hasher hashes data that comes in from a channel until the channel closes.
//When the channel closes, the checksum is sent to the res channel.
func SHA256Hasher(in <-chan []byte, res chan<- []byte) {
	h := sha256.New()
	for elem := range in {
		h.Write(elem)
	}
	res <- h.Sum(nil)
}
