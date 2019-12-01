//Package omissions contains some functions that were weirdly left out of the Go programming language,
//such as min(int, int). Not so "simple" after all.
package omissions

//Min adds necessary stuff to go
func Min(a uint32, b uint32) uint32 {
	if a > b {
		return b
	}
	return a
}

//Max adds necessary stuff to go
func Max(a uint32, b uint32) uint32 {
	if a < b {
		return b
	}
	return a
}
