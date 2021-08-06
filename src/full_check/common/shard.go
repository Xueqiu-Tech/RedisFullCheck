package common

import (
	"fmt"
)

func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

func GetShardIndex(key []byte, shardSize int) (int, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("key is empty")
	} 
	return int(abs(MurmurHash64A(key) % int64(shardSize))), nil
}