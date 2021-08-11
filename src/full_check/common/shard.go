package common

import (
	"fmt"
	"strconv"

	"github.com/emirpasic/gods/utils"
	"github.com/emirpasic/gods/maps/treemap"
)

const(
	Weight = 1
)

type ShardInfo struct {
	index int // targetClientList里的偏移，初始化时，顺序需要与targetClientList保持一致
	weight int
	name string
}

type ConsistentHashing struct {
	nodes *treemap.Map
}

func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

// Deprecated: Use GetShard instead.
func GetShardIndexOrig(key []byte, shardSize int) (int, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("key is empty")
	} 
	return int(abs(MurmurHash64A(key) % int64(shardSize))), nil
}

func NewConsistentHashing(shardNameList []string) (*ConsistentHashing, error) {
	mappingAlgorithm := new(ConsistentHashing)
	root := treemap.NewWith(utils.Int64Comparator)

	for i, shardName := range shardNameList {
		if len(shardName) == 0 {
			return nil, fmt.Errorf("shard name is empty")
		}

		shardInfo := ShardInfo {
			index: i,
			weight: Weight,
			name: shardName,
		}

		for n := 0; n < 160 * shardInfo.weight; n = n+1 {
			root.Put(MurmurHash(shardInfo.name + "*" + strconv.Itoa(shardInfo.weight) + strconv.Itoa(n)), shardInfo)
		}
	}

	mappingAlgorithm.nodes = root
	return mappingAlgorithm, nil
}

func (p *ConsistentHashing) GetMinShard() ShardInfo {
	_, min := p.nodes.Min()
	return min.(ShardInfo)
}

func (p *ConsistentHashing) GetShard(key []byte) (int, string) {
	_, node := p.nodes.Ceiling(MurmurHash64A(key))
	if node == nil {
		_, head := p.nodes.Min()
		shardInfo, err := head.(ShardInfo)
		if !err {
			panic(Logger.Errorf("ShardInfo conversion error"))
		}
		return shardInfo.index, shardInfo.name
	}

	shardInfo, err := node.(ShardInfo)
	if !err {
		panic(Logger.Errorf("ShardInfo conversion error"))
	}
	return shardInfo.index, shardInfo.name
}
