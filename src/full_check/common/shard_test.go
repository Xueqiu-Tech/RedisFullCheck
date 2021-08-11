package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShard(t *testing.T) {

	fmt.Printf("Test Shard.\n")

	cases := []struct {
		Key   string
		ShardName string
	}{
		// key/value pair are generated by java murmurhash
		{"indicator:1m:QH_DELAY_SUIC", "master6"},
		{"indicator:2m:QH_DELAY_SUIC", "master7"},
		{"indicator:3m:QH_DELAY_SUIC", "master5"},
		{"indicator:4m:QH_DELAY_SUIC", "master6"},
		{"indicator:5m:QH_DELAY_SUIC", "master3"},
		{"indicator:6m:QH_DELAY_SUIC", "master7"},
		{"indicator:7m:QH_DELAY_SUIC", "master3"},
		{"indicator:8m:QH_DELAY_SUIC", "master2"},
		{"indicator:9m:QH_DELAY_SUIC", "master6"},
		{"indicator:10m:QH_DELAY_SUIC", "master4"},
		{"indicator:11m:QH_DELAY_SUIC", "master5"},
		{"indicator:12m:QH_DELAY_SUIC", "master2"},
		{"indicator:13m:QH_DELAY_SUIC", "master4"},
		{"indicator:14m:QH_DELAY_SUIC", "master7"},
		{"indicator:15m:QH_DELAY_SUIC", "master7"},
		{"indicator:16m:QH_DELAY_SUIC", "master6"},
		{"indicator:17m:QH_DELAY_SUIC", "master5"},
		{"indicator:18m:QH_DELAY_SUIC", "master7"},
		{"indicator:19m:QH_DELAY_SUIC", "master6"},
		{"indicator:20m:QH_DELAY_SUIC", "master0"},
		{"indicator:21m:QH_DELAY_SUIC", "master4"},
		{"indicator:22m:QH_DELAY_SUIC", "master0"},
		{"indicator:23m:QH_DELAY_SUIC", "master5"},
		{"indicator:24m:QH_DELAY_SUIC", "master0"},
		{"indicator:25m:QH_DELAY_SUIC", "master4"},
		{"indicator:26m:QH_DELAY_SUIC", "master4"},
		{"indicator:27m:QH_DELAY_SUIC", "master0"},
		{"indicator:28m:QH_DELAY_SUIC", "master4"},
		{"indicator:29m:QH_DELAY_SUIC", "master4"},
		{"indicator:30m:QH_DELAY_SUIC", "master3"},
		{"indicator:31m:QH_DELAY_SUIC", "master7"},
		{"indicator:32m:QH_DELAY_SUIC", "master5"},
		{"indicator:33m:QH_DELAY_SUIC", "master4"},
		{"indicator:34m:QH_DELAY_SUIC", "master6"},
		{"indicator:35m:QH_DELAY_SUIC", "master4"},
		{"indicator:36m:QH_DELAY_SUIC", "master4"},
		{"indicator:37m:QH_DELAY_SUIC", "master4"},
		{"indicator:38m:QH_DELAY_SUIC", "master1"},
		{"indicator:39m:QH_DELAY_SUIC", "master2"},
		{"indicator:40m:QH_DELAY_SUIC", "master1"},
		{"indicator:41m:QH_DELAY_SUIC", "master5"},
		{"indicator:42m:QH_DELAY_SUIC", "master2"},
		{"indicator:43m:QH_DELAY_SUIC", "master1"},
		{"indicator:44m:QH_DELAY_SUIC", "master0"},
		{"indicator:45m:QH_DELAY_SUIC", "master4"},
		{"indicator:46m:QH_DELAY_SUIC", "master5"},
		{"indicator:47m:QH_DELAY_SUIC", "master3"},
		{"indicator:48m:QH_DELAY_SUIC", "master4"},
		{"indicator:49m:QH_DELAY_SUIC", "master0"},
		{"indicator:50m:QH_DELAY_SUIC", "master5"},
		{"indicator:51m:QH_DELAY_SUIC", "master1"},
		{"indicator:52m:QH_DELAY_SUIC", "master4"},
		{"indicator:53m:QH_DELAY_SUIC", "master6"},
		{"indicator:54m:QH_DELAY_SUIC", "master5"},
		{"indicator:55m:QH_DELAY_SUIC", "master0"},
		{"indicator:56m:QH_DELAY_SUIC", "master4"},
		{"indicator:57m:QH_DELAY_SUIC", "master3"},
		{"indicator:58m:QH_DELAY_SUIC", "master7"},
		{"indicator:59m:QH_DELAY_SUIC", "master5"},
		{"indicator:60m:QH_DELAY_SUIC", "master4"},
		{"indicator:61m:QH_DELAY_SUIC", "master0"},
		{"indicator:62m:QH_DELAY_SUIC", "master6"},
		{"indicator:63m:QH_DELAY_SUIC", "master1"},
		{"indicator:64m:QH_DELAY_SUIC", "master6"},
		{"indicator:65m:QH_DELAY_SUIC", "master5"},
		{"indicator:66m:QH_DELAY_SUIC", "master1"},
		{"indicator:67m:QH_DELAY_SUIC", "master2"},
		{"indicator:68m:QH_DELAY_SUIC", "master5"},
		{"indicator:69m:QH_DELAY_SUIC", "master5"},
		{"indicator:70m:QH_DELAY_SUIC", "master5"},
		{"indicator:71m:QH_DELAY_SUIC", "master7"},
		{"indicator:72m:QH_DELAY_SUIC", "master1"},
		{"indicator:73m:QH_DELAY_SUIC", "master3"},
		{"indicator:74m:QH_DELAY_SUIC", "master2"},
		{"indicator:75m:QH_DELAY_SUIC", "master3"},
		{"indicator:76m:QH_DELAY_SUIC", "master7"},
		{"indicator:77m:QH_DELAY_SUIC", "master1"},
		{"indicator:78m:QH_DELAY_SUIC", "master6"},
		{"indicator:79m:QH_DELAY_SUIC", "master6"},
		{"indicator:80m:QH_DELAY_SUIC", "master4"},
		{"indicator:81m:QH_DELAY_SUIC", "master5"},
		{"indicator:82m:QH_DELAY_SUIC", "master2"},
		{"indicator:83m:QH_DELAY_SUIC", "master3"},
		{"indicator:84m:QH_DELAY_SUIC", "master4"},
		{"indicator:85m:QH_DELAY_SUIC", "master2"},
		{"indicator:86m:QH_DELAY_SUIC", "master1"},
		{"indicator:87m:QH_DELAY_SUIC", "master0"},
		{"indicator:88m:QH_DELAY_SUIC", "master4"},
		{"indicator:89m:QH_DELAY_SUIC", "master2"},
		{"indicator:90m:QH_DELAY_SUIC", "master5"},
		{"indicator:91m:QH_DELAY_SUIC", "master4"},
		{"indicator:92m:QH_DELAY_SUIC", "master1"},
		{"indicator:93m:QH_DELAY_SUIC", "master0"},
		{"indicator:94m:QH_DELAY_SUIC", "master5"},
		{"indicator:95m:QH_DELAY_SUIC", "master0"},
		{"indicator:96m:QH_DELAY_SUIC", "master2"},
		{"indicator:97m:QH_DELAY_SUIC", "master4"},
		{"indicator:98m:QH_DELAY_SUIC", "master3"},
		{"indicator:99m:QH_DELAY_SUIC", "master5"},
		{"indicator:100m:QH_DELAY_SUIC", "master5"},
	}

	shardNameList := []string {
		"master0",
		"master1",
		"master2",
		"master3",
		"master4",
		"master5",
		"master6",
		"master7",
	}
	ConsistentHash, _ := NewConsistentHashing(shardNameList)

	for _, c := range cases {
		_, name := ConsistentHash.GetShard([]byte(c.Key))
		assert.Equal(t, name, c.ShardName, "should be equal")
	}
}  
