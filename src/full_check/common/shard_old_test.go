package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestOldShard(t *testing.T) {

	fmt.Printf("Test Shard Tmp.\n")

	opType := "EXISTS"

	shardNameList := []string{
		"shard01",
		"shard02",
		"shard03",
		"shard04",
		"shard05",
		"shard06",
		"shard07",
		"shard08",
	}

	shardNameToAddr := map[string]string{
		"shard01": "10.10.30.5:18601",
		"shard02": "10.10.30.6:18601",
		"shard03": "10.10.22.4:18601",
		"shard04": "10.10.30.8:18601",
		"shard05": "10.10.27.2:18601",
		"shard06": "10.10.36.3:18601",
		"shard07": "10.10.28.3:18602",
		"shard08": "10.10.37.3:18601",
	}

	ConsistentHash, _ := NewConsistentHashing(shardNameList)

	fi, err := os.Open("/Users/zhantian/tian/sourceCode/RedisFullCheck/keyList")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()

	logFileName := "/Users/zhantian/tian/sourceCode/RedisFullCheck/old-sharding.sh"
	var logFi *os.File
	if checkFileIsExist(logFileName) {
		logFi, _ = os.OpenFile(logFileName, os.O_APPEND, 0666)
	} else {
		logFi, _ = os.Create(logFileName)
	}
	defer logFi.Close()

	writers := bufio.NewWriter(logFi)

	br := bufio.NewReader(fi)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		_, name := ConsistentHash.GetShard(line)
		addr := shardNameToAddr[name]
		s := strings.Split(addr, ":")
		fmt.Printf("redis-cli -h %s -p %s %s %s\n", s[0], s[1], opType, line)
		n, _ := writers.WriteString(fmt.Sprintf("redis-cli -h %s -p %s %s %s\n", s[0], s[1], opType, line))
		fmt.Printf("write %d byte into file", n)
	}
	writers.Flush()
}
