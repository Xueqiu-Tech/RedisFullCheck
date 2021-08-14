package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestShardTmp(t *testing.T) {

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
		"shard09",
		"shard10",
		"shard11",
		"shard12",
		"shard13",
		"shard14",
		"shard15",
		"shard16",
	}

	shardNameToAddr := map[string]string{
		"shard01": "10.10.28.2:12101",
		"shard02": "10.10.44.6:12105",
		"shard03": "10.10.50.7:12112",
		"shard04": "10.10.50.7:12115",
		"shard05": "10.10.26.3:12121",
		"shard06": "10.10.164.15:12125",
		"shard07": "10.10.30.9:12131",
		"shard08": "10.10.25.2:12135",
		"shard09": "10.10.25.3:12143",
		"shard10": "10.10.162.10:12145",
		"shard11": "10.10.28.3:12151",
		"shard12": "10.10.44.5:12155",
		"shard13": "10.10.45.5:12161",
		"shard14": "10.10.45.6:12165",
		"shard15": "10.10.162.9:12171",
		"shard16": "10.10.27.3:12175",
	}

	ConsistentHash, _ := NewConsistentHashing(shardNameList)

	fi, err := os.Open("/Users/zhantian/tian/sourceCode/RedisFullCheck/keyList")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()

	logFileName := "/Users/zhantian/tian/sourceCode/RedisFullCheck/new-sharding.sh"
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

func checkFileIsExist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
	   return false
	}
	return true
 }
