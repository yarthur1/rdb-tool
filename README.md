## Parse rdb file and Analyze Memory

The tool is for redis Memory Report like [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools). The tool consists of three parts: parse rdb, count keys' information and generate csv report file. Parse rdb section is based on [BrotherGao/RDB](https://github.com/BrotherGao/RDB), the statistics section references  [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools).

The tool is almostly 3-10 times faster than [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools) when parse and calculate all keys in rdb file.


RDB parse modification：
*  skip module and stream keys' parse

Statistics section statistics
*  the calculation of string keys' size is different(reference [pull 176](https://github.com/sripathikrishnan/redis-rdb-tools/pull/176))

## example
reference examples/test.go
```go
package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"path"
	"time"

	"github.com/yarthur1/rdb-tool"

	"github.com/natefinch/lumberjack"
)

func decodeRDB(csvPath string, rdbPath string, logWriter *log.Logger, version int) {
	start := time.Now()
	csvFile, err := os.OpenFile(csvPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		logWriter.Printf("create csv file error: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()
	csvWriter := bufio.NewWriterSize(csvFile, 1<<20) //1MB

	deco := &rdb.DecoderImp{}  //use DecoderImp to parse rdb and calculate key size
	deco.Init(csvWriter, version, logWriter)

	rdbRead, err := os.Open(rdbPath)
	if err != nil {
		logWriter.Printf("open rdb file error: %v\n", err)
		os.Exit(1)
	}
	err = rdb.Decode(rdbRead, deco)
	if err != nil {
		logWriter.Printf("decode rdb file error: %v\n", err)
		os.Exit(1)
	}
	csvWriter.Flush()
	csvFile.Sync()
	elapsed := time.Since(start)
	logWriter.Println("decode rdb file time: ", elapsed)
}

var logFile string = "./parse.log"
var rdbFile string = "./dump.rdb"
var csvFile string = "./mem.csv"
var version int = 3

func main() {
	flag.StringVar(&logFile, "l", "./parse.log", "log file")
	flag.StringVar(&rdbFile, "rdb", "./dump.rdb", "rdb data file")
	flag.StringVar(&csvFile, "csv", "./mem.csv", "parsed csv data file")
	flag.IntVar(&version, "v", 3, "redis version 3-6 such as 3.xx=3, 4.xx=4, 5.xx=5")
	flag.Parse()

	os.MkdirAll(path.Dir(logFile), 0755)
	hook := &lumberjack.Logger{
		Filename:   logFile, //filePath
		MaxSize:    50,       // megabytes
		MaxBackups: 1,
		MaxAge:     30,    //days
		Compress:   false, // disabled by default
	}
	defer hook.Close()
	logWriter := log.New(hook, "", log.LstdFlags)

	//redis version 3-6, such as 3.xx=3, 4.xx=4, 5.xx=5
	decodeRDB(csvFile, rdbFile, logWriter, 3)
}

```
csv file format：dbnum,key type,key,size,elements,expiry

expiry unit is ms, 0 present no expiry 
```powershell
0,sortedset,klose,96,1,0
2,string,klose_ttl,88,8,1545854316442
11,string,klose,56,8,0
11,list,list,169,10,0
```

