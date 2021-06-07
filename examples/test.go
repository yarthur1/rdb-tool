// This is a very basic example of a program that implements rdb.decoder and
// outputs a human readable diffable dump of the rdb file.
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

func main() {
	flag.StringVar(&logFile, "l", "./parse.log", "log file")
	flag.StringVar(&rdbFile, "rdb", "./dump.rdb", "rdb data file")
	flag.StringVar(&csvFile, "csv", "./mem.csv", "parsed csv data file")
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
