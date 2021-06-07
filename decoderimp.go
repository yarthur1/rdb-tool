package rdb

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "strings"

    "github.com/yarthur1/rdb-tool/nopdecoder"
)

type keyInfo struct {
    key                 string
    keyType             string //key obj type
    size                uint64 //key size
    eleNums             uint64 //key element num
    quickListFlag       bool
    curZipCnt           uint64    //for calculate ziplist para
    listItemsZippedSize uint64    //for calculate ziplist para
    curZipSize          uint64    //for calculate ziplist para
    encodeType          ValueType //key enncoding type
    expiry              int64
}

type DecoderImp struct {
    db           int
    csvWriter    *bufio.Writer
    curKey       keyInfo
    redisVersion int
    logWriter    *log.Logger
    nopdecoder.NopDecoder
}

func (p *DecoderImp) Init(csvWriter *bufio.Writer, version int, logWriter *log.Logger) {
    p.csvWriter = csvWriter
    p.curKey = keyInfo{}
    p.redisVersion = version
    p.logWriter = logWriter
}

// StartDatabase is called when database n starts.
// Once a database starts, another database will not start until EndDatabase is called.
func (p *DecoderImp) StartDatabase(n int) {
    p.db = n
}

// Set is called once for each string key.
func (p *DecoderImp) Set(key, value []byte, expiry int64) {
    size := topLevelObjOverhead(key, expiry) + sizeOfString(value)
    len := stringElementNum(value)
    keyStr := strings.ReplaceAll(strings.ReplaceAll(string(key), ",", "["),"\n","]")
    lineStr := fmt.Sprintf("%d,string,%s,%d,%d,%d\n", p.db, keyStr, size, len, expiry)
    p.writeString(lineStr, key)
}

// StartList is called at the beginning of a list.
// Rpush will be called exactly length times before EndList.
// If length of the list is not known, then length is -1
func (p *DecoderImp) StartList(key []byte, length, expiry int64, encodeType ValueType, sizeZip uint64) {
    p.curKey.key = string(key)
    p.curKey.keyType = "list"
    p.curKey.size = topLevelObjOverhead(key, expiry)
    p.curKey.expiry = expiry
    if length == -1 { //Quicklist
        p.curKey.quickListFlag = true
        p.curKey.eleNums = 0
        p.curKey.curZipCnt = 1
        p.curKey.curZipSize = 0
        p.curKey.listItemsZippedSize = 0
    } else {
        p.curKey.eleNums = uint64(length)
    }
}

func (p *DecoderImp) Rpush(key, value []byte) {
    if p.curKey.quickListFlag {
        p.curKey.eleNums++
        sizeZip := ziplistEntryOverHead(value)
        if p.curKey.curZipSize+sizeZip > LIST_MAX_ZIPLIST_SIZE {
            p.curKey.curZipSize = sizeZip
            p.curKey.curZipCnt += 1
        } else {
            p.curKey.curZipSize += sizeZip
        }
        p.curKey.listItemsZippedSize += sizeZip
    }
}

// EndList is called when there are no more values in a list.
func (p *DecoderImp) EndList(key []byte) {
    if p.curKey.quickListFlag {
        p.curKey.size += quickListOverHead(p.curKey.curZipCnt) + zipListHeaderOverHead()*(p.curKey.curZipCnt) +
                (p.curKey.listItemsZippedSize)
        p.curKey.quickListFlag = false
    }
    keyStr := strings.ReplaceAll(strings.ReplaceAll(p.curKey.key, ",", "["),"\n","]")
    lineStr := fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", p.db, p.curKey.keyType, keyStr, p.curKey.size, p.curKey.eleNums, p.curKey.expiry)
    p.writeString(lineStr, key)
}

// StartHash is called at the beginning of a hash.
// Hset will be called exactly length times before EndHash.
func (p *DecoderImp) StartHash(key []byte, length, expiry int64, encodeType ValueType, sizeZip uint64) {
    p.curKey.key = string(key)
    p.curKey.eleNums = uint64(length)
    p.curKey.keyType = "hash"
    p.curKey.encodeType = encodeType
    p.curKey.size = topLevelObjOverhead(key, expiry) + sizeZip
    p.curKey.expiry = expiry
    if encodeType == TypeHash {
        p.curKey.size += hashTableOverHead(uint64(length), p.redisVersion)
    }
}

// Hset is called once for each field=value pair in a hash.
func (p *DecoderImp) Hset(key, field, value []byte) {
    if p.curKey.encodeType == TypeHash {
        p.curKey.size += sizeOfString(field) + sizeOfString(value) + hashTableEntryOverHead()
        if p.redisVersion < 4 {
            p.curKey.size += 2 * robjOverHead()
        }
    }
}

// EndHash is called when there are no more fields in a hash.
func (p *DecoderImp) EndHash(key []byte) {
    keyStr := strings.ReplaceAll(strings.ReplaceAll(p.curKey.key, ",", "["),"\n","]")
    lineStr := fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", p.db, p.curKey.keyType, keyStr, p.curKey.size, p.curKey.eleNums, p.curKey.expiry)
    p.writeString(lineStr, key)
}

// StartSet is called at the beginning of a set.
// Sadd will be called exactly cardinality times before EndSet.
// A set is exactly like a hashmap
func (p *DecoderImp) StartSet(key []byte, cardinality, expiry int64, encodeType ValueType, sizeZip uint64) {
    p.curKey.key = string(key)
    p.curKey.eleNums = uint64(cardinality)
    p.curKey.keyType = "set"
    p.curKey.encodeType = encodeType
    p.curKey.size = topLevelObjOverhead(key, expiry) + sizeZip
    p.curKey.expiry = expiry
    if encodeType == TypeSet {
        p.curKey.size += hashTableOverHead(uint64(cardinality), p.redisVersion)
    }
}

// Sadd is called once for each member of a set.
func (p *DecoderImp) Sadd(key, member []byte) {
    if p.curKey.encodeType == TypeSet {
        p.curKey.size += sizeOfString(member) + hashTableEntryOverHead()
        if p.redisVersion < 4 {
            p.curKey.size += robjOverHead()
        }
    }
}

// EndSet is called when there are no more fields in a set.
func (p *DecoderImp) EndSet(key []byte) {
    keyStr := strings.ReplaceAll(strings.ReplaceAll(p.curKey.key, ",", "["),"\n","]")
    lineStr := fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", p.db, p.curKey.keyType, keyStr, p.curKey.size, p.curKey.eleNums, p.curKey.expiry)
    p.writeString(lineStr, key)
}

// StartZSet is called at the beginning of a sorted set.
// Zadd will be called exactly cardinality times before EndZSet.
func (p *DecoderImp) StartZSet(key []byte, cardinality, expiry int64, encodeType ValueType, sizeZip uint64) {
    p.curKey.key = string(key)
    p.curKey.eleNums = uint64(cardinality)
    p.curKey.keyType = "sortedset"
    p.curKey.encodeType = encodeType
    p.curKey.size = topLevelObjOverhead(key, expiry) + sizeZip
    p.curKey.expiry = expiry
    if encodeType == TypeZSet || encodeType == TypeZSet2{
        p.curKey.size += skipListOverHead(uint64(cardinality), p.redisVersion)
    }
}

// Zadd is called once for each member of a sorted set.
func (p *DecoderImp) Zadd(key []byte, score float64, member []byte) {
    if p.curKey.encodeType == TypeZSet || p.curKey.encodeType == TypeZSet2{
        p.curKey.size += DOUBLE_SIZE + sizeOfString(member) + skipListEntryOverHead()
        if p.redisVersion < 4 {
            p.curKey.size += robjOverHead()
        }
    }
}

// EndZSet is called when there are no more members in a sorted set.
func (p *DecoderImp) EndZSet(key []byte) {
    keyStr := strings.ReplaceAll(strings.ReplaceAll(p.curKey.key, ",", "["),"\n","]")
    lineStr := fmt.Sprintf("%d,%s,%s,%d,%d,%d\n", p.db, p.curKey.keyType, keyStr, p.curKey.size, p.curKey.eleNums, p.curKey.expiry)
    p.writeString(lineStr, key)
}

// write string
func (p *DecoderImp) writeString(str string, key []byte) {
    _,err:=p.csvWriter.WriteString(str)
    if err != nil{
        p.logWriter.Printf("write csv file error: %v, %v \n", err,key)
        os.Exit(1)
    }
}