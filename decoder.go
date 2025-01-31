// Package decode implements parsing of the Redis RDB file format.
/*
1.modified Decoder interface func StartHash StartSet StartList StartZSet(add paras encodetype and sizezip)
2.const Version = 6  from cupcake rdb/encoder.go
3.ZSET version 2 with doubles stored in binary.(not used)
4.modified read func readZipmap readZiplist readIntset readZiplistZset readZiplistHash(add paras encodetype)
5.skip module and stream parse
*/

package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/yarthur1/rdb-tool/crc64"
)

//from cupcake rdb/encoder.go
const Version = 6

// A Decoder must be implemented to parse a RDB file.
type Decoder interface {
	// StartRDB is called when parsing of a valid RDB file starts.
	StartRDB()
	// StartDatabase is called when database n starts.
	// Once a database starts, another database will not start until EndDatabase is called.
	StartDatabase(n int)
	// AUX field
	Aux(key, value []byte)
	// ResizeDB hint
	ResizeDatabase(dbSize, expiresSize uint64)
	// Set is called once for each string key.
	Set(key, value []byte, expiry int64)
	// StartHash is called at the beginning of a hash.
	// Hset will be called exactly length times before EndHash.
	StartHash(key []byte, length, expiry int64, encodeType ValueType, sizeZip uint64) //modified
	// Hset is called once for each field=value pair in a hash.
	Hset(key, field, value []byte)
	// EndHash is called when there are no more fields in a hash.
	EndHash(key []byte)
	// StartSet is called at the beginning of a set.
	// Sadd will be called exactly cardinality times before EndSet.
	StartSet(key []byte, cardinality, expiry int64, encodeType ValueType, sizeZip uint64) //modified
	// Sadd is called once for each member of a set.
	Sadd(key, member []byte)
	// EndSet is called when there are no more fields in a set.
	EndSet(key []byte)
	// StartList is called at the beginning of a list.
	// Rpush will be called exactly length times before EndList.
	// If length of the list is not known, then length is -1
	StartList(key []byte, length, expiry int64, encodeType ValueType, sizeZip uint64) //modified
	// Rpush is called once for each value in a list.
	Rpush(key, value []byte)
	// EndList is called when there are no more values in a list.
	EndList(key []byte)
	// StartZSet is called at the beginning of a sorted set.
	// Zadd will be called exactly cardinality times before EndZSet.
	StartZSet(key []byte, cardinality, expiry int64, encodeType ValueType, sizeZip uint64) //modified
	// Zadd is called once for each member of a sorted set.
	Zadd(key []byte, score float64, member []byte)
	// EndZSet is called when there are no more members in a sorted set.
	EndZSet(key []byte)
	// EndDatabase is called at the end of a database.
	EndDatabase(n int)
	// EndRDB is called when parsing of the RDB file is complete.
	EndRDB()
}

// Decode parses a RDB file from r and calls the decode hooks on d.
func Decode(r io.Reader, d Decoder) error {                 //decode entrance
	decoder := &decode{d, make([]byte, 8), bufio.NewReader(r), 0}
	return decoder.decode()
}

// Decode a byte slice from the Redis DUMP command. The dump does not contain the
// database, key or expiry, so they must be included in the function call (but
// can be zero values).
func DecodeDump(dump []byte, db int, key []byte, expiry int64, d Decoder) error {
	err := verifyDump(dump)
	if err != nil {
		return err
	}

	decoder := &decode{d, make([]byte, 8), bytes.NewReader(dump[1:]), 0}
	decoder.event.StartRDB()
	decoder.event.StartDatabase(db)

	err = decoder.readObject(key, ValueType(dump[0]), expiry)

	decoder.event.EndDatabase(db)
	decoder.event.EndRDB()
	return err
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

type decode struct {
	event  Decoder
	intBuf []byte
	r      byteReader
	rdbVersion int
}

type ValueType byte

const (
	TypeString  ValueType = 0
	TypeList    ValueType = 1
	TypeSet     ValueType = 2
	TypeZSet    ValueType = 3
	TypeHash    ValueType = 4
	TypeZSet2   ValueType = 5  // ZSET version 2 with doubles stored in binary.
	TypeModule  ValueType = 6
	TypeModule2 ValueType = 7

	TypeHashZipmap    ValueType = 9
	TypeListZiplist   ValueType = 10
	TypeSetIntset     ValueType = 11
	TypeZSetZiplist   ValueType = 12
	TypeHashZiplist   ValueType = 13
	TypeListQuicklist ValueType = 14
	TypeStreamListPacks ValueType = 15
)

const (
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3

	rdbFlagModuleAux= 0xf7
	rdbFlagIdle     = 0xf8
	rdbFlagFreq     = 0xf9
	rdbFlagAux      = 0xfa
	rdbFlagResizeDB = 0xfb
	rdbFlagExpiryMS = 0xfc
	rdbFlagExpiry   = 0xfd
	rdbFlagSelectDB = 0xfe
	rdbFlagEOF      = 0xff

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	// reference https://github.com/sripathikrishnan/redis-rdb-tools/blob/548b11ec3c81a603f5b321228d07a61a0b940159/rdbtools/parser.py#L61
	rdbModuleEOF = 0   // End of module value.
	rdbModuleSInt = 1
	rdbModuleUInt = 2
	rdbModuleFloat = 3
	rdbModuleDouble = 4
	rdbModuleString = 5

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15
)

func (d *decode) decode() error {
	err := d.checkHeader()
	if err != nil {
		return err
	}

	d.event.StartRDB()

	var db uint64
	var expiry int64
	firstDB := true
	for {
		objType, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		switch objType {
		case rdbFlagAux:
			auxKey, err := d.readString()
			if err != nil {
				return err
			}
			auxVal, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Aux(auxKey, auxVal)
		case rdbFlagResizeDB:
			dbSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			expiresSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			d.event.ResizeDatabase(dbSize, expiresSize)
		case rdbFlagExpiryMS:
			_, err := io.ReadFull(d.r, d.intBuf)
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint64(d.intBuf))
		case rdbFlagExpiry:
			_, err := io.ReadFull(d.r, d.intBuf[:4])
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint32(d.intBuf)) * 1000
		case rdbFlagIdle:
			_, _, err := d.readLength()
			if err != nil {
				return err
			}
		case rdbFlagFreq:
			_, err := d.r.ReadByte()
			if err != nil {
				return err
			}
		case rdbFlagSelectDB:
			if !firstDB {
				d.event.EndDatabase(int(db))
			}
			db, _, err = d.readLength()
			if err != nil {
				return err
			}
			d.event.StartDatabase(int(db))
			firstDB = false
		case rdbFlagModuleAux:
			d.skipModule()
		case rdbFlagEOF:
			d.event.EndDatabase(int(db))
			d.event.EndRDB()
			if d.rdbVersion >= 5{
				d.skipNBytes(8)
			}
			return nil
		default:
			key, err := d.readString()
			if err != nil {
				return err
			}
			err = d.readObject(key, ValueType(objType), expiry)
			if err != nil {
				return err
			}
			expiry = 0 // no expiry
		}
	}

	panic("not reached")
}

// ValueType https://github.com/redis/redis/blob/265341b577dadcbdd40343bdf32d7dcb0a4bc1d1/src/rdb.c#L625
func (d *decode) readObject(key []byte, typ ValueType, expiry int64) error {
	switch typ {
	case TypeString:
		value, err := d.readString()
		if err != nil {
			return err
		}
		d.event.Set(key, value, expiry)
	case TypeList:  // encoding linkedlist
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartList(key, int64(length), expiry, typ, 0)
		for i := uint64(0); i < length; i++ {
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Rpush(key, value)
		}
		d.event.EndList(key)
	case TypeListQuicklist:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartList(key, int64(-1), expiry, typ, 0)
		for i := uint64(0); i < length; i++ {
			d.readZiplist(key, 0, false, typ)
		}
		d.event.EndList(key)
	case TypeSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartSet(key, int64(cardinality), expiry, typ, 0)
		for i := uint64(0); i < cardinality; i++ {
			member, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Sadd(key, member)
		}
		d.event.EndSet(key)
	case TypeZSet, TypeZSet2: // encoding skiplist
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartZSet(key, int64(cardinality), expiry, typ, 0)
		for i := uint64(0); i < cardinality; i++ {
			member, err := d.readString()
			if err != nil {
				return err
			}
			var score float64
			if typ == TypeZSet2 {
				if score, err = d.readBinaryFloat64(); err != nil {
					return err
				}
			} else {
				if score, err = d.readFloat64(); err != nil {
					return err
				}
			}
			d.event.Zadd(key, score, member)
		}
		d.event.EndZSet(key)
	case TypeHash:  // encoding hashtable
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartHash(key, int64(length), expiry, typ, 0)
		for i := uint64(0); i < length; i++ {
			field, err := d.readString()
			if err != nil {
				return err
			}
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Hset(key, field, value)
		}
		d.event.EndHash(key)
	case TypeHashZipmap:
		return d.readZipmap(key, expiry, typ)
	case TypeListZiplist:
		return d.readZiplist(key, expiry, true, typ)
	case TypeSetIntset:
		return d.readIntset(key, expiry, typ)
	case TypeZSetZiplist:
		return d.readZiplistZset(key, expiry, typ)
	case TypeHashZiplist:
		return d.readZiplistHash(key, expiry, typ)
	case TypeModule2:
		return d.skipModule()
	case TypeStreamListPacks:
		return d.skipStream()
	case TypeModule:
		return fmt.Errorf("rdb: Unable to read Redis Modules RDB objects (key %s)", key)
	default:
		return fmt.Errorf("rdb: unknown object type %d for key %s", typ, key)
	}
	return nil
}

func (d *decode) readZipmap(key []byte, expiry int64, typ ValueType) error {
	var length int
	zipmap, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(zipmap)
	lenByte, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if lenByte >= 254 { // we need to count the items manually
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return err
		}
	} else {
		length = int(lenByte)
	}
	d.event.StartHash(key, int64(length), expiry, typ, uint64(len(zipmap)))
	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

func (d *decode) readZiplist(key []byte, expiry int64, addListEvents bool, typ ValueType) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	if addListEvents {
		d.event.StartList(key, length, expiry, typ, uint64(len(ziplist)))
	}
	for i := int64(0); i < length; i++ {
		entry, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Rpush(key, entry)
	}
	if addListEvents {
		d.event.EndList(key)
	}
	return nil
}

func (d *decode) readZiplistZset(key []byte, expiry int64, typ ValueType) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	cardinality, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2
	d.event.StartZSet(key, cardinality, expiry, typ, uint64(len(ziplist)))
	for i := int64(0); i < cardinality; i++ {
		member, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		d.event.Zadd(key, score, member)
	}
	d.event.EndZSet(key)
	return nil
}

func (d *decode) readZiplistHash(key []byte, expiry int64, typ ValueType) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	d.event.StartHash(key, length, expiry, typ, uint64(len(ziplist)))
	for i := int64(0); i < length; i++ {
		field, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZiplistLength(buf *sliceBuffer) (int64, error) {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func readZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (d *decode) readIntset(key []byte, expiry int64, typ ValueType) error {
	intset, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(intset)
	intSizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(intSizeBytes)

	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("rdb: unknown intset encoding: %d", intSize)
	}

	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)

	d.event.StartSet(key, int64(cardinality), expiry, typ, uint64(len(intset)))
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		d.event.Sadd(key, []byte(intString))
	}
	d.event.EndSet(key)
	return nil
}

func (d *decode) skipModule() error {
	var err error
	var lcode uint64
	_, _, err = d.readLength()
	if err != nil {
		return err
	}
	lcode, _, err = d.readLength()
	if err != nil {
		return err
	}
	for lcode != rdbModuleEOF {
		switch lcode {
		case rdbModuleSInt,rdbModuleUInt :
			_, _, err = d.readLength()
			if err != nil {
				return err
			}
		case rdbModuleFloat:
			err = d.skipBinaryFloat32()
			if err != nil {
				return err
			}
		case rdbModuleDouble:
			err = d.skipBinaryDouble()
			if err != nil {
				return err
			}
		case rdbModuleString:
			err = d.skipString()
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("rdb: Unknown module:%d", lcode)
		}
		lcode, _, err = d.readLength()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *decode) skipStream() error {
	var err error
	var l uint64
	var lc uint64
	var pending uint64
	var consumers uint64
	l, _, err = d.readLength()
	if err != nil {
		return err
	}
	for i:=uint64(0); i<l; i++ {
		if err = d.skipString();err != nil {  // err is function scope
			return err
		}
		if err = d.skipString();err != nil {
			return err
		}
	}
	for i:=0; i<3; i++ {
		if _, _, err = d.readLength(); err != nil {
			return err
		}
	}

	l, _, err = d.readLength()
	if err != nil {
		return err
	}
	for i:=uint64(0); i<l; i++ {
		if err = d.skipString();err != nil {
			return err
		}
		for j:=0; j<2; j++ {
			if _, _, err = d.readLength(); err != nil {
				return err
			}
		}
		pending, _, err = d.readLength()
		if err != nil {
			return err
		}
		for k:=uint64(0); k< pending; k++ {
			if err=d.skipNBytes(24); err != nil {
				return err
			}
			if _, _, err=d.readLength(); err != nil {
				return err
			}
		}
		consumers, _, err = d.readLength()
		if err != nil {
			return err
		}
		for k:=uint64(0); k< consumers; k++ {
			if err=d.skipString(); err != nil {
				return err
			}
			if err=d.skipNBytes(8); err != nil {
				return err
			}
			lc, _, err=d.readLength()
			if err != nil {
				return err
			}
			if err=d.skipNBytes(lc*16); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *decode) checkHeader() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(d.r, header)
	if err != nil {
		return err
	}

	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return fmt.Errorf("rdb: invalid file format")
	}

	version, _ := strconv.ParseInt(string(header[5:]), 10, 64)
	if version < 1 || version > 9 {
		return fmt.Errorf("rdb: invalid RDB version number %d", version)
	}
	d.rdbVersion = int(version)
	return nil
}

func (d *decode) readString() ([]byte, error) {
	length, encoded, err := d.readLength()
	if err != nil {
		return nil, err
	}
	if encoded {
		switch length {
		case rdbEncInt8:
			i, err := d.readUint8()
			return []byte(strconv.FormatInt(int64(int8(i)), 10)), err
		case rdbEncInt16:
			i, err := d.readUint16()
			return []byte(strconv.FormatInt(int64(int16(i)), 10)), err
		case rdbEncInt32:
			i, err := d.readUint32()
			return []byte(strconv.FormatInt(int64(int32(i)), 10)), err
		case rdbEncLZF:
			clen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			ulen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			compressed := make([]byte, clen)
			_, err = io.ReadFull(d.r, compressed)
			if err != nil {
				return nil, err
			}
			decompressed := lzfDecompress(compressed, int(ulen))
			if len(decompressed) != int(ulen) {
				return nil, fmt.Errorf("decompressed string length %d didn't match expected length %d", len(decompressed), ulen)
			}
			return decompressed, nil
		}
	}

	str := make([]byte, length)
	_, err = io.ReadFull(d.r, str)
	return str, err
}

func (d *decode) skipString() error {
	length, encoded, err := d.readLength()
	if err != nil {
		return err
	}
	var skip uint64
	if encoded {
		switch length {
		case rdbEncInt8:
			skip = 1
		case rdbEncInt16:
			skip = 2
		case rdbEncInt32:
			skip = 4
		case rdbEncLZF:
			clen, _, err := d.readLength()
			if err != nil {
				return err
			}
			_, _, err = d.readLength()
			if err != nil {
				return err
			}
			skip = clen
		}
	} else {
		skip = length
	}

	str := make([]byte, skip)
	_, err = io.ReadFull(d.r, str)
	return err
}

func (d *decode) skipNBytes(n uint64) error {
	str := make([]byte, n)
	_, err := io.ReadFull(d.r, str)
	return err
}

func (d *decode) readUint8() (uint8, error) {
	b, err := d.r.ReadByte()
	return uint8(b), err
}

func (d *decode) readUint16() (uint16, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:2])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(d.intBuf), nil
}

func (d *decode) readUint32() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(d.intBuf), nil
}

func (d *decode) readUint64() (uint64, error) {
	_, err := io.ReadFull(d.r, d.intBuf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(d.intBuf), nil
}

func (d *decode) readUint32Big() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(d.intBuf), nil
}

func (d *decode) readUint64Big() (uint64, error) {
	_, err := io.ReadFull(d.r, d.intBuf)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(d.intBuf), nil
}

// Doubles are saved as strings prefixed by an unsigned
// 8 bit integer specifying the length of the representation.
// This 8 bit integer has special values in order to specify the following
// conditions:
// 253: not a number
// 254: + inf
// 255: - inf
func (d *decode) readFloat64() (float64, error) {
	length, err := d.readUint8()
	if err != nil {
		return 0, err
	}
	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		floatBytes := make([]byte, length)
		_, err := io.ReadFull(d.r, floatBytes)
		if err != nil {
			return 0, err
		}
		f, err := strconv.ParseFloat(string(floatBytes), 64)
		return f, err
	}

	panic("not reached")
}

//add zset2
func (d *decode) readBinaryFloat64() (float64, error) {
	if _, err := io.ReadFull(d.r, d.intBuf); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(d.intBuf)
	return math.Float64frombits(bits), nil
}

func (d *decode) skipBinaryFloat32() error {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return err
	}
	return nil
}

func (d *decode) skipBinaryDouble() error {
	_, err := io.ReadFull(d.r, d.intBuf[:8])
	if err != nil {
		return err
	}
	return nil
}

func (d *decode) readLength() (uint64, bool, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, false, err
	}
	// The first two bits of the first byte are used to indicate the length encoding type
	switch (b & 0xc0) >> 6 {
	case rdb6bitLen:
		// When the first two bits are 00, the next 6 bits are the length.
		return uint64(b & 0x3f), false, nil
	case rdb14bitLen:
		// When the first two bits are 01, the next 14 bits are the length.
		bb, err := d.r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return (uint64(b&0x3f) << 8) | uint64(bb), false, nil
	case rdbEncVal:
		// When the first two bits are 11, the next object is encoded.
		// The next 6 bits indicate the encoding type.
		return uint64(b & 0x3f), true, nil
	default:
		// When the first two bits are 10
		if b == rdb32bitLen {
			// The next 4 bytes are the length.
			length, err := d.readUint32Big()
			return uint64(length), false, err

		} else if b == rdb64bitLen {
			// The next 8 bytes are the length.
			length, err := d.readUint64Big()
			return length, false, err
		}
	}

	panic("Unknown length encoding in rdbLoadLen()")
}

func verifyDump(d []byte) error {
	if len(d) < 10 {
		return fmt.Errorf("rdb: invalid dump length")
	}
	version := binary.LittleEndian.Uint16(d[len(d)-10:])
	if version != uint16(Version) {
		return fmt.Errorf("rdb: invalid version %d, expecting %d", version, Version)
	}

	if binary.LittleEndian.Uint64(d[len(d)-8:]) != crc64.Digest(d[:len(d)-8]) {
		return fmt.Errorf("rdb: invalid CRC checksum")
	}

	return nil
}

func lzfDecompress(in []byte, outlen int) []byte {
	out := make([]byte, outlen)
	for i, o := 0, 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	return out
}
