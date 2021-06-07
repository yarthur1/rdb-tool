package rdb

import (
    "math/rand"
    "strconv"
    "time"
)

//redis server machine    architecture 64bit os
const POINT_SIZE = 8
const LONG_SIZE = 8
const DOUBLE_SIZE = 8
const LIST_MAX_ZIPLIST_SIZE = 8192
const RAND_MAX = 0xFFFF
const ZSKIPLIST_MAXLEVEL = 32

//size classes from jemalloc 4.0.4 using LG_QUANTUM=3
var jemallocSizeClasses = []uint64{
    8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024,
    1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576,
    28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, 163840, 196608, 229376, 262144, 327680,
    393216, 458752, 524288, 655360, 786432, 917504, 1048576, 1310720, 1572864, 1835008, 2097152, 2621440, 3145728,
    3670016, 4194304, 5242880, 6291456, 7340032, 8388608, 10485760, 12582912, 14680064, 16777216, 20971520, 25165824,
    29360128, 33554432, 41943040, 50331648, 58720256, 67108864, 83886080, 100663296, 117440512, 134217728, 167772160,
    201326592, 234881024, 268435456, 335544320, 402653184, 469762048, 536870912, 671088640, 805306368, 939524096,
    1073741824, 1342177280, 1610612736, 1879048192, 2147483648, 2684354560, 3221225472, 3758096384, 4294967296,
    5368709120, 6442450944, 7516192768, 8589934592, 10737418240, 12884901888, 15032385536, 17179869184, 21474836480,
    25769803776, 30064771072, 34359738368, 42949672960, 51539607552, 60129542144, 68719476736, 85899345920,
    103079215104, 120259084288, 137438953472, 171798691840, 206158430208, 240518168576, 274877906944, 343597383680,
    412316860416, 481036337152, 549755813888, 687194767360, 824633720832, 962072674304, 1099511627776, 1374389534720,
    1649267441664, 1924145348608, 2199023255552, 2748779069440, 3298534883328, 3848290697216, 4398046511104,
    5497558138880, 6597069766656, 7696581394432, 8796093022208, 10995116277760, 13194139533312, 15393162788864,
    17592186044416, 21990232555520, 26388279066624, 30786325577728, 35184372088832, 43980465111040, 52776558133248,
    61572651155456, 70368744177664, 87960930222080, 105553116266496, 123145302310912, 140737488355328, 175921860444160,
    211106232532992, 246290604621824, 281474976710656, 351843720888320, 422212465065984, 492581209243648,
    562949953421312, 703687441776640, 844424930131968, 985162418487296, 1125899906842624, 1407374883553280,
    1688849860263936, 1970324836974592, 2251799813685248, 2814749767106560, 3377699720527872, 3940649673949184,
    4503599627370496, 5629499534213120, 6755399441055744, 7881299347898368, 9007199254740992, 11258999068426240,
    13510798882111488, 15762598695796736, 18014398509481984, 22517998136852480, 27021597764222976, 31525197391593472,
    36028797018963968, 45035996273704960, 54043195528445952, 63050394783186944, 72057594037927936, 90071992547409920,
    108086391056891904, 126100789566373888, 144115188075855872, 180143985094819840, 216172782113783808,
    252201579132747776, 288230376151711744, 360287970189639680, 432345564227567616, 504403158265495552,
    576460752303423488, 720575940379279360, 864691128455135232, 1008806316530991104, 1152921504606846976,
    1441151880758558720, 1729382256910270464, 2017612633061982208, 2305843009213693952, 2882303761517117440,
    3458764513820540928, 4035225266123964416, 4611686018427387904, 5764607523034234880, 6917529027641081856,
    8070450532247928832, 9223372036854775808, 11529215046068469760, 13835058055282163712, 16140901064495857664,
}

func index(arr []uint64, x uint64) int {
    //Return the index where to insert item x in list a, assuming a is sorted.
    //
    //The return value i is such that all e in a[:i] have e < x, and all e in
    //a[i:] have e >= x.  So if x already appears in the list, a.insert(x) will
    //insert just before the leftmost x already there.
    //
    //Optional args lo (default 0) and hi (default len(a)) bound the
    //slice of a to be searched.
    high := len(arr)
    low := 0
    for low < high {
        mid := (low + high) >> 1
        if arr[mid] < x {
            low = mid + 1
        } else {
            high = mid
        }
    }
    return low
}

func mallocOverHead(size uint64) uint64 {
    idx := index(jemallocSizeClasses, size)
    if idx < len(jemallocSizeClasses) {
        return jemallocSizeClasses[idx]
    } else {
        return size
    }
}

func isIntegerType(val []byte) (bool, uint64) { //is integer ,return val,or return strlen
    l := len(val)
    if l == 0 {
        return false, 0 //index out of range [0] with length 0   called by sizeOfString
    }
    if val[0] == '0' && l > 1 {
        return false, uint64(l)
    }
    if val[0] == '-' && val[1] == '0' && l > 1 {
        return false, uint64(l)
    }
    num, err := strconv.ParseInt(string(val), 10, 64) //REDIS_ENCODING_INT long 64bit architecture
    if err == nil {
        return true, uint64(num)
    }
    return false, uint64(l)
}

func stringElementNum(str []byte) uint64 {
    l := len(str)
    if l == 0 {
        return 0
    }
    if str[0] == '0' && l > 1 {
        return uint64(l)
    }
    _, err := strconv.ParseInt(string(str), 10, 64) //REDIS_ENCODING_INT long
    if err == nil {
        return LONG_SIZE
    }
    return uint64(l)
}

//if is raw or embstr sds ,the value 0f sizeOfString include sdshdr
func sizeOfString(str []byte) uint64 {
    //https://github.com/antirez/redis/blob/unstable/src/sds.h
    //the integer <10000 shared,otherwise the integer is part of the robj, no extra memory
    flag, l := isIntegerType(str)
    if flag {
        return 0
    }
    //sdshdr5 is never used
    if l < 1<<8 {
        return mallocOverHead(l + 1 + 2 + 1)
    }
    if l < 1<<16 {
        return mallocOverHead(l + 1 + 4 + 1)
    }
    if l < 1<<32 {
        return mallocOverHead(l + 1 + 8 + 1)
    }
    return mallocOverHead(l + 1 + 16 + 1)
}

func hashTableEntryOverHead() uint64 {
    //See  https://github.com/antirez/redis/blob/unstable/src/dict.h
    //Each dictEntry has 2 pointers + int64
    return 2*POINT_SIZE + 8
}

func nextPower(length uint64) uint64 {
    var power uint64 = 1
    for power <= length {
        power = power << 1
    }
    return power
}

func hashTableOverHead(eleNums uint64, version int) uint64 {
    //# See  https://github.com/antirez/redis/blob/unstable/src/dict.h
    //# See the structures dict and dictht
    //  v3.2
    //# 2 * (3 unsigned longs + 1 pointer) + int + long + 2 pointers
    //# v4 v5
    //  2 * (3 unsigned longs + 1 pointer) + long + long + 2 pointers
    //# Additionally, see **table in dictht
    //# The length of the table is the next power of 2
    //# When the hashtable is rehashing, another instance of **table is created
    //# Due to the possibility of rehashing during loading, we calculate the worse
    //# case in which both tables are allocated, and so multiply
    //# the size of **table by 1.5
    if version > 3 {
        return 8*LONG_SIZE + 4*POINT_SIZE + (nextPower(eleNums)*POINT_SIZE*3)>>1
    }
    //fix Memory alignment
    //4 + 7*LONG_SIZE + 4*POINT_SIZE + (nextPower(eleNums)*POINT_SIZE*3)>>1
    return 8*LONG_SIZE + 4*POINT_SIZE + (nextPower(eleNums)*POINT_SIZE*3)>>1
}

func zipListHeaderOverHead() uint64 {
    //# See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
    //# <zlbytes><zltail><zllen><entry><entry><zlend>
    return 4 + 4 + 2 + 1
}

func ziplistEntryOverHead(val []byte) uint64 {
    //See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
    flag, value := isIntegerType(val)
    var size uint64 = 0
    var header uint64 = 0
    if flag {
        header = 1
        switch {
        case value < 12:
            size = 0
        case value < 1<<8:
            size = 1
        case value < 1<<16:
            size = 2
        case value < 1<<24:
            size = 3
        case value < 1<<32:
            size = 4
        default:
            size = 8
        }
    } else {
        size = value
        switch {
        case size <= 63:
            header = 1
        case size <= 16383:
            header = 2
        default:
            header = 5
        }
    }
    if size < 254 {
        return 1 + header + size
    }
    return 5 + header + size
}

func linkedListOverHead() uint64 {
    //# See https://github.com/antirez/redis/blob/unstable/src/adlist.h
    //# A list has 5 pointers + an unsigned long
    return 5*POINT_SIZE + LONG_SIZE
}

func linkedListEntryOverHead() uint64 {
    //# See https://github.com/antirez/redis/blob/unstable/src/adlist.h
    //# A node has 3 pointers
    return 3 * POINT_SIZE
}

func quickListOverHead(zipCnt uint64) uint64 {
    quickList := uint64(2*POINT_SIZE + LONG_SIZE + 2*4)
    quickItem := uint64(4*POINT_SIZE + LONG_SIZE + 2*4)
    return quickList + zipCnt*quickItem
}

func randomLevel() uint64 {
    level := 1
    r := rand.New(rand.NewSource(time.Now().Unix()))
    rInt := r.Intn(1 << 16)
    for rInt < (RAND_MAX >> 2) {
        level += 1
        rInt = r.Intn(1 << 16)
    }
    if level < ZSKIPLIST_MAXLEVEL {
        return uint64(level)
    }
    return ZSKIPLIST_MAXLEVEL
}

func skipListOverHead(eleNums uint64, version int) uint64 {
    return 4*POINT_SIZE + hashTableOverHead(eleNums, version) + 16
}

func skipListEntryOverHead() uint64 {    //randomLevel()*(POINT_SIZE+8)  memory alignment
    return 2*POINT_SIZE + 8 + randomLevel()*(POINT_SIZE+8) + hashTableEntryOverHead()
}

func keyExpiryOverHead(expiry int64) uint64 {
    if expiry == 0 {
        return 0
    }
    //Key expiry is stored in a hashtable, so we have to pay for the cost of a hashtable entry
    //The timestamp itself is stored as an int64, which is a 8 bytes  or one item of dictentry* array
    return hashTableEntryOverHead() + 8
}

func robjOverHead() uint64 {
    //See  https://github.com/antirez/redis/blob/unstable/src/server.h
    // 1 pointers + int + 3*unsigned
    return POINT_SIZE + 8
}

func topLevelObjOverhead(key []byte, expiry int64) uint64 {
    return hashTableEntryOverHead() + robjOverHead() + keyExpiryOverHead(expiry) + sizeOfString(key)
}
