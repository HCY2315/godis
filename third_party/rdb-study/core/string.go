package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/lzf"
	"math"
	"strconv"
	"unicode"
)

const (
	len6Bit      = 0
	len14Bit     = 1
	len32or64Bit = 2
	lenSpecial   = 3
	len32Bit     = 0x80
	len64Bit     = 0x81

	encodeInt8  = 0
	encodeInt16 = 1
	encodeInt32 = 2
	encodeLZF   = 3

	maxUint6  = 1<<6 - 1
	maxUint14 = 1<<14 - 1
	minInt24  = -1 << 23
	maxInt24  = 1<<23 - 1

	len14BitMask      byte = 0b01000000
	encodeInt8Prefix       = lenSpecial<<6 | encodeInt8
	encodeInt16Prefix      = lenSpecial<<6 | encodeInt16
	encodeInt32Prefix      = lenSpecial<<6 | encodeInt32
	encodeLZFPrefix        = lenSpecial<<6 | encodeLZF
)

// readLength 解析 RDB 的「长度编码」(Length Encoding)。
// RDB 用变长编码存各种长度（字符串长度、list/hash/zset 元素个数等）以节省空间。
// 第一个字节的高 2 位决定编码方式，低 6 位含义随方式而变：
//
//	firstByte: [ bit7 bit6 | bit5 bit4 bit3 bit2 bit1 bit0 ]
//	            └─ 高2位 ─┘   └──────── 低6位 ────────┘
//
//	 00 (len6Bit)     : 低6位即长度，取值范围 0~63
//	 01 (len14Bit)    : 长度 = 低6位<<8 | 下一字节（共 14 位，最长 16383）
//	 10 (len32or64Bit): 再看 firstByte —— ==0x80 读 4 字节(大端)，==0x81 读 8 字节(大端)
//	 11 (lenSpecial)  : 低6位是「特殊编码类型」而非长度，此时 special 标记为 true
//
// 返回值：
//
//	length  : 真实长度；当 special==true 时它其实是编码类型(int8/int16/int32/LZF)
//	special : 是否为特殊编码；调用方(如 readString)据此决定后续如何解码 value
//
// 参考: https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#length-encoding
func (dec *Decoder) readLength() (uint64, bool, error) {
	// 读第 1 个字节，它决定采用哪种长度编码方式
	firstByte, err := dec.readByte()
	if err != nil {
		return 0, false, fmt.Errorf("read length failed: %v", err)
	}

	// 取高 2 位：0xc0=1100_0000 保留高2位，>>6 把它移到最低位 → 得到 0/1/2/3
	lenType := (firstByte & 0xc0) >> 6 // get first 2 bits

	var length uint64 // 解析出的长度；special 时它代表编码类型
	special := false  // 是否命中「特殊编码」(11 分支)

	switch lenType {
	case len6Bit: // 00：长度就在低 6 位里（0~63）
		length = uint64(firstByte) & 0x3f // 0x3f=0011_1111 只保留低 6 位

	case len14Bit: // 01：14 位长度 = 低6位左移8位 拼上下一字节（大端）
		nextByte, err := dec.readByte()
		if err != nil {
			return 0, false, fmt.Errorf("read len14Bit failed: %v", err)
		}
		length = (uint64(firstByte)&0x3f)<<8 | uint64(nextByte)

	case len32or64Bit: // 10：再用 firstByte 区分 32 位还是 64 位
		if firstByte == len32Bit { // 0x80 → 32 位长度
			err = dec.readFull(dec.buffer[0:4]) // 读 4 字节到复用的暂存区(dec.buffer 长 8)
			if err != nil {
				return 0, false, fmt.Errorf("read len32Bit failed: %v", err)
			}
			length = uint64(binary.BigEndian.Uint32(dec.buffer)) // 大端解析
		} else if firstByte == len64Bit { // 0x81 → 64 位长度
			err = dec.readFull(dec.buffer) // buffer 正好 8 字节
			if err != nil {
				return 0, false, fmt.Errorf("read len64Bit failed: %v", err)
			}
			length = binary.BigEndian.Uint64(dec.buffer)
		} else {
			// 高2位=10 但低6位既不是 0(0x80) 也不是 1(0x81)，编码非法
			return 0, false, fmt.Errorf("illegal length encoding: %x", firstByte)
		}

	case lenSpecial: // 11：特殊编码（非长度），低6位是编码类型
		special = true
		length = uint64(firstByte) & 0x3f // 0=int8, 1=int16, 2=int32, 3=LZF
	}

	return length, special, nil
}

func (dec *Decoder) readString() ([]byte, error) {
	length, special, err := dec.readLength()
	if err != nil {
		return nil, err
	}

	if special {
		switch length {
		case encodeInt8:
			b, err := dec.readByte()
			return []byte(strconv.Itoa(int(int8(b)))), err
		case encodeInt16:
			b, err := dec.readInt16()
			return []byte(strconv.Itoa(int(b))), err
		case encodeInt32:
			b, err := dec.readInt32()
			return []byte(strconv.Itoa(int(b))), err
		case encodeLZF:
			return dec.readLZF()
		default:
			return []byte{}, errors.New("Unknown string encode type ")
		}
	}

	res := make([]byte, length)
	err = dec.readFull(res)
	return res, err
}

func (dec *Decoder) readInt16() (int16, error) {
	err := dec.readFull(dec.buffer[:2])
	if err != nil {
		return 0, fmt.Errorf("read uint16 error: %v", err)
	}

	i := binary.LittleEndian.Uint16(dec.buffer[:2])
	return int16(i), nil
}

func (dec *Decoder) readInt32() (int32, error) {
	err := dec.readFull(dec.buffer[:4])
	if err != nil {
		return 0, fmt.Errorf("read uint16 error: %v", err)
	}

	i := binary.LittleEndian.Uint32(dec.buffer[:4])
	return int32(i), nil
}

func (dec *Decoder) readLiteralFloat() (float64, error) {
	first, err := dec.readByte()
	if err != nil {
		return 0, err
	}
	if first == 0xff {
		return math.Inf(-1), nil
	} else if first == 0xfe {
		return math.Inf(1), nil
	} else if first == 0xfd {
		return math.NaN(), nil
	}
	buf := make([]byte, first)
	err = dec.readFull(buf)
	if err != nil {
		return 0, err
	}
	str := unsafeBytes2Str(buf)
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, fmt.Errorf("")
	}
	return val, err
}

func (dec *Decoder) readFloat() (float64, error) {
	err := dec.readFull(dec.buffer)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(dec.buffer)
	return math.Float64frombits(bits), nil
}
func (dec *Decoder) readFloat32() (f float32, err error) {
	err = dec.readFull(dec.buffer[:4])
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint32(dec.buffer[:4])
	return math.Float32frombits(bits), nil
}

func (dec *Decoder) readLZF() ([]byte, error) {
	inLen, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	outLen, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	val := make([]byte, inLen)
	err = dec.readFull(val)
	if err != nil {
		return nil, err
	}
	return lzf.Decompress(val, int(inLen), int(outLen))
}

func (enc *Encoder) writeLength(value uint64) error {
	var buf []byte
	if value <= maxUint6 {
		// 00 + 6 bits of data
		enc.buffer[0] = byte(value)
		buf = enc.buffer[0:1]
	} else if value <= maxUint14 {
		enc.buffer[0] = byte(value>>8) | len14BitMask // high 6 bit and mask(0x40)
		enc.buffer[1] = byte(value)                   // low 8 bit
		buf = enc.buffer[0:2]
	} else if value <= math.MaxUint32 {
		buf = make([]byte, 5)
		buf[0] = len32Bit
		binary.BigEndian.PutUint32(buf[1:], uint32(value))
	} else {
		buf = make([]byte, 9)
		buf[0] = len64Bit
		binary.BigEndian.PutUint64(buf[1:], value)
	}
	return enc.write(buf)
}

func (enc *Encoder) writeSimpleString(s string) error {
	err := enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.write([]byte(s))
}

func (enc *Encoder) tryWriteIntString(s string) (bool, error) {
	intVal, ok := isEncodableUint32(s)
	if !ok {
		return false, nil
	}
	var err error
	if intVal >= math.MinInt8 && intVal <= math.MaxInt8 {
		err = enc.write([]byte{encodeInt8Prefix, byte(int8(intVal))})
	} else if intVal >= math.MinInt16 && intVal <= math.MaxInt16 {
		buf := enc.buffer[0:3]
		buf[0] = encodeInt16Prefix
		binary.LittleEndian.PutUint16(buf[1:], uint16(int16(intVal)))
		err = enc.write(buf)
	} else if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
		buf := enc.buffer[0:5]
		buf[0] = encodeInt32Prefix
		binary.LittleEndian.PutUint32(buf[1:], uint32(int32(intVal)))
		err = enc.write(buf)
	} else {
		// beyond int32 range, but within int64 range
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, nil
}

func (enc *Encoder) writeLZFString(s string) error {
	out, err := lzf.Compress([]byte(s))
	if err != nil {
		return err
	}
	err = enc.write([]byte{encodeLZFPrefix})
	if err != nil {
		return err
	}
	// write compressed length
	err = enc.writeLength(uint64(len(out)))
	if err != nil {
		return err
	}
	// write uncompressed length
	err = enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.write(out)
}

func (enc *Encoder) writeString(s string) error {
	isInt, err := enc.tryWriteIntString(s)
	if err != nil {
		return err
	}
	if isInt {
		return nil
	}
	// Try LZF compression - under 20 bytes it's unable to compress even so skip it
	// see rdbSaveRawString at [rdb.c](https://github.com/redis/redis/blob/unstable/src/rdb.c#L449)
	if enc.compress && len(s) > 20 {
		err = enc.writeLZFString(s)
		if err == nil { // lzf may failed, while out > in
			return nil
		}
	}
	return enc.writeSimpleString(s)
}

// write string without try int string. for tryWriteIntSetEncoding, writeZipList
func (enc *Encoder) writeNanString(s string) error {
	if enc.compress && len(s) > 20 {
		err := enc.writeLZFString(s)
		if err == nil { // lzf may failed, while out > in
			return nil
		}
	}
	return enc.writeSimpleString(s)
}

func (enc *Encoder) WriteStringObject(key string, value []byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	err = enc.write([]byte{typeString})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeString(unsafeBytes2Str(value))
	if err != nil {
		return err
	}
	enc.state = writtenObjectState
	return nil
}

func (enc *Encoder) writeFloat64(f float64) error {
	bin := math.Float64bits(f)
	binary.LittleEndian.PutUint64(enc.buffer, bin)
	return enc.write(enc.buffer)
}

// a string might be encoded as an integer, but only subset (aka uint32) of the number set
// can be encoded and then decoded back.
// e.g. the following strings can not be encoded as an integer:
// "007", "-0", "-1", "+0", "+1", "0x11"
func isEncodableUint32(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	if s[0] == '0' && len(s) > 1 {
		return 0, false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return 0, false
		}
	}
	intVal, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return intVal, true
}
