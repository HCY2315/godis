// listpack.go 负责解析 Redis 的 listpack（紧凑列表）编码。
//
// 背景与使用场景：
//   - listpack 从 Redis 5.0 引入，最初用于 Stream 的底层存储；Redis 7.0 起逐渐
//     取代 ziplist，成为 hash/zset/set 小对象的默认紧凑编码，也是 quicklist v2
//     每个节点的载荷。RDB 中它同样以 RDB string 的形式出现（可能经 LZF 压缩）。
//
// 整体布局（多字节整数均为小端）：
//
//	+----------------+-----------------+---------+-----+---------+--------+
//	| total-bytes(4) | num-elements(2) | entry0  | ... | entryN  | 0xFF   |
//	+----------------+-----------------+---------+-----+---------+--------+
//
//	total-bytes : 整个 listpack 的字节数（含头尾）
//	num-elements: entry 个数；与 ziplist 的 uint16 一样，达到 65535 时会饱和，
//	              真实数量需遍历才能确定（本解析器直接使用该字段）
//	0xFF        : 结束标记
//
// 单个 entry 布局：
//
//	+----------+------------------+----------+
//	| encoding | data ...         | backlen  |
//	+----------+------------------+----------+
//
//	encoding: 变长头部，既区分字符串/整数，也编码长度或整数位宽
//	data    : 字符串内容或定长小端整数
//	backlen : 前面「encoding + data」的总长度，使用 7 位一组的变长编码，最多 5 字节。
//	          它被放在 entry 末尾，且从右往左可解，因此可以从后一个 entry 的尾部
//	          向前跳回当前 entry 的起点。
//
// 与 ziplist 的核心区别：
//   - ziplist 在 entry 头部放 prevlen，删除/修改元素可能引发后续 prevlen 连锁更新
//     （cascade update）；listpack 把长度信息放到 entry 尾部的 backlen，只描述
//     「本 entry 有多长」，不会因别的 entry 变化而连锁改写。
//   - listpack 的 backlen 只用于反向遍历，正向解析时读完 entry 后直接跳过即可，
//     本文件正是这样做的（只按长度跳过，不校验其内容）。
//   - 两者都支持字符串与整数混排，整数都以定长小端补码存储。
//
// 使用技巧：
//   - readListPackEntry 会返回 entry 的「编码+内容+backlen」总长度，调用方可用它
//     估算内存占用（参见 list.go 的 quicklist detail）。
//   - Stream 对象内部也使用 listpack，但它的 entry 语义由 stream.go 额外约定，
//     本文件只负责最基础的「扁平元素」读取。
package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

// readListPack 读取一个完整的 listpack，返回所有 entry 的内容及其字节长度。
//
// 返回值：
//   - entries   : 按物理顺序排列的元素；整数 entry 会被格式化成十进制字符串
//   - entrySizes: 与 entries 一一对应，表示该 entry 占用的总字节数
//     （encoding + data + backlen），用于内存统计
//   - err       : 任一环节失败立即返回
//
// readListPack returns: list of entry, list of entry size, error
func (dec *Decoder) readListPack() ([][]byte, []uint32, error) {
	// 从文件流读出一个 RDB string，即整块 listpack 数据
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// 读 6 字节头（4 字节总长 + 2 字节元素数），返回 num-elements
	size := readListPackLength(buf, &cursor)
	// 已知数量，预分配两个切片，避免循环 append 时反复扩容
	entries := make([][]byte, 0, size)
	entrySizes := make([]uint32, 0, size)
	for i := 0; i < size; i++ {
		// 一次解析出一个 entry；str 与 intval 只有一个有效
		str, intval, length, err := dec.readListPackEntry(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		if str == nil {
			// 整数编码：统一转成十进制字符串，屏蔽底层编码差异
			str = []byte(strconv.FormatInt(intval, 10))
		}
		entries = append(entries, str)
		entrySizes = append(entrySizes, length)
	}
	return entries, entrySizes, nil
}

// readListPackLength 读取 listpack 头部并返回元素个数，同时把 cursor 推进到首个 entry。
//
// 头部字段（小端）：
//
//	[0:4] total-bytes  —— 整个 listpack 的字节数（本函数不使用，仅跳过）
//	[4:6] num-elements —— entry 个数
//
// 注意：这里只跳过 6 字节定长头，没有校验结尾的 0xFF 标记。
func readListPackLength(buf []byte, cursor *int) int {
	// start 指向 num-elements（cursor 当前为 0，故为 4）
	start := *cursor + 4
	// end = start + 2，num-elements 占 2 字节
	end := start + 2
	// list pack buf: [0, 4] -> total bytes, [4:6] -> entry count
	size := int(binary.LittleEndian.Uint16(buf[start:end]))
	// cursor 前进 6 字节，正好越过整个头部
	*cursor += 6
	return size
}

// getBackLen 返回编码「elementLen」这个长度值所需的 backlen 字节数（1~5）。
//
// elementLen 指的是 entry 中 encoding + data 的字节数（不含 backlen 自身）。
// backlen 每字节携带 7 位有效数据，因此：
//
//	<= 127        → 1 字节（7 位）
//	<  2^14 - 1   → 2 字节（14 位）
//	<  2^21 - 1   → 3 字节（21 位）
//	<  2^28 - 1   → 4 字节（28 位）
//	其它          → 5 字节（35 位，足够覆盖 32 位长度）
//
// 本文件只关心「需要跳过多少字节」，并不真正解码 backlen 的值。
func getBackLen(elementLen uint32) uint32 {
	if elementLen <= 127 {
		return 1
	} else if elementLen < (1<<14)-1 {
		return 2
	} else if elementLen < (1<<21)-1 {
		return 3
	} else if elementLen < (1<<28)-1 {
		return 4
	} else {
		return 5
	}
}

// readListPackEntry 解析一个 listpack entry。
//
// 返回值：
//   - []byte: 字符串内容；整数 entry 返回 nil
//   - int64 : 整数值；字符串 entry 返回 0
//   - uint32: entry 总长度（encoding + data + backlen）
//   - error : 解析失败
//
// 编码分派规则（按 header 的高位）：
//
//	0xxxxxxx              → 7 位无符号整数（0~127）
//	10xxxxxx              → 长度 <= 63 的字符串
//	110xxxxx yyyyyyyy     → 13 位有符号整数
//	1110xxxx yyyyyyyy     → 长度 < 2^12 的字符串
//	11110000 + 4 字节     → 长度 < 2^32 的字符串（4 字节小端长度）
//	11110001 + 2 字节     → int16
//	11110010 + 3 字节     → int24
//	11110011 + 4 字节     → int32
//	11110100 + 8 字节     → int64
//	11111111              → listpack 结束标记，正常解析时不应作为 entry 出现
//
// readListPackEntry returns: string content, int content, entry length(encoding+content+backlen), error
func (dec *Decoder) readListPackEntry(buf []byte, cursor *int) ([]byte, int64, uint32, error) {
	header, err := readByte(buf, cursor)
	if err != nil {
		return nil, 0, 0, err
	}
	switch header >> 6 {
	case 0, 1: // 0xxxxxxx, uint7
		// 最高位为 0（header>>6 为 0 或 1，合起来覆盖 0xxxxxxx 的全部取值）。
		// 数值直接内嵌在头部，没有额外 data；int8 转换在这里是安全的，
		// 因为 header <= 127，不会产生负数。
		result := int64(int8(header))
		// contentLen 只算 encoding+data，本分支 data 为空，故为 1
		var contentLen uint32 = 1
		backlen := getBackLen(contentLen)
		// backlen 只用于反向遍历，正向解析直接跳过
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 2: // 10xxxxxx + content, string(len<=63)
		// 低 6 位是字符串长度，随后是真实内容
		strLen := int(header & 0x3f)
		result, err := readBytes(buf, cursor, strLen)
		if err != nil {
			return nil, 0, 0, err
		}
		// contentLen = 1 字节头部 + strLen 字节内容
		var contentLen = uint32(1 + strLen)
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return result, 0, contentLen + backlen, nil
	}
	// assert header == 11xxxxxx
	switch header >> 4 {
	case 12, 13: // 110xxxxx yyyyyyyy, int13
		// header>>4 为 12(1100xxxx) 或 13(1101xxxx)，合起来即 110xxxxx，
		// 共 13 位补码：先拼成 13 位无符号值，再判断符号位（bit12）。
		// see https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/fba43c524524cbdb54955a28af228b513420d78d/src/listpack.c#L586
		next, err := readByte(buf, cursor)
		if err != nil {
			return nil, 0, 0, err
		}
		val := ((uint(header) & 0x1F) << 8) | uint(next)
		if val >= uint(1<<12) {
			// 符号位为 1：把 13 位补码转成负值。
			// 不能直接 val-8191（val 是 uint，会下溢），所以写成 -(8191-val)-1
			val = -(8191 - val) - 1 // val is uint, must use -(8191 - val), val - 8191 will cause overflow
		}
		result := int64(val)
		// contentLen = 2 字节整数编码
		var contentLen uint32 = 2
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 14: // 1110xxxx yyyyyyyy + content, string(len < 1<<12)
		// 12 位字符串长度：高 4 位在 header 低 4 位，低 8 位在下一字节，
		// 借助 dec.buffer 前 2 字节按大端拼合
		dec.buffer[0] = header & 0x0f
		dec.buffer[1], err = readByte(buf, cursor)
		if err != nil {
			return nil, 0, 0, err
		}
		strLen := binary.BigEndian.Uint16(dec.buffer[:2])
		result, err := readBytes(buf, cursor, int(strLen))
		if err != nil {
			return nil, 0, 0, err
		}
		// contentLen = 2 字节编码头 + 内容
		var contentLen = uint32(2 + strLen)
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return result, 0, contentLen + backlen, nil
	}
	// assert header == 1111xxxx
	switch header & 0x0f {
	case 0: // 11110000 aaaaaaaa bbbbbbbb cccccccc dddddddd + content, string(len < 1<<32)
		// 4 字节小端长度，随后是内容
		var lenBytes []byte
		lenBytes, err = readBytes(buf, cursor, 4)
		if err != nil {
			return nil, 0, 0, err
		}
		strLen := int(binary.LittleEndian.Uint32(lenBytes))
		result, err := readBytes(buf, cursor, strLen)
		if err != nil {
			return nil, 0, 0, err
		}
		// contentLen = 1 字节标记 + 4 字节长度 + 内容
		var contentLen = uint32(1 + 4 + strLen)
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return result, 0, contentLen + backlen, nil
	case 1: // 11110001 aaaaaaaa bbbbbbbb, int16
		var bs []byte
		bs, err = readBytes(buf, cursor, 2)
		if err != nil {
			return nil, 0, 0, err
		}
		result := int64(int16(binary.LittleEndian.Uint16(bs)))
		// contentLen = 1 字节标记 + 2 字节整数
		var contentLen uint32 = 3
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 2: // 11110010 aaaaaaaa bbbbbbbb cccccccc, int24
		var bs []byte
		bs, err = readBytes(buf, cursor, 3)
		if err != nil {
			return nil, 0, 0, err
		}
		// 补 0 扩展成 4 字节，再算术右移 8 位完成符号扩展
		bs = append([]byte{0}, bs...)
		result := int64(int32(binary.LittleEndian.Uint32(bs)) >> 8)
		// contentLen = 1 字节标记 + 3 字节整数
		var contentLen uint32 = 4
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 3: // 1111 0011 -> int32
		var bs []byte
		bs, err = readBytes(buf, cursor, 4)
		if err != nil {
			return nil, 0, 0, err
		}
		result := int64(int32(binary.LittleEndian.Uint32(bs)))
		// contentLen = 1 字节标记 + 4 字节整数
		var contentLen uint32 = 5
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 4: // 11110100 8Byte -> int64
		var bs []byte
		bs, err = readBytes(buf, cursor, 8)
		if err != nil {
			return nil, 0, 0, err
		}
		result := int64(binary.LittleEndian.Uint64(bs))
		// contentLen = 1 字节标记 + 8 字节整数
		var contentLen uint32 = 9
		backlen := getBackLen(contentLen)
		*cursor += int(backlen)
		return nil, result, contentLen + backlen, nil
	case 15: // 11111111 -> end
		// 0xFF 是 listpack 的结束标记，不能当作普通 entry 解析
		return nil, 0, 0, errors.New("unexpected end")
	}
	return nil, 0, 0, fmt.Errorf("unknown entry header")
}

// readListPackEntryAsString return a string representation of entry
// It means if the entry is a integer, then format it as string
//
// 用途：调用方明确希望拿到「文本」时使用（如 hash 的 field/value、stream 的字段名）。
// 整数 entry 会被转成十进制字符串，字符串 entry 原样返回。
func (dec *Decoder) readListPackEntryAsString(buf []byte, cursor *int) ([]byte, error) {
	str, intval, _, err := dec.readListPackEntry(buf, cursor)
	if err != nil {
		return nil, fmt.Errorf("read from failed: %v", err)
	}
	// str != nil 说明底层是字符串编码，可直接返回
	if str != nil {
		return str, nil
	}
	// 否则是整数编码，格式化成十进制文本
	str = []byte(strconv.FormatInt(intval, 10))
	return str, nil
}

// readListPackEntryAsInt 读取一个 entry 并要求它必须是整数编码。
//
// 用途：调用方明确需要数值时使用（如 stream 的 count、flags、id 差值）。
// 如果底层存的是字符串（即使内容看起来像数字），这里也会返回错误，
// 因为 listpack 的编码类型本身携带了「整数/字符串」语义。
func (dec *Decoder) readListPackEntryAsInt(buf []byte, cursor *int) (int64, error) {
	str, intval, _, err := dec.readListPackEntry(buf, cursor)
	if err != nil {
		return 0, fmt.Errorf("read from failed: %v", err)
	}
	if str != nil {
		return 0, fmt.Errorf("%s is not a integer", string(str))
	}
	return intval, nil
}
