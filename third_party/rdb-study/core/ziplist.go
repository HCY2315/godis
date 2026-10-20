// ziplist.go 负责解析/生成 Redis 的 ziplist（压缩列表）紧凑编码。
//
// 背景与使用场景：
//   - ziplist 是 Redis 2.6 ~ 6.2 时代用于「小对象」的紧凑编码：list 元素较少时
//     整个 list 存成一个 ziplist；hash/zset 元素较少时也用 ziplist。quicklist
//     （Redis 3.2+）的每个节点同样是一个 ziplist。Redis 7.0 起逐步被 listpack 取代。
//   - RDB 中，这类对象先以 RDB string 的形式读出一整块字节（可能经过 LZF 压缩），
//     再在这块内存里按 ziplist 自己的格式顺序解析。因此本文件的 cursor 是「块内偏移」，
//     与 Decoder 读取文件流的游标不是一回事。
//
// ziplist 整体布局（多字节整数均为小端）：
//
//	+----------+----------+--------+--------+-----+--------+--------+-------+
//	| zlbytes  | zltail   | zllen  | entry0 | ... | entryN | zlend  |
//	|  4 字节  |  4 字节  | 2 字节 |        |     |        | 0xFF   |
//	+----------+----------+--------+--------+-----+--------+--------+-------+
//
//	zlbytes : 整个 ziplist 占用的字节数（含头尾），用于 O(1) 得到总大小/重分配
//	zltail  : 最后一个 entry 的起始偏移，支持 O(1) 定位表尾（如 RPOP）
//	zllen   : entry 个数；类型是 uint16，真实数量 >= 65535 时会被饱和写成 65535，
//	          此时必须遍历才能得到准确个数（本解析器直接使用该字段，见 readZipList）
//	zlend   : 固定 0xFF，表示结束
//
// 单个 entry 布局：
//
//	+----------+----------+-----------------+
//	| prevlen  | encoding | entry-data ...  |
//	+----------+----------+-----------------+
//
//	prevlen : 前一个 entry 的总字节数。1 字节可表示 0~253；若 >= 254，则首字节写
//	          0xFE（zipBigPrevLen），后跟 4 字节小端真实长度。它的存在使 ziplist 能
//	          从后往前遍历：当前 entry 起始地址 - prevlen = 上一个 entry 起始地址。
//	          注意：正向解析时 prevlen 只用于跳过，不参与元素值计算。
//	encoding: 既描述类型（字符串/整数），也描述长度。高 2 位区分大类：
//	          00/01/10 → 字符串（分别 6 位长度、14 位长度、32 位长度，见下）
//	          11       → 整数（其中 1111xxxx 是 4 位立即数，其余用固定头部标记）
//	entry-data: 字符串的真实字节，或整数的定长小端补码。
//
// 字符串编码细节：
//
//	00xxxxxx                  : 低 6 位即长度（0~63）
//	01xxxxxx yyyyyyyy         : 14 位长度 = 低6位<<8 | yyyyyyyy（大端组合，0~16383）
//	10______ + 4 字节          : 后 4 字节按大端给出长度（低 6 位不使用）
//
// 整数编码细节：
//
//	11000000 + 2 字节小端      : int16
//	11010000 + 4 字节小端      : int32
//	11100000 + 8 字节小端      : int64
//	11110000 + 3 字节小端      : int24（解析时补 0 后算术右移 8 位完成符号扩展）
//	11111110 + 1 字节          : int8
//	1111xxxx（xxxx=0001~1101） : 4 位立即数，值 = xxxx - 1，即 0~12
//
// 使用技巧：
//   - ziplist 不保存「类型元信息」，只保存扁平 entry；hash/zset 会把 key/value
//     或 member/score 交替放进同一个 ziplist，由调用方（hash.go/zset.go）成对消费。
//   - 元素类型完全由每个 entry 自己的 encoding 决定，同一个 ziplist 里字符串和整数
//     可以混排，这与 listpack 的设计一致。
//   - 本文件的 readZipListEntry 是正向解析；若要做「从尾向头」遍历，需利用 prevlen，
//     但当前实现未使用该能力。
package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"unicode"
)

// readZipList 解析一个 ziplist 编码的对象（RDB 中 ziplist 以 RDB string 形式存储）。
//
// 步骤：
//  1. readString 取出整块 ziplist 字节（自动处理长度前缀、整数/LZF 特殊编码）；
//  2. readZipListLength 读 10 字节头部，返回 zllen，并把块内 cursor 移到首个 entry；
//  3. 按 zllen 逐个调用 readZipListEntry，把每个 entry 统一转成 []byte。
//
// 返回值顺序与 ziplist 中 entry 的物理顺序一致。注意 zllen 是 uint16，元素数达到
// 65535 时 Redis 会把它写成饱和值 65535，本函数直接把它当真实个数使用——这是该
// 解析器既有的行为，学习时需留意该边界。
func (dec *Decoder) readZipList() ([][]byte, error) {
	// 从文件流读出一个 RDB string，即整块 ziplist 数据
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	// 读取 10 字节头并返回 zllen；cursor 从 0 前进到首个 entry 的起点
	size := readZipListLength(buf, &cursor)
	// 已知数量，预分配容量，避免循环 append 时反复扩容
	entries := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		// 每个 entry 长度可变，由 entry 内部的 encoding 字段自行描述
		entry, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// readZipListEntry 从 ziplist 数据块中解析一个 entry，返回其内容的字节表示。
//
// 关键点：
//   - 整数值也会被格式化成十进制字符串返回，因为调用方统一按 []byte 消费；
//     这样 ziplist 中「整数编码」与「字符串编码」在外部看来没有差异。
//   - prevlen 只用于定位前一个 entry（反向遍历），正向解析时读出来即丢弃。
//   - 解析完 entry-data 后 cursor 恰好停在下一个 entry 的 prevlen 上。
func (dec *Decoder) readZipListEntry(buf []byte, cursor *int) (result []byte, err error) {
	// prevlen：前一个 entry 的总长度。先读 1 字节
	prevLen := buf[*cursor]
	*cursor++
	// 若为 0xFE，说明前一个 entry >= 254 字节，后面还有 4 字节小端长度需一并跳过
	if prevLen == zipBigPrevLen {
		*cursor += 4
	}
	// encoding 字节：高 2 位决定是字符串还是整数
	header := buf[*cursor]
	*cursor++
	// 取高 2 位：0/1/2 表示字符串且同时给出长度档位，3 表示整数
	typ := header >> 6
	switch typ {
	case zipStr06B: // 00xxxxxx：长度就在低 6 位，0~63
		length := int(header & 0x3f)
		result, err = readBytes(buf, cursor, length)
		return
	case zipStr14B: // 01xxxxxx yyyyyyyy：14 位长度，高 6 位在 header 低 6 位
		b := buf[*cursor]
		*cursor++
		// 大端组合：header 的低 6 位是高位，下一个字节是低 8 位
		length := (int(header&0x3f) << 8) | int(b)
		result, err = readBytes(buf, cursor, length)
		return
	case zipStr32B: // 10______ + 4 字节大端长度
		var lenBytes []byte
		lenBytes, err = readBytes(buf, cursor, 4)
		if err != nil {
			return
		}
		length := int(binary.BigEndian.Uint32(lenBytes))
		result, err = readBytes(buf, cursor, length)
		return
	}
	// 走到这里说明 typ == 3（11xxxxxx），即整数编码。
	// 先匹配「整字节标记」的定长整数；4 位立即数最后单独判断。
	switch header {
	case zipInt08B: // 11111110 + 1 字节，按有符号 int8 解释
		var b byte
		b, err = readByte(buf, cursor)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int8(b)), 10))
		return
	case zipInt16B: // 11000000 + 2 字节小端 int16
		var bs []byte
		bs, err = readBytes(buf, cursor, 2)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(bs))), 10))
		return
	case zipInt32B: // 11010000 + 4 字节小端 int32
		var bs []byte
		bs, err = readBytes(buf, cursor, 4)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))), 10))
		return
	case zipInt64B: // 11100000 + 8 字节小端 int64
		var bs []byte
		bs, err = readBytes(buf, cursor, 8)
		if err != nil {
			return
		}
		result = []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(bs)), 10))
		return
	case zipInt24B: // 11110000 + 3 字节小端 int24
		var bs []byte
		bs, err = readBytes(buf, cursor, 3)
		if err != nil {
			return
		}
		// 在最高位补一个 0 字节，把小端 24 位扩展成小端 32 位
		bs = append([]byte{0}, bs...)
		// 再算术右移 8 位：既去掉补齐的 0 字节，又利用 int32 的符号位完成符号扩展
		result = []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bs))>>8), 10))
		return
	}
	// 4 位立即数：1111xxxx，其中 xxxx 不能为 0000/1110/1111（这些已被上面的定长标记占用）
	if header>>4 == zipInt04B {
		// 存储值是 xxxx = 实际值 + 1，所以这里减 1 还原；可表示 0~12
		result = []byte(strconv.FormatInt(int64(header&0x0f)-1, 10))
		return
	}
	return nil, fmt.Errorf("unknown entry header")
}

// encodeZipListEntry 是 readZipListEntry 的逆过程：把 prevLen 和字符串 val 编码成
// 一个 ziplist entry。prevLen 需要调用方计算并传入（即上一个 entry 的总字节数）。
//
// 编码策略与 Redis 一致：
//   - 先尝试把 val 识别成十进制整数，用更省空间的整数编码；
//   - 否则按字符串编码，长度分 6/14/32 位三档。
func encodeZipListEntry(prevLen uint32, val string) []byte {
	buf := bytes.NewBuffer(nil)
	// encode prevLen
	// < 254 用 1 字节；>= 254 用 0xFE + 4 字节小端真实长度
	if prevLen < zipBigPrevLen {
		buf.Write([]byte{byte(prevLen)})
	} else {
		buf.Write([]byte{0xfe})
		buf0 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf0, prevLen)
		buf.Write(buf0)
	}
	// try int encoding
	// 只有「规范的非负十进制整数」才会走整数编码；前导 0、符号、空串等保持字符串，
	// 否则解码后就无法还原原始文本（例如 "007" 会变成 "7"）。
	intVal, ok := isEncodableUint64(val)
	if ok {
		// use int encoding
		if intVal >= 0 && intVal <= 12 {
			// 4 位立即数：存储值为 val+1，头部固定 0xF0
			buf.Write([]byte{0xf0 | byte(intVal+1)})
		} else if intVal >= math.MinInt8 && intVal <= math.MaxInt8 {
			// bytes.Buffer never failed
			// 11111110 + 1 字节补码
			buf.Write([]byte{byte(zipInt08B), byte(intVal)})
		} else if intVal >= minInt24 && intVal <= maxInt24 {
			// 11110000 + 3 字节小端：先写满 4 字节小端，再取低 3 字节
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(intVal))
			buf.Write([]byte{byte(zipInt24B)})
			buf.Write(buffer[0:3])
		} else if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
			// 11010000 + 4 字节小端 int32
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(intVal))
			buf.Write([]byte{byte(zipInt32B)})
			buf.Write(buffer)
		} else {
			// 超出 int32 范围时使用 11100000 + 8 字节小端 int64
			buffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(buffer, uint64(intVal))
			buf.Write([]byte{byte(zipInt64B)})
			buf.Write(buffer)
		}
		return buf.Bytes()
	}
	// use string encoding
	if len(val) <= maxUint6 {
		// 00xxxxxx：长度直接放低 6 位
		buf.Write([]byte{byte(len(val))}) // 00 + xxxxxx
	} else if len(val) <= maxUint14 {
		// 01xxxxxx yyyyyyyy：高 6 位长度左移 8 位，再或上 0x40 前缀
		buf.Write([]byte{byte(len(val)>>8) | len14BitMask, byte(len(val))})
	} else if len(val) <= math.MaxUint32 {
		// 10______ + 4 字节大端长度。
		// 注意：当前实现实际写入了 8 字节（buffer 长度 8，其中低 4 字节为长度、
		// 高 4 字节为 0），而标准 ziplist 的 32 位长度只占 4 字节。这里仅作说明，
		// 保持原有逻辑不变。
		buffer := make([]byte, 8)
		binary.LittleEndian.PutUint32(buffer, uint32(len(val)))
		buf.Write([]byte{0x80})
		buf.Write(buffer)
	} else {
		panic("too large string")
	}
	// 最后写入字符串真实内容（entry-data）
	buf.Write([]byte(val))
	return buf.Bytes()
}

// isEncodableUint64 判断字符串能否无损地编码成 ziplist 的整数形式。
//
// 返回 (整数值, 是否可编码)。只接受「规范的非负十进制表示」：
//   - 空串不行；
//   - 长度大于 1 且以 '0' 开头不行（"007" 解码回来会变成 "7"，丢失前导 0）；
//   - 必须全部是 ASCII 数字（因此负号、正号、小数点、空白都不行）；
//   - 最后再用 ParseInt(..., 64) 兜底，防止超出 int64 范围。
//
// 函数名里的 Uint64 是历史命名，实际返回的是 int64；由于前面已排除符号，
// 这里得到的值必然非负。
func isEncodableUint64(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	// 拒绝 "0" 开头但长度 > 1 的写法，保证解码后文本完全一致
	if s[0] == '0' && len(s) > 1 {
		return 0, false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return 0, false
		}
	}
	intVal, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return intVal, true
}
