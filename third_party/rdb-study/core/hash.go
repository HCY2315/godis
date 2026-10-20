// hash.go 负责解析与生成 Redis 的 Hash（哈希）类型。
//
// 对应 RDB 类型常量（见 decoder.go）：
//   - typeHash          (4)  普通哈希：长度前缀 + 交替出现的 field/value 字符串
//   - typeHashZipMap    (9)  zipmap：早期 Redis 的紧凑哈希编码（已淘汰）
//   - typeHashZipList   (13) ziplist：field/value 依次排列在同一个 ziplist 中
//   - typeHashListPack  (16) listpack：Redis 7+ 的紧凑哈希编码
//
// 所有紧凑编码都遵循同一约定：field 与 value 成对出现，因此条目总数是偶数，
// 解析时以 2 为步长推进。listpack/ziplist 内部的数据布局细节见对应解析函数。
//
// 使用技巧：读到的三种紧凑编码最终都会被展开成 map[string][]byte，
// 调用方无需区分底层编码；只有做内存分析时才需要用到 detail.RawStringSize。
package core

import (
	"encoding/binary"
	"errors"
	"github.com/hdt3213/rdb/model"
)

/*
	if hlen <= ZIPMAP_VALUE_MAX_FREE
*/

// readHashMap 解析 typeHash（普通哈希）对象。
//
// 字节布局：
//
//	[长度编码 N][field #0][value #0][field #1][value #1]...[field #(N-1)][value #(N-1)]
//
// field/value 都是 RDB string，各自带长度或特殊编码。
// 返回 field -> value 的映射；注意 value 直接引用底层读取到的字节切片。
func (dec *Decoder) readHashMap() (map[string][]byte, error) {
	// 哈希中的键值对数量
	size, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	m := make(map[string][]byte)
	for i := 0; i < int(size); i++ {
		// 每轮读一对 field/value
		field, err := dec.readString()
		if err != nil {
			return nil, err
		}
		value, err := dec.readString()
		if err != nil {
			return nil, err
		}
		// field 作为 map 键需要 string；这里零拷贝转换，避免额外分配
		m[unsafeBytes2Str(field)] = value
	}
	return m, nil
}

// readZipMapHash 解析 typeHashZipMap（RDB type 9）的 zipmap 编码。
//
// zipmap 整体被包在一个 RDB string 里，内部布局：
//
//	[1 字节 zmlen][entry #0][entry #1]...[0xFF]
//
// 每个 entry 由 field、value 两部分组成：
//
//	[field 长度][field 数据][value 长度][free 字节数][value 数据]
//
// 其中 field 没有 free 字段（free 是 value 为了支持原地追加而预留的空闲空间）；
// value 的长度之后要多读 1 字节 free，读完后跳过 free 个填充字节。
//
// zmlen 是条目数量提示：正常情况下可直接使用；但本实现只在 zmlen > 254（即 0xFF）
// 时才认为头部长度不可信，需要实际扫描一遍统计条目数。
//
// 返回值：展开后的 field -> value 映射。
func (dec *Decoder) readZipMapHash() (map[string][]byte, error) {
	// zipmap 是作为一个完整字符串存放的，先取出整段字节
	buf, err := dec.readString()
	if err != nil {
		return nil, err
	}
	cursor := 0
	// 首字节是条目数提示
	bLen, err := readByte(buf, &cursor)
	if err != nil {
		return nil, err
	}
	length := int(bLen)
	if bLen > 254 {
		// 头部长度不可信：先从头扫描统计 entry 数，再回到第一个 entry 重新解析
		cursor0 := cursor // record current cursor
		length, err = countZipMapEntries(buf, &cursor)
		if err != nil {
			return nil, err
		}
		// countZipMapEntries 统计的是 field+value 的个数，除以 2 才是键值对数
		length /= 2
		cursor = cursor0 // recover cursor at begin position of first zip map entry
	}
	m := make(map[string][]byte)
	for i := 0; i < length; i++ {
		// field：length 后没有 free 字节，readFree=false
		fieldB, err := readZipMapEntry(buf, &cursor, false)
		if err != nil {
			return nil, err
		}
		field := unsafeBytes2Str(fieldB)
		// value：length 后还有 1 字节 free，readFree=true
		value, err := readZipMapEntry(buf, &cursor, true)
		if err != nil {
			return nil, err
		}
		m[field] = value
	}
	return m, nil
}

// readZipMapEntryLen 读取 zipmap 中一个 field 或 value 的长度信息。
//
// 参数 readFree 表示「该位置后面是否带 free 字节」：
//   - field 为 false（field 没有 free）
//   - value 为 true （value 有 1 字节 free）
//
// 长度的编码规则（按本实现）：
//
//	0~252 : 该字节本身就是长度
//	253   : 大长度，后跟 5 字节：前 4 字节按大端解释为长度，第 5 字节是 free 数
//	254   : 非法长度（zipmap 中不应出现）
//	255   : 结束标记，返回 length = -1 作为哨兵
//
// 返回值：len、free、error；length == -1 表示遇到 zipmap 结尾。
// return: len, free, error
func readZipMapEntryLen(buf []byte, cursor *int, readFree bool) (int, int, error) {
	b, err := readByte(buf, cursor)
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		// 大长度分支：4 字节长度 + 1 字节 free 一起读出
		bs, err := readBytes(buf, cursor, 5)
		if err != nil {
			return 0, 0, err
		}
		length := int(binary.BigEndian.Uint32(bs))
		free := int(bs[4])
		return length, free, nil
	case 254:
		return 0, 0, errors.New("illegal zip map item length")
	case 255:
		// zipmap 的结束字节，调用方用 -1 判断
		return -1, 0, nil
	default:
		// 普通短长度：长度就是 b；只有 value 才额外读取 free 字节
		var free byte
		if readFree {
			free, err = readByte(buf, cursor)
		}
		return int(b), int(free), err
	}
}

// readZipMapEntry 读取一个完整的 zipmap 条目（field 或 value 的数据部分）。
//
// 先解析长度和 free，再读取 length 字节的数据，最后跳过 free 个填充字节。
// 若长度解析返回 -1（结尾标记），则返回 (nil, nil)，由调用方决定如何结束。
func readZipMapEntry(buf []byte, cursor *int, readFree bool) ([]byte, error) {
	length, free, err := readZipMapEntryLen(buf, cursor, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		// 遇到 0xFF 结束标记
		return nil, nil
	}
	value, err := readBytes(buf, cursor, length)
	if err != nil {
		return nil, err
	}
	*cursor += free // skip free bytes
	return value, nil
}

// countZipMapEntries 从头扫描 zipmap，统计其中 field+value 的总个数。
//
// 由于 zipmap 的 field 与 value 交替出现，且只有 value 带 free 字节，
// 这里用 n 的奇偶性决定 readFree：偶数下标是 field，奇数下标是 value。
// 扫描到 0xFF 结束标记为止。
//
// 注意：函数结束时会把 cursor 重置为 0，但调用方 readZipMapHash 会用自己的
// cursor0 恢复位置，所以这里的重置不影响后续解析。
func countZipMapEntries(buf []byte, cursor *int) (int, error) {
	n := 0
	for {
		readFree := n%2 != 0
		length, free, err := readZipMapEntryLen(buf, cursor, readFree)
		if err != nil {
			return 0, err
		}
		if length == -1 {
			break
		}
		// 跳过数据本体和 free 填充，移动到下一个长度字段
		*cursor += length + free
		n++
	}
	*cursor = 0 // reset cursor
	return n, nil
}

// readZipListHash 解析 typeHashZipList（RDB type 13）对象。
//
// 整个哈希被编码成一个 ziplist 字符串，ziplist 内部按 [field, value, field, value, ...]
// 交替存放，因此 zllen（entry 个数）是键值对数的 2 倍，循环步长为 2。
//
// 返回值：field -> value 映射，以及记录原始 ziplist 字节数的 detail。
func (dec *Decoder) readZipListHash() (map[string][]byte, *model.ZiplistDetail, error) {
	// 先整体读出 ziplist 字符串
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// readZipListLength 会解析 10 字节头并跳过头部，返回 entry 总数
	size := readZipListLength(buf, &cursor)
	m := make(map[string][]byte)
	// field/value 成对出现，步长 2
	for i := 0; i < size; i += 2 {
		key, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		val, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		m[unsafeBytes2Str(key)] = val
	}
	detail := &model.ZiplistDetail{
		RawStringSize: len(buf),
	}
	return m, detail, nil
}

// readListPackHash 解析 typeHashListPack（RDB type 16）对象。
//
// 与 ziplist 版本结构完全对应：整个哈希是一个 listpack 字符串，
// 内部按 [field, value, field, value, ...] 交替存放，所以同样以 2 为步长。
// 区别只在 listpack 的头部和 entry 编码格式（见 listpack.go）。
func (dec *Decoder) readListPackHash() (map[string][]byte, *model.ListpackDetail, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// 解析 listpack 6 字节头（4 字节总长度 + 2 字节 entry 数）并跳过
	size := readListPackLength(buf, &cursor)
	m := make(map[string][]byte)
	for i := 0; i < size; i += 2 {
		// listpack 的整数 entry 会被格式化成字符串，统一按 string 返回
		key, err := dec.readListPackEntryAsString(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		val, err := dec.readListPackEntryAsString(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		m[unsafeBytes2Str(key)] = val
	}
	detail := &model.ListpackDetail{
		RawStringSize: len(buf),
	}
	return m, detail, nil
}

// WriteHashMapObject 是编码器写 Hash 对象的统一入口。
//
// 策略与 Redis 的 hash-max-ziplist-* 配置一致：
//  1. 键值对数量 <= maxEntries 且所有 value 长度 <= maxValue 时，使用 ziplist 编码；
//  2. 否则使用普通哈希编码（typeHash），逐个写 field/value。
//
// options 目前支持 WithTTL，由 beforeWriteObject 统一处理。
func (enc *Encoder) WriteHashMapObject(key string, hash map[string][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteZipListHashMap(key, hash, options...)
	if err != nil {
		return err
	}
	if !ok {
		// 不满足 ziplist 条件，回退到普通哈希编码
		err = enc.writeHashEncoding(key, hash, options...)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

// writeHashEncoding 用普通哈希编码（typeHash）写对象。
//
// 字节布局：
//
//	[typeHash][key][长度编码 N][field #0][value #0]...[field #(N-1)][value #(N-1)]
//
// field/value 都使用 writeString 写出；由于遍历 map 的顺序是随机的，
// 同一个 map 多次编码得到的字节顺序可能不同（RDB 语义上不要求顺序）。
func (enc *Encoder) writeHashEncoding(key string, hash map[string][]byte, options ...interface{}) error {
	err := enc.write([]byte{typeHash})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	// 先写键值对数量，解析方据此决定循环次数
	err = enc.writeLength(uint64(len(hash)))
	if err != nil {
		return err
	}
	for field, value := range hash {
		err = enc.writeString(field)
		if err != nil {
			return err
		}
		err = enc.writeString(unsafeBytes2Str(value))
		if err != nil {
			return err
		}
	}
	return nil
}

// tryWriteZipListHashMap 尝试用 ziplist 编码写哈希。
//
// 与 tryWriteListZipList 相同的 (ok, err) 语义：
// (false, nil) 表示尚未写出任何字节，调用方可以安全回退；
// (true, err) 表示已经开始写 type/key，只能中止。
//
// 注意：这里只检查了 value 的长度是否超过 maxValue，并未单独检查 field 的长度，
// 这是现有实现的取舍（注释仅说明现状，不改变行为）。
func (enc *Encoder) tryWriteZipListHashMap(key string, hash map[string][]byte, options ...interface{}) (bool, error) {
	if len(hash) > enc.hashZipListOpt.getMaxEntries() {
		return false, nil
	}
	maxValue := enc.hashZipListOpt.getMaxValue()
	for _, v := range hash {
		if len(v) > maxValue {
			return false, nil
		}
	}
	err := enc.write([]byte{typeHashZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	// ziplist 内 field/value 交替存放：展开成 [f0, v0, f1, v1, ...]
	entries := make([]string, 0, len(hash)*2)
	for k, v := range hash {
		entries = append(entries, k, unsafeBytes2Str(v))
	}
	err = enc.writeZipList(entries)
	if err != nil {
		return true, err
	}
	return true, nil
}

// 使用技巧：
//  1. 三种紧凑编码在解析后都变成 map，业务代码无需关心原始编码；
//  2. 如果要还原 Redis 的内存占用，请使用 detail.RawStringSize（编码后字节数），
//     而不是 len(map)，后者只反映逻辑条目数；
//  3. zipmap 是历史编码（Redis 2.6 之前），现代 RDB 一般只会遇到 ziplist/listpack。
