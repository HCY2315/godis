// set.go 负责解析与生成 Redis 的 Set（集合）类型。
//
// 对应 RDB 类型常量（见 decoder.go）：
//   - typeSet          (2)  普通集合：长度前缀 + 逐个字符串成员
//   - typeSetIntSet    (11) intset：当所有成员都是整数时，用定宽整数数组紧凑存储
//   - typeSetListPack  (20) listpack：Redis 7+ 的紧凑集合编码（成员可混合类型）
//
// 解析侧三种编码最终都返回 [][]byte，调用方无需区分；只有做内存分析时才会用到
// detail.RawStringSize。编码侧会优先尝试 intset，失败再退回普通集合。
//
// 使用技巧：intset 里的整数在解析后会被格式化为十进制字符串，因此
// 原本是整数和原本是字符串的成员在返回值里看起来完全一样。
package core

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// readSet 解析 typeSet（普通集合）对象。
//
// 字节布局：
//
//	[长度编码 N][RDB string #0][RDB string #1]...[RDB string #(N-1)]
//
// 集合在 RDB 中是无序的，返回切片的顺序就是文件中的存储顺序，不代表任何语义。
func (dec *Decoder) readSet() ([][]byte, error) {
	// 成员个数
	size64, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	size := int(size64)
	// 已知数量，预分配容量减少扩容
	values := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		// 每个成员都是一个 RDB string（可能整数/LZF 编码）
		val, err := dec.readString()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// readIntSet 解析 typeSetIntSet（RDB type 11）的 intset 编码。
//
// intset 整体被包在一个 RDB string 里，内部布局（全部小端）：
//
//	offset 0  4 字节 encoding    ：每个元素的字节宽度，只能是 2/4/8
//	offset 4  4 字节 length      ：元素个数（基数）
//	offset 8  ...               ：连续存放 length 个定宽有符号整数，按值升序排列
//
// 元素按 encoding 解释：
//
//	2 -> int16，4 -> int32，8 -> int64
//
// 返回：把每个整数格式化成十进制字符串后的成员列表，以及原始字节数 detail。
// 注意：intset 保证元素有序且唯一，但返回的 [][]byte 只保留值，不保留这个约束信息。
func (dec *Decoder) readIntSet() (result [][]byte, detail *model.IntsetDetail, err error) {
	var buf []byte
	// 先读出整个 intset 字符串
	buf, err = dec.readString()
	if err != nil {
		return nil, nil, err
	}
	// 前 4 字节：每个整数的宽度（2/4/8）
	sizeBytes := buf[0:4]
	intSize := int(binary.LittleEndian.Uint32(sizeBytes))
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return nil, nil, fmt.Errorf("unknown intset encoding: %d", intSize)
	}
	// 接下来 4 字节：元素个数
	lenBytes := buf[4:8]
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	cursor := 8
	result = make([][]byte, 0, cardinality)
	for i := uint32(0); i < cardinality; i++ {
		// 按固定宽度读取一个整数
		var intBytes []byte
		intBytes, err = readBytes(buf, &cursor, intSize)
		if err != nil {
			return
		}
		var intString string
		switch intSize {
		case 2:
			// 用 int16 转换保留符号（负数为补码）
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			// uint64 -> int64 是位模式重解释，负数补码可正确还原
			intString = strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)
		}
		result = append(result, []byte(intString))
	}
	detail = &model.IntsetDetail{
		RawStringSize: len(buf),
	}
	return
}

// readListPackSet 解析 typeSetListPack（RDB type 20）对象。
//
// 整个集合被编码成一个 listpack 字符串，成员逐个存放（不像 hash/zset 那样成对）。
// listpack 头 6 字节（4 字节总长度 + 2 字节 entry 数）由 readListPackLength 解析并跳过，
// 之后每个 entry 就是一个成员；整数 entry 会被格式化成十进制字符串。
func (dec *Decoder) readListPackSet() ([][]byte, *model.ListpackDetail, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// 读取 listpack 头部，得到成员个数，cursor 移动到第一个 entry
	size := readListPackLength(buf, &cursor)
	values := make([][]byte, 0, size)
	for i := 0; i < size; i += 1 {
		member, err := dec.readListPackEntryAsString(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		values = append(values, member)
	}
	detail := &model.ListpackDetail{
		RawStringSize: len(buf),
	}
	return values, detail, nil
}

// WriteSetObject 是编码器写 Set 对象的统一入口。
//
// 策略：如果所有成员都能表示为十进制整数，优先用 intset 编码（更紧凑）；
// 否则退回普通集合（typeSet），逐个写成员字符串。
// options 目前支持 WithTTL，由 beforeWriteObject 统一处理。
func (enc *Encoder) WriteSetObject(key string, values [][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteIntSetEncoding(key, values)
	if err != nil {
		return err
	}
	if !ok {
		// 存在非整数成员或超出 int64 范围，无法用 intset
		err = enc.writeSetEncoding(key, values)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

// writeSetEncoding 用普通集合编码（typeSet）写对象。
//
// 字节布局：
//
//	[typeSet][key][长度编码 N][member #0]...[member #(N-1)]
//
// 每个成员都用 writeString 写出，因此纯数字字符串也可能被压缩成整数编码。
func (enc *Encoder) writeSetEncoding(key string, values [][]byte) error {
	err := enc.write([]byte{typeSet})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	// 成员数量，解析方据此循环
	err = enc.writeLength(uint64(len(values)))
	if err != nil {
		return err
	}
	for _, value := range values {
		err = enc.writeString(unsafeBytes2Str(value))
		if err != nil {
			return err
		}
	}
	return nil
}

// tryWriteIntSetEncoding 尝试把集合编码成 intset。
//
// 前置条件：所有成员都必须是「可还原的十进制整数」——isEncodableUint64 会拒绝
// 空串、带前导 0 的串（"007"）、正负号、非数字字符等，因为这类字符串一旦转成整数
// 再格式化回来就和原值不同了。
//
// 步骤：
//  1. 遍历所有成员，转换成 int64 并同时求 min/max；
//  2. 根据 min/max 选择每个元素的最小定宽：int16(2) / int32(4) / int64(8)；
//  3. 按值升序排序（intset 要求有序存储）；
//  4. 写 type/key，再写 intset 的 8 字节头 + 定宽元素数组。
//
// 返回值 (ok, err) 与其它 tryWriteXxx 一致：
// (false, nil) 表示尚未写字节、可以回退；(true, err) 表示已开始写、只能中止。
//
// 说明：min/max 的初始值与 else-if 写法存在一个边界情况——若第一个元素就是最大值，
// 且后续元素都比它小，max 不会被更新而停留在 MinInt64；此时宽度可能只依据 min 选择，
// 对超大首元素存在截断风险。这里仅如实说明原实现行为，不修改逻辑。
func (enc *Encoder) tryWriteIntSetEncoding(key string, values [][]byte) (bool, error) {
	max := int64(math.MinInt64)
	min := int64(math.MaxInt64)
	intList := make([]int64, len(values))
	for i, v := range values {
		str := unsafeBytes2Str(v)
		intV, ok := isEncodableUint64(str)
		if !ok {
			// 只要有一个成员不能无损转成整数，就放弃 intset
			return false, nil
		}
		// 维护区间：[min, max] 决定最终使用哪种定宽
		if intV < min {
			min = intV
		} else if intV > max {
			max = intV
		}
		intList[i] = intV
	}
	// 默认 8 字节；能装进更窄的宽度就缩小以节省空间
	intSize := uint32(8)
	if min >= math.MinInt16 && max <= math.MaxInt16 {
		intSize = 2
	} else if min >= math.MinInt32 && max <= math.MaxInt32 {
		intSize = 4
	}
	// intset 内部要求元素按升序排列
	sort.Slice(intList, func(i, j int) bool {
		return intList[i] < intList[j]
	})

	err := enc.write([]byte{typeSetIntSet})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	// intset 头部 8 字节：encoding(4) + length(4)，后面预分配元素空间
	buf := make([]byte, 8, 8+int(intSize)*len(values))
	binary.LittleEndian.PutUint32(buf[0:4], intSize)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(values)))

	for _, value := range intList {
		// 有符号整数按补码写入；uint16/32/64 转换只是改变解释方式，不改变位模式
		switch intSize {
		case 2:
			binary.LittleEndian.PutUint16(enc.buffer[0:2], uint16(value))
			buf = append(buf, enc.buffer[0:2]...)
		case 4:
			binary.LittleEndian.PutUint32(enc.buffer[0:4], uint32(value))
			buf = append(buf, enc.buffer[0:4]...)
		case 8:
			binary.LittleEndian.PutUint64(enc.buffer, uint64(value))
			buf = append(buf, enc.buffer...)
		}
	}
	// 用 writeNanString 原样写入二进制，避免整段数据被误判为可整数编码
	err = enc.writeNanString(unsafeBytes2Str(buf))
	if err != nil {
		return true, err
	}
	return true, nil
}
