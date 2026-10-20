package memprofiler

import (
	"github.com/hdt3213/rdb/model"
	"math"
	"strconv"
)

// sizeOfListObject 根据 list 对象的底层编码选择对应的估算方式。
//
// Redis list 在不同版本中使用过多种编码：
//   - quicklist：Redis 3.2~6.x 的默认编码，由多个 ziplist 节点组成的双向链表；
//   - list：早期版本的普通双向链表（adlist）；
//   - ziplist：小型 list 的紧凑编码；
//   - quicklist2：Redis 7.0 起使用，节点可以存 listpack 或 plain 字符串。
func sizeOfListObject(obj *model.ListObject) int {
	switch obj.GetEncoding() {
	case model.QuickListEncoding:
		detail := obj.Extra.(*model.QuicklistDetail)
		return sizeOfQuicklist(detail)
	case model.ListEncoding:
		return sizeOfList(obj.Values)
	case model.ZipListEncoding:
		return sizeOfZiplist(obj.Values)
	case model.QuickList2Encoding:
		detail := obj.Extra.(*model.Quicklist2Detail)
		return sizeOfQuicklist2(obj.Values, detail)
	}
	return 0
}

// zipListIntEntryOverhead 估算 ziplist 中一个整数 entry 的字节数。
//
// ziplist entry 由三部分组成：
//   - prevlen：前一个 entry 的长度，1 字节或 5 字节；
//   - encoding/header：1 字节，标记该 entry 是整数以及整数宽度；
//   - 实际数据：根据整数范围使用 0/1/2/3/4/8 字节。
//
// 本实现按值的大小选择数据宽度，并保留原有的 prevlen 估算口径。
func zipListIntEntryOverhead(v int) int {
	header := 1
	size := 0
	if v < 12 {
		// 0~11（4 位立即数）可以直接编码进 encoding 字节，不占额外数据。
		size = 0
	} else if v < int(math.Pow(2, 8)) {
		size = 1
	} else if v < int(math.Pow(2, 16)) {
		size = 2
	} else if v < int(math.Pow(2, 24)) {
		size = 3
	} else if v < int(math.Pow(2, 32)) {
		size = 4
	} else {
		size = 8
	}
	// prevlen 表示前一条目长度字段的宽度；这里沿用原实现的估算方式：
	// 默认 1 字节，size < 254 时按 5 字节计算。
	prevLen := 1
	if size < 254 {
		prevLen = 5
	}
	return prevLen + header + size
}

// zipListStrEntryOverhead 估算 ziplist 中一个字符串 entry 的字节数。
//
// 组成同样是 prevlen + encoding header + 字符串内容：
//   - 长度 <= 63：使用 6 位长度编码，header 为 1 字节；
//   - 其余情况：使用 32 位长度编码，header 为 5 字节；
//   - 代码中保留了 size <= 1 时 header=2 的分支（沿用原实现）。
func zipListStrEntryOverhead(v string) int {
	size := len(v)
	header := 1
	if size <= 63 {
		header = 1
	} else if size <= 1 {
		header = 2
	} else {
		header = 5
	}
	// prevlen 的估算方式与整数 entry 保持一致。
	prevLen := 1
	if size < 254 {
		prevLen = 5
	}
	return prevLen + header + size
}

// sizeOfZiplist 估算一个 ziplist 的总字节数。
//
// ziplist 布局：<zlbytes><zltail><zllen><entry>...<zlend>
//   - zlbytes：4 字节，整个 ziplist 的字节数；
//   - zltail：4 字节，最后一个 entry 的偏移；
//   - zllen：2 字节，entry 数量；
//   - zlend：1 字节，结束标记 0xFF。
//
// 每个元素根据能否解析为整数，分别按整数 entry 或字符串 entry 估算。
func sizeOfZiplist(values [][]byte) int {
	// See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
	// <zlbytes><zltail><zllen><entry><entry><zlend>
	size := 4 + 4 + 2 + 1
	for _, value := range values {
		str := unsafeBytes2Str(value)
		i, err := strconv.Atoi(str)
		if err != nil { // not int
			size += zipListStrEntryOverhead(str)
		} else { // is int
			size += zipListIntEntryOverhead(i)
		}
	}
	return size
}

// sizeOfQuicklist 估算 quicklist 编码的 list 内存。
//
// quicklist 本身是一个双向链表，结构体包含 head/tail 指针、元素计数、
// 节点数以及 fill/compress 等配置字段。
//
// 每个 quicklistNode 再包含 prev/next/ziplist 指针、大小、元素计数等字段；
// 节点内部则是一个完整的 ziplist。
func sizeOfQuicklist(detail *model.QuicklistDetail) int {
	// quicklist 结构体：2 个指针 + 1 个 unsigned long + 2 个 int。
	size := 2*sizeOfPointer() + sizeOfLong() + 2*4
	// quicklistNode 结构体：4 个指针 + 1 个 long + 2 个 int。
	nodeOverhead := 4*sizeOfPointer() + sizeOfLong() + 2*4
	size += len(detail.ZiplistStruct) * nodeOverhead
	for _, ziplist := range detail.ZiplistStruct {
		size += sizeOfZiplist(ziplist)
	}
	return size
}

// sizeOfQuicklist2 估算 Redis 7.0+ 的 quicklist2 编码内存。
//
// quicklist2 的节点有两种容器类型：
//   - plain：节点直接保存一个字符串，按 SDS 估算；
//   - listpack：节点保存 listpack，需要 listpack 头（4 字节总长 + 2 字节元素数
//     + 1 字节结束标记）加上每个 entry 的大小。
//
// detail.NodeEncodings 记录每个节点的容器类型，
// detail.ListPackEntrySize 记录对应节点内每个 entry 的字节数。
func sizeOfQuicklist2(values [][]byte, detail *model.Quicklist2Detail) int {
	// quicklist 结构体（Redis 7）：2 个指针 + 2 个 long + 2 个 int。
	size := 2*sizeOfPointer() + 2*sizeOfLong() + 2*4
	// https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/quicklist.h#L60
	// quicklistNode（Redis 7）：3 个指针 + 1 个 long + 1 个 int。
	nodeOverhead := 3*sizeOfPointer() + sizeOfLong() + 4
	size += nodeOverhead * len(detail.NodeEncodings)
	for i, enc := range detail.NodeEncodings {
		if enc == model.QuicklistNodeContainerPlain {
			// plain 节点：直接保存一个 SDS 字符串。
			size += sizeOfString(unsafeBytes2Str(values[i]))
		} else {
			// listpack overhead: <total_bytes><size>...<end>
			// listpack 头部：4 字节总长度 + 2 字节元素数 + 1 字节结束标记。
			size += 4 + 2 + 1
			for _, s := range detail.ListPackEntrySize[i] {
				size += int(s)
			}
		}
	}
	return size
}

// sizeOfList 估算旧版普通双向链表（adlist）编码的 list 内存。
//
// adlist 结构体：5 个指针 + 1 个 unsigned long（长度）。
// 每个 listNode：3 个指针（prev、next、value）。
// 每个元素的值本身是一个 SDS 字符串。
func sizeOfList(values [][]byte) int {
	// See https://github.com/antirez/redis/blob/unstable/src/adlist.h
	// A list has 5 pointers + an unsigned long
	size := 5*sizeOfPointer() + sizeOfLong()
	// A node has 3 pointers
	entryHeadSize := 3 * sizeOfPointer()
	size += len(values) * entryHeadSize
	// fixme: since redis 4.0, make it compatible with older version
	for _, v := range values {
		s := unsafeBytes2Str(v)
		size += sizeOfString(s)
	}
	return size
}
