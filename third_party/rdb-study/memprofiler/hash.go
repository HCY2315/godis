package memprofiler

import "github.com/hdt3213/rdb/model"

// hashTableEntryOverhead 返回 Redis 字典中一个 dictEntry 的内存开销。
//
// dictEntry 结构大致包含：
//   - 指向下一个元素的 next 指针；
//   - 指向 key 的指针；
//   - 保存 value 的 union（这里按 8 字节估算）。
//
// 因此总共是 2 个指针 + 8 字节，即 24 字节（64 位平台）。
func hashTableEntryOverhead() int {
	// See  https://github.com/antirez/redis/blob/unstable/src/dict.h
	// Each dictEntry has 2 pointers + int64
	return 2*sizeOfPointer() + 8
}

// hashtableOverhead 估算 Redis 字典（dict）除 key/value 本身之外的结构开销。
//
// 公式拆解：
//   - 4 字节：dict 中的一些标志/计数器；
//   - 7*sizeOfLong()：dict/dictht 中的长度、掩码、计数器等字段；
//   - 4*sizeOfPointer()：dict 的指针、两个 dictht 的 table 指针等；
//   - nextPower(size)*sizeOfPointer()*3/2：桶数组本身。
//
// 桶数组长度会取大于元素数量的下一个 2 的幂。Redis 在 rehash 期间会同时存在
// 新旧两张桶表，所以这里按最坏情况乘以 1.5 来估算（旧表 + 新表的部分）。
func hashtableOverhead(size int) int {
	// See  https://github.com/antirez/redis/blob/unstable/src/dict.h
	// See the structures dict and dictht
	// 2 * (3 unsigned longs + 1 pointer) + int + long + 2 pointers
	//
	// Additionally, see **table in dictht
	// The length of the table is the next nextPower of 2
	// When the hashtable is rehashing, another instance of **table is created
	// Due to the possibility of rehashing during loading, we calculate the worse
	// case in which both tables are allocated, and so multiply
	// the size of **table by 1.5
	return 4 + 7*sizeOfLong() + 4*sizeOfPointer() + nextPower(size)*sizeOfPointer()*3/2
}

// sizeOfHashObject 估算 Redis hash 对象的内存占用。
//
// Redis hash 有两种底层编码：
//   - ziplist / listpack：紧凑编码，直接把整个原始字符串的大小作为占用，
//     此时 obj.Extra 中保存了从 RDB 读到的原始字节数 RawStringSize；
//   - hashtable：普通哈希表，需要计算 dict 结构开销，并为每个 field/value
//     加上 dictEntry、field 的 SDS、value 的 SDS。
func sizeOfHashObject(obj *model.HashObject) int {
	if obj.GetEncoding() == model.ZipListEncoding {
		extra := obj.Extra.(*model.ZiplistDetail)
		return extra.RawStringSize
	} else if obj.GetEncoding() == model.ListPackEncoding {
		extra := obj.Extra.(*model.ListpackDetail)
		return extra.RawStringSize
	}
	size := hashtableOverhead(len(obj.Hash))
	for k, v := range obj.Hash {
		// 每个键值对占用一个 dictEntry。
		size += hashTableEntryOverhead()
		// field 和 value 都以 SDS 形式存储。
		size += sizeOfString(k)
		size += sizeOfString(unsafeBytes2Str(v))
	}
	return size
}

// sizeOfSetObject 估算 Redis set 对象的内存占用。
//
// Redis set 的底层编码：
//   - intset / listpack：紧凑编码，直接使用 RDB 中记录的原始字符串大小；
//   - hashtable：普通哈希表，元素只作为 key 存入字典，value 为 NULL。
//
// 因此 hashtable 编码下，每个成员需要一个 dictEntry 和一份成员 SDS，
// 不需要再为 value 计算字符串内存。
func sizeOfSetObject(obj *model.SetObject) int {
	if obj.GetEncoding() == model.IntSetEncoding {
		extra := obj.Extra.(*model.IntsetDetail)
		return extra.RawStringSize
	} else if obj.GetEncoding() == model.ListPackEncoding {
		extra := obj.Extra.(*model.ListpackDetail)
		return extra.RawStringSize
	}
	size := hashtableOverhead(len(obj.Members))
	for _, v := range obj.Members {
		size += hashTableEntryOverhead() + sizeOfString(unsafeBytes2Str(v))
	}
	return size
}
