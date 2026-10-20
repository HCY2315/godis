package memprofiler

import (
	"github.com/hdt3213/rdb/model"
)

// used to evaluate memory usage

// RedisMeta 记录 Redis 的版本与架构信息。
// 注意：当前实现尚未按版本/架构做差异化估算（见 SizeOfObject 里的 todo），
// 这是一个预留的扩展点。
type RedisMeta struct {
	Version string
	Bits    int // 32/64
}

// SizeOfObject 估算一个 Redis 对象在内存中占用的字节数。
//
// 计算方式 = 顶层固定开销 + 对象自身开销，其中：
//   - 顶层固定开销 topLevelObjectOverhead：主字典 dictEntry、key 的 SDS、redisObject、（有 TTL 时）过期字典项；
//   - 对象自身开销：按具体类型和底层编码分别估算（字符串按 SDS，list/set/hash/zset/stream 各有公式）。
//
// 这是一个「估算值」，用于内存分析（找大 key、按前缀统计），不保证与 INFO memory 完全一致。
//
// 用法：
//
//	size := memprofiler.SizeOfObject(obj) // 单位：字节
func SizeOfObject(obj model.RedisObject) int {
	// todo: memory profile by redis version and architecture
	size := topLevelObjectOverhead(obj.GetKey(), obj.GetExpiration() != nil)
	switch o := obj.(type) {
	case *model.StringObject:
		size += sizeOfString(unsafeBytes2Str(o.Value))
	case *model.ListObject:
		size += sizeOfListObject(o)
	case *model.SetObject:
		size += sizeOfSetObject(o)
	case *model.HashObject:
		size += sizeOfHashObject(o)
	case *model.ZSetObject:
		size += sizeOfZSetObject(o)
	case *model.StreamObject:
		size += sizeOfStreamObject(o)
	}
	return size
}
