// Package model 定义了 rdb 解析结果的「统一数据模型」。
//
// 无论 RDB 文件里用的是哪种底层编码（普通数组、ziplist、listpack、quicklist、intset……），
// 解析后都会被还原成这里的几种对象：StringObject / ListObject / SetObject / HashObject /
// ZSetObject / StreamObject 等。调用方只面向这些对象编程，无需关心底层编码差异。
//
// 两个容易混淆的概念：
//   - Type    ：Redis 的「逻辑类型」，即用户看到的 string/list/set/hash/zset/stream；
//   - Encoding：底层的「物理编码」，如 ziplist/listpack/quicklist2/intset 等。
//     同一个 Type 可能对应多种 Encoding（例如 list 可能是 quicklist、ziplist 或普通 list）。
package model

import (
	"encoding/json"
	"time"
)

// 逻辑类型常量：对应 Redis 对外暴露的数据类型（命令里的类型）。
const (
	// StringType is redis string
	StringType = "string"
	// ListType is redis list
	ListType = "list"
	// SetType is redis set
	SetType = "set"
	// HashType is redis hash
	HashType = "hash"
	// ZSetType is redis sorted set
	ZSetType = "zset"
	// AuxType is redis metadata key-value pair
	AuxType = "aux"
	// DBSizeType is for RDB_OPCODE_RESIZEDB
	DBSizeType = "dbsize"
	// StreamType is a redis stream
	StreamType = "stream"
)

// 物理编码常量：对应数据在 RDB 文件/内存中的实际存储方式（Redis 的 encoding 字段）。
// 了解它们有助于理解「为什么同样 100 个元素，内存占用差别很大」。
const (
	// StringEncoding for string
	StringEncoding = "string"
	// ListEncoding is formed by a length encoding and some string
	ListEncoding = "list"
	// SetEncoding is formed by a length encoding and some string
	SetEncoding = "set"
	// ZSetEncoding is formed by a length encoding and some string
	ZSetEncoding = "zset"
	// HashEncoding is formed by a length encoding and some string
	HashEncoding = "hash"
	// ZSet2Encoding is zset version2 which stores doubles in binary format
	ZSet2Encoding = "zset2"
	// ZipMapEncoding has been deprecated
	ZipMapEncoding = "zipmap"
	// ZipListEncoding  stores data in contiguous memory
	ZipListEncoding = "ziplist"
	// IntSetEncoding is a ordered list of integers
	IntSetEncoding = "intset"
	// QuickListEncoding is a list of ziplist
	QuickListEncoding = "quicklist"
	// ListPackEncoding is a new replacement for ziplist
	ListPackEncoding = "listpack"
	// QuickList2Encoding is a list of listpack
	QuickList2Encoding = "quicklist2"
)

// CallbackFunc 是解析回调：每解析出一个对象就调用一次。
// 返回 true 表示继续解析下一个对象，返回 false 表示停止解析（提前退出）。
//
// 用法：
//
//	dec.Parse(func(obj model.RedisObject) bool {
//	    fmt.Println(obj.GetKey())
//	    return true
//	})
type CallbackFunc func(object RedisObject) bool

// RedisObject 是所有 Redis 对象的公共接口。
// 通过它可以用统一的方式获取对象的元信息，再用类型断言取具体数据。
//
// 用法示例：
//
//	switch o := obj.(type) {
//	case *model.StringObject: fmt.Println(o.Value)
//	case *model.ListObject:   fmt.Println(o.Values)
//	}
type RedisObject interface {
	// GetType returns redis type of object: string/list/set/hash/zset
	GetType() string
	// GetKey returns key of object
	GetKey() string
	// GetDBIndex returns db index of object
	GetDBIndex() int
	// GetExpiration returns expiration time, expiration of persistent object is nil
	GetExpiration() *time.Time
	// GetSize returns rdb value size in Byte
	GetSize() int
	// GetElemCount returns number of elements in list/set/hash/zset
	GetElemCount() int
	// GetEncoding returns encoding of object
	GetEncoding() string
}

// BaseObject 是所有具体对象（String/List/Set/Hash/ZSet...）的「公共基底」，
// 通过结构体嵌入(*BaseObject)被复用，从而统一提供 DB/Key/过期时间/大小 等元信息。
type BaseObject struct {
	DB         int         `json:"db"`                   // DB is db index of redis object
	Key        string      `json:"key"`                  // Key is key of redis object
	Expiration *time.Time  `json:"expiration,omitempty"` // Expiration is expiration time, expiration of persistent object is nil
	Size       int         `json:"size"`                 // Size is rdb value size in Byte
	Type       string      `json:"type"`                 // Type is one of string/list/set/hash/zset
	Encoding   string      `json:"encoding"`             // Encoding is the exact encoding method
	Extra      interface{} `json:"-"`                    // Extra stores more detail of encoding for memory profiler and other usages
}

// GetKey returns key of object
func (o *BaseObject) GetKey() string {
	return o.Key
}

// GetDBIndex returns db index of object
func (o *BaseObject) GetDBIndex() int {
	return o.DB
}

// GetEncoding returns encoding of object
func (o *BaseObject) GetEncoding() string {
	return o.Encoding
}

// GetExpiration returns expiration time, expiration of persistent object is nil
func (o *BaseObject) GetExpiration() *time.Time {
	return o.Expiration
}

// GetSize  returns rdb value size in Byte
func (o *BaseObject) GetSize() int {
	return o.Size
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *BaseObject) GetElemCount() int {
	return 0
}

// StringObject stores a string object
//
// Value 用 []byte 而非 string：Redis 的字符串是「二进制安全」的，
// 可以存任意字节（图片、序列化数据等），用 []byte 才不会丢失信息。
type StringObject struct {
	*BaseObject
	Value []byte
}

// GetType returns redis object type
func (o *StringObject) GetType() string {
	return StringType
}

// MarshalJSON 自定义 JSON 序列化。
//
// 为什么要自定义？Go 默认把 []byte 序列化成 base64 字符串，
// 那样输出的可读性很差；这里改成把字节直接当字符串输出，便于人阅读。
// 注意：其它对象的 MarshalJSON 都是同一套路（把 []byte 转成 string/map）。
func (o *StringObject) MarshalJSON() ([]byte, error) {
	o2 := struct {
		*BaseObject
		Value string `json:"value"`
	}{
		BaseObject: o.BaseObject,
		Value:      string(o.Value),
	}
	return json.Marshal(o2)
}

// ListObject stores a list object
//
// Values 按顺序保存列表元素（Redis 的 list 是有序的，类似双向链表/数组）。
type ListObject struct {
	*BaseObject
	Values [][]byte
}

// GetType returns redis object type
func (o *ListObject) GetType() string {
	return ListType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *ListObject) GetElemCount() int {
	return len(o.Values)
}

// MarshalJSON marshal []byte as string
func (o *ListObject) MarshalJSON() ([]byte, error) {
	values := make([]string, len(o.Values))
	for i, v := range o.Values {
		values[i] = string(v)
	}
	o2 := struct {
		*BaseObject
		Values []string `json:"values"`
	}{
		BaseObject: o.BaseObject,
		Values:     values,
	}
	return json.Marshal(o2)
}

// HashObject stores a hash object
//
// Hash 是「字段名 → 值」的映射（Redis hash 常用于存对象，如 user:1 的 name/age）。
// 注意：map 无序，遍历顺序不固定。
type HashObject struct {
	*BaseObject
	Hash map[string][]byte
}

// GetType returns redis object type
func (o *HashObject) GetType() string {
	return HashType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *HashObject) GetElemCount() int {
	return len(o.Hash)
}

// MarshalJSON marshal []byte as string
func (o *HashObject) MarshalJSON() ([]byte, error) {
	m := make(map[string]string)
	for k, v := range o.Hash {
		m[k] = string(v)
	}
	o2 := struct {
		*BaseObject
		Hash map[string]string `json:"hash"`
	}{
		BaseObject: o.BaseObject,
		Hash:       m,
	}
	return json.Marshal(o2)
}

// SetObject stores a set object
//
// Members 是无序去重的成员集合（Redis set 保证成员唯一）。
type SetObject struct {
	*BaseObject
	Members [][]byte
}

// GetType returns redis object type
func (o *SetObject) GetType() string {
	return SetType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *SetObject) GetElemCount() int {
	return len(o.Members)
}

// MarshalJSON marshal []byte as string
func (o *SetObject) MarshalJSON() ([]byte, error) {
	values := make([]string, len(o.Members))
	for i, v := range o.Members {
		values[i] = string(v)
	}
	o2 := struct {
		*BaseObject
		Members []string `json:"members"`
	}{
		BaseObject: o.BaseObject,
		Members:    values,
	}
	return json.Marshal(o2)
}

// ZSetEntry is a key-score in sorted set
//
// Member 是成员，Score 是排序分值。ZSet 按 Score 升序排列，Score 相同则按 Member 字典序。
type ZSetEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// ZSetObject stores a sorted set object
//
// Entries 是有序集合成员及其分值（Redis zset = 跳表 + 字典，支持按分值范围查询）。
type ZSetObject struct {
	*BaseObject
	Entries []*ZSetEntry `json:"entries"`
}

// GetType returns redis object type
func (o *ZSetObject) GetType() string {
	return ZSetType
}

// GetElemCount returns number of elements in list/set/hash/zset
func (o *ZSetObject) GetElemCount() int {
	return len(o.Entries)
}

// AuxObject stores redis metadata
//
// 例如 Key="redis-ver"、Value="7.0.0"。只有在 Decoder 开启 WithSpecialOpCode 时才会回调出来。
type AuxObject struct {
	*BaseObject
	Value string
}

// GetType returns redis object type
func (o *AuxObject) GetType() string {
	return AuxType
}

// MarshalJSON marshal []byte as string
func (o *AuxObject) MarshalJSON() ([]byte, error) {
	o2 := struct {
		*BaseObject
		Value string `json:"value"`
	}{
		BaseObject: o.BaseObject,
		Value:      string(o.Value),
	}
	return json.Marshal(o2)
}

// DBSizeObject stores db size metadata
//
// 对应 RDB 的 RESIZEDB 操作码：告知该 db 的 key 总数与带过期时间的 key 数（仅提示信息）。
// 仅在 Decoder 开启 WithSpecialOpCode 时回调。
type DBSizeObject struct {
	*BaseObject
	KeyCount uint64
	TTLCount uint64
}

// GetType returns redis object type
func (o *DBSizeObject) GetType() string {
	return DBSizeType
}

// ModuleTypeObject stores a module type object parsed by custom handler
//
// Redis Module（如 RedisJSON）写入的数据格式由模块自定义，rdb 库无法内置解析，
// 需要调用方通过 Decoder.WithSpecialType 注册 handler，解析结果就放在 Value 里。
type ModuleTypeObject struct {
	*BaseObject
	ModuleType string
	Value      interface{}
}

// GetType returns module type name
func (o *ModuleTypeObject) GetType() string {
	return o.ModuleType
}

// MarshalJSON marshal []byte as string
func (o *ModuleTypeObject) MarshalJSON() ([]byte, error) {
	o2 := struct {
		*BaseObject
		ModuleType string      `json:"moduleType"`
		Value      interface{} `json:"value"`
	}{
		BaseObject: o.BaseObject,
		ModuleType: o.ModuleType,
		Value:      o.Value,
	}
	return json.Marshal(o2)
}
