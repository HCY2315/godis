// Package parser is interface for parser
//
// Package parser 是解析侧的「门面包」（facade）：它不实现具体解析逻辑，
// 而是把 core 包的解码器和 model 包的数据类型以别名/常量转发的形式重新导出。
// 这样使用方只需 import 本包（parser），就能拿到 NewDecoder、RedisObject
// 以及所有对象类型和类型常量，而不必同时依赖 core 与 model 的内部路径。
//
// 典型用法：
//
//	decoder := parser.NewDecoder(rdbFile)
//	err := decoder.Parse(func(o parser.RedisObject) bool {
//		if str, ok := o.(*parser.StringObject); ok {
//			println(str.Key, string(str.Value))
//		}
//		return true
//	})
//
// 由于这些导出项都是别名，它们与 core/model 中的定义完全等价，
// 升级底层实现时本包可以保持 API 稳定。
package parser

import (
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

const (
	// StringType is redis string
	// StringType 是 Redis string 类型标识，等价于 model.StringType。
	StringType = model.StringType
	// ListType is redis list
	// ListType 是 Redis list 类型标识，等价于 model.ListType。
	ListType = model.ListType
	// SetType is redis set
	// SetType 是 Redis set 类型标识，等价于 model.SetType。
	SetType = model.SetType
	// HashType is redis hash
	// HashType 是 Redis hash 类型标识，等价于 model.HashType。
	HashType = model.HashType
	// ZSetType is redis sorted set
	// ZSetType 是 Redis sorted set 类型标识，等价于 model.ZSetType。
	ZSetType = model.ZSetType
	// AuxType is redis metadata key-value pair
	// AuxType 是 RDB 辅助元数据（aux field）的类型标识；默认不交给回调，
	// 需要 Decoder.WithSpecialOpCode() 才会返回 AuxObject。
	AuxType = model.AuxType
	// DBSizeType is for RDB_OPCODE_RESIZEDB
	// DBSizeType 对应 RDB_OPCODE_RESIZEDB（db 大小提示）的类型标识。
	DBSizeType = model.DBSizeType
	// StreamType is for redis stream
	// StreamType 是 Redis stream 类型标识。
	StreamType = model.StreamType
)

type (
	// RedisObject is interface for a redis object
	// RedisObject 是所有解析结果的公共接口：提供 GetType/GetKey/GetDBIndex/
	// GetExpiration/GetSize 等方法，具体对象再通过类型断言获取字段。
	RedisObject = model.RedisObject
	// StringObject stores a string object
	// StringObject 对应 string 类型，字段为 Key 与 Value([]byte)。
	StringObject = model.StringObject
	// ListObject stores a list object
	// ListObject 对应 list 类型，字段 Values 按顺序保存元素。
	ListObject = model.ListObject
	// SetObject stores a set object
	// SetObject 对应 set 类型，字段 Members 保存所有成员。
	SetObject = model.SetObject
	// HashObject stores a hash object
	// HashObject 对应 hash 类型，字段 Hash 是 field -> value 的映射。
	HashObject = model.HashObject
	// ZSetObject stores a sorted set object
	// ZSetObject 对应 zset 类型，字段 Entries 保存 member 与 score。
	ZSetObject = model.ZSetObject
	// StreamObject stores a redis stream object
	// StreamObject 对应 stream 类型，包含消息条目与消费者组信息。
	StreamObject = model.StreamObject
	// AuxObject stores redis metadata
	// AuxObject 对应 aux 元数据（如 redis-ver、redis-bits）。
	AuxObject = model.AuxObject
	// DBSizeObject stores db size metadata
	// DBSizeObject 对应 RESIZEDB 记录（key 数量与带 TTL 的 key 数量）。
	DBSizeObject = model.DBSizeObject
)

var (
	// NewDecoder creates a new RDB decoder
	// NewDecoder 是 core.NewDecoder 的函数变量转发：接收任意 io.Reader，
	// 返回 *core.Decoder。返回的 decoder 支持链式调用
	// WithSpecialOpCode()/WithSpecialType() 等配置，然后调用 Parse 遍历对象。
	//
	// 使用技巧：
	//   - 这里用 var 而不是 func 包装，是为了让导出项与 core 的实现保持同一签名，
	//     调用方式与 core.NewDecoder 完全一致；
	//   - 通过本包获取的 decoder 具体类型仍是 *core.Decoder，因此 core 上的
	//     所有链式方法依然可用。
	NewDecoder = core.NewDecoder
)
