package model

import "strconv"

// StreamObject 表示一个 Redis Stream（流）对象。
//
// Stream 是 Redis 5.0 引入的「追加日志」型数据结构，常用于消息队列。
// 它在底层是「基数树(radix tree) + listpack」的混合结构，
// 但解析后这里把它摊平成 Entries/Groups 这种便于使用的形式。
//
// 版本说明：RDB 中 stream 有多种版本（LISTPACKS / LISTPACKS_2 / LISTPACKS_3），
// Version 字段表示解析到的具体版本，部分字段只在 V2 及以上有效。
type StreamObject struct {
	*BaseObject
	// Version 2 means is RDB_TYPE_STREAM_LISTPACKS_2, 3 means is RDB_TYPE_STREAM_LISTPACKS_3
	Version uint `json:"version,omitempty"`
	// Entries stores messages in stream
	Entries []*StreamEntry `json:"entries,omitempty"`
	// Groups is consumer groups of stream
	Groups []*StreamGroup `json:"groups,omitempty"`
	// Length is current number of messages inside this stream
	Length uint64 `json:"len"`
	// LastId is the ID of last entry in stream
	LastId *StreamId `json:"lastId"`
	// FirstId is the ID of first entry in stream. only valid in V2
	FirstId *StreamId `json:"firstId,omitempty"`
	// MaxDeletedId is the maximal ID that was deleted in stream. only valid in V2
	MaxDeletedId *StreamId `json:"maxDeletedId,omitempty"`
	// AddedEntriesCount is count of elements added in all time. only valid in V2
	AddedEntriesCount uint64 `json:"addedEntriesCount,omitempty"`
}

// GetType 返回该对象的 Redis 逻辑类型（固定为 "stream"）。
func (obj *StreamObject) GetType() string {
	return StreamType
}

// StreamEntry 是 stream 底层基数树中的一个「节点」，每个节点是一个 listpack，
// 里面压缩存放了若干条消息。使用时通常不需要关心「某条消息属于哪个节点」，
// 直接遍历 Msgs 即可。
type StreamEntry struct {
	FirstMsgId *StreamId        `json:"firstMsgId"` // 该节点内第一条消息的 ID
	Fields     []string         `json:"fields"`     // master entry 的字段名（压缩用：多条消息共享字段名）
	Msgs       []*StreamMessage `json:"msgs"`       // 该节点包含的消息
}

// StreamMessage 是 stream 中的一条消息。
type StreamMessage struct {
	Id      *StreamId         `json:"id"`      // 消息 ID（毫秒时间戳-序号）
	Fields  map[string]string `json:"fields"`  // 消息体：字段名 → 值
	Deleted bool              `json:"deleted"` // 是否已被删除（stream 中删除会留墓碑）
}

// StreamId 是 stream 的消息 ID：一个 128 位数，由「毫秒时间戳」+「序号」组成。
// 形如 "1704557973866-0"，保证同一毫秒内也能严格递增且唯一。
type StreamId struct {
	Ms       uint64 `json:"ms"`       // 毫秒时间戳部分
	Sequence uint64 `json:"sequence"` // 同一毫秒内的序号
}

// MarshalText 实现 encoding.TextMarshaler，把 ID 序列化成 "ms-sequence" 的字符串形式
// （如 "1704557973866-0"），这样 JSON 输出时就是可读字符串而非嵌套对象。
func (id *StreamId) MarshalText() (text []byte, err error) {
	txt := strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
	return []byte(txt), nil
}

// StreamGroup 是 stream 的「消费者组」，用于多消费者协同消费。
type StreamGroup struct {
	Name      string            `json:"name"`                // 组名
	LastId    *StreamId         `json:"lastId"`              // 该组最后投递的消息 ID
	Pending   []*StreamNAck     `json:"pending,omitempty"`   // 已投递但未确认(ACK)的消息
	Consumers []*StreamConsumer `json:"consumers,omitempty"` // 组内消费者
	// EntriesRead is only valid in V2. The following comments are from redis to illustrate its use
	/*In a perfect world (CG starts at 0-0, no dels, no XGROUP SETID, ...), this is the total number of
	group reads. In the real world, the reasoning behind this value is detailed at the top comment of
	streamEstimateDistanceFromFirstEverEntry(). */
	EntriesRead uint64 `json:"entriesRead,omitempty"` // 该组累计读取过的消息数（仅 V2 有效）
}

// StreamNAck 表示一条「已投递但尚未 ACK」的待确认消息（PENDING 列表项）。
type StreamNAck struct {
	Id            *StreamId `json:"id"`            // 消息 ID
	DeliveryTime  uint64    `json:"deliveryTime"`  // 最后一次投递时间（毫秒时间戳）
	DeliveryCount uint64    `json:"deliveryCount"` // 投递次数（重试次数）
}

// StreamConsumer 是消费者组中的一个消费者。
type StreamConsumer struct {
	Name       string      `json:"name"`              // 消费者名
	SeenTime   uint64      `json:"seenTime"`          // 最后一次被观察到的活跃时间
	Pending    []*StreamId `json:"pending,omitempty"` // 该消费者持有的待确认消息 ID 列表
	ActiveTime uint64      `json:"activeTime"`        // 最后活跃时间；rdb11 起才有，见 redis#11099
}
