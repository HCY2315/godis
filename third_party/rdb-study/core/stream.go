// stream.go 负责解析 Redis Stream 在 RDB 中的紧凑存储格式。
//
// 版本与使用场景（对应 decoder.go 中的 RDB 类型）：
//   - typeStreamListPacks   (15)：Redis 5.0 引入，version=1
//   - typeStreamListPacks2  (19)：Redis 7.0 引入，version=2，额外保存 first-id、
//     max-deleted-entry-id、entries-added
//   - typeStreamListPacks3  (21)：Redis 7.2 引入，version=3，消费者额外保存 active-time
//
// 一个 Stream 在 RDB 中的逻辑结构：
//
//	[num-listpacks]
//	  [16 字节 radix-tree key][listpack value]   × num-listpacks
//	[stream-length][last-id]
//	version >= 2: [first-id][max-deleted-id][entries-added]
//	[group-count] { ... 消费者组 ... }
//
// 其中 radix-tree key 和 message id 都由 8 字节毫秒时间戳 + 8 字节序号组成，
// 但字节序/编码方式不同：
//   - radix-tree key 在 RDB 中按「大端」直接写固定 16 字节；
//   - 消费者组里的 pending id 也按「大端」写固定 16 字节；
//   - listpack 内部的消息 id 则存成相对 master entry 的「有符号差值」；
//   - 单独的 last-id/first-id 等字段使用 RDB 通用长度编码。
//
// 每个 listpack 对应 radix tree 的一个节点，内部布局为：
//
//	master entry: [count][deleted][num-fields][field-name ...][lp-count]
//	消息 0..N    : [flags][ms-diff][seq-diff]
//	               [num-fields?][field-name/value ...][lp-count]
//
//	master entry 用来压缩字段名：当多条消息的字段集合相同（SAMEFIELDS）时，
//	消息里只需存 value，字段名直接复用 master entry 中的列表。
//	count 是仍有效的消息数，deleted 是已删除但尚未从 listpack 清理的消息数，
//	因此实际遍历条数是 count + deleted；删除的消息通过 flags 的 deleted 位标记。
//
// 消息 id 压缩方式：
//   - master entry 的第一条消息 id 存在 radix-tree key 中；
//   - 后续消息在 listpack 里存 ms/seq 相对 first-id 的差值（可能为负，
//     用有符号整数编码），解析时再加回 first-id 得到完整 id。
//
// 使用技巧：
//   - entries 是「物理分页」视角：一个 StreamEntry 对应一个 listpack，里面可能有多条
//     消息；日常使用若只关心消息本身，可以把所有 entry.Msgs 展平。
//   - deleted 消息仍会出现在 entries 中（Deleted=true），做业务统计时要自行过滤。
package core

import (
	"encoding/binary"
	"fmt"

	"github.com/hdt3213/rdb/model"
)

const (
	// StreamItemFlagNone means No special flags.
	StreamItemFlagNone = 0
	// StreamItemFlagDeleted means entry was deleted
	StreamItemFlagDeleted = 1 << 0
	// StreamItemFlagSameFields means entry has the same fields as the master entry
	StreamItemFlagSameFields = 1 << 1
)

// readStreamListPacks 解析一个 Stream 对象的主体（不含 BaseObject 元信息）。
//
// 读取顺序严格对应 RDB 中 Stream 的字段：
//  1. num-listpacks + 每个 listpack（readStreamEntries）
//  2. stream 当前有效消息数 len
//  3. last-id
//  4. version >= 2 时追加 first-id、max-deleted-id、entries-added
//  5. 消费者组（readStreamGroups）
//
// version 由 decoder.go 根据 RDB 类型（15/19/21）换算成 1/2/3 后传入，
// 不同版本只影响「后面还有哪些字段」，不影响已读取部分的布局。
func (dec *Decoder) readStreamListPacks(version uint) (*model.StreamObject, error) {
	// 先读取所有 listpack 节点及其消息
	entries, err := dec.readStreamEntries()
	if err != nil {
		return nil, err
	}
	// stream 长度使用 RDB 通用长度编码（6/14/32/64 位）
	steamLen, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	// last-id 由 ms、seq 两段长度编码组成
	lastId, err := dec.readStreamId()
	if err != nil {
		return nil, err
	}
	stream := &model.StreamObject{
		Entries: entries,
		Length:  steamLen,
		LastId:  lastId,
	}

	// version 2 起 RDB 额外保存三个统计字段，version 1 没有
	if version >= 2 {
		// first-id：Stream 中现存最老消息的 id
		firstId, err := dec.readStreamId()
		if err != nil {
			return nil, err
		}
		stream.FirstId = firstId
		// max-deleted-id：被删除过的最大消息 id，用于判断消息是否曾存在
		maxDeletedId, err := dec.readStreamId()
		if err != nil {
			return nil, err
		}
		stream.MaxDeletedId = maxDeletedId
		// entries-added：Stream 生命周期内累计写入的消息数（含已删除）
		addedCount, _, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		stream.AddedEntriesCount = addedCount
	}
	// 消费者组可能不存在（count 为 0），但仍必须读取这个计数
	groups, err := dec.readStreamGroups(version)
	if err != nil {
		return nil, err
	}
	stream.Groups = groups
	stream.Version = version
	return stream, nil
}

// readStreamId 读取一个 Stream 消息 id（毫秒时间戳 + 序号）。
//
// 这里的两段都使用 RDB 通用长度编码，而不是固定 8 字节；长度编码本身可表示
// 0~2^64-1，所以能覆盖完整 id 范围。注意与 radix-tree key / pending id 区分：
// 后者在 RDB 中是固定 8+8 字节大端。
func (dec *Decoder) readStreamId() (*model.StreamId, error) {
	// 高 64 位：毫秒时间戳
	ms, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	// 低 64 位：同一毫秒内的序号
	seq, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	return &model.StreamId{
		Ms:       ms,
		Sequence: seq,
	}, nil
}

// readStreamEntries read entries
//
// readStreamEntries 读取 Stream 的所有 listpack 节点，并解析成 StreamEntry 列表。
//
// 每个节点的存储形式：
//
//		[RDB string: 16 字节 key][RDB string: listpack]
//
//	  - key = 该节点第一条消息的 id，按 8 字节 ms + 8 字节 seq「大端」存储；
//	  - value = 一个完整 listpack，内部按 master entry + 多条消息组织。
//
// 注意：listpack 头 6 字节（4 字节 total-bytes + 2 字节 num-elements）在这里被
// 手动跳过，然后交给 readStreamEntryContent 从 master entry 开始解析。
func (dec *Decoder) readStreamEntries() ([]*model.StreamEntry, error) {
	// listpack 节点个数
	length, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	var result []*model.StreamEntry
	for i := uint64(0); i < length; i++ {
		// 节点 key：固定 16 字节 = 8 字节 ms + 8 字节 seq，大端
		header, err := dec.readString()
		if err != nil {
			return nil, err
		}
		cursor := 0
		// 前 8 字节：master entry 第一条消息的毫秒时间戳
		msBin, err := readBytes(header, &cursor, 8)
		if err != nil {
			return nil, err
		}
		ms := binary.BigEndian.Uint64(msBin)
		// 后 8 字节：第一条消息的序号
		seqBin, err := readBytes(header, &cursor, 8)
		if err != nil {
			return nil, err
		}
		seq := binary.BigEndian.Uint64(seqBin)
		firstId := &model.StreamId{
			Ms:       ms,
			Sequence: seq,
		}

		// 节点 value：整块 listpack 字节
		buf, err := dec.readString()
		if err != nil {
			return nil, err
		}
		cursor = 0
		// skip 4Byte total-bytes + 2Byte num-elements
		// 头部信息对逐 entry 解析没有用，直接跳过；后续从 master entry 开始
		_, _ = readBytes(buf, &cursor, 6)
		entry, err := dec.readStreamEntryContent(buf, &cursor, firstId)
		if err != nil {
			return nil, err
		}
		// FirstMsgId 即该 listpack 的第一条消息 id（也就是 radix-tree key）
		entry.FirstMsgId = firstId
		result = append(result, entry)
	}
	return result, nil
}

// readStreamEntryContent read messages in a stream entry
//
// readStreamEntryContent 解析一个 listpack 内的 master entry 和所有消息。
//
// listpack 内部逻辑布局：
//
//		[count][deleted][num-fields][field-name ...][lp-count]      ← master entry
//		[flags][ms-diff][seq-diff][num-fields?][field/value ...][lp-count] × (count+deleted)
//
//	  - count     : 当前有效消息数
//	  - deleted   : 已删除但仍留在 listpack 中的消息数
//	  - num-fields: master entry 保存的字段名个数
//	  - lp-count  : 该逻辑单元占用的 listpack entry 个数，Redis 反向遍历时用于定位；
//	                本解析器只读取并丢弃（原错误文案叫 fields end flag）
//
// 每条消息：
//   - flags 的 bit0 = deleted，bit1 = same-fields；
//   - ms-diff / seq-diff 是相对 firstId 的「有符号差值」，可能为负；
//   - same-fields 为 1 时不再重复字段名，直接按 master 字段顺序只读 value；
//     否则先读字段个数，再按 field-name/value 交替读取。
func (dec *Decoder) readStreamEntryContent(buf []byte, cursor *int, firstId *model.StreamId) (*model.StreamEntry, error) {
	// read count
	// master entry 第 1 项：有效消息数
	count, err := dec.readListPackEntryAsInt(buf, cursor)
	if err != nil {
		return nil, fmt.Errorf("read stream entry count failed: %v", err)
	}
	// master entry 第 2 项：已删除消息数（仍未从 listpack 移除）
	deleted, err := dec.readListPackEntryAsInt(buf, cursor)
	if err != nil {
		return nil, fmt.Errorf("read stream entry deleted count failed: %v", err)
	}

	// read field names of master entry
	// master entry 第 3 项：字段名个数，随后是逐个字段名
	fieldNum0, err := dec.readListPackEntryAsInt(buf, cursor)
	if err != nil {
		return nil, fmt.Errorf("read stream field number failed: %v", err)
	}
	masterFieldNum := int(fieldNum0)
	masterFieldNames := make([]string, masterFieldNum)
	for i := 0; i < masterFieldNum; i++ {
		// 字段名以 listpack entry 形式存储，可能是字符串或整数编码
		name, err := dec.readListPackEntryAsString(buf, cursor)
		if err != nil {
			return nil, fmt.Errorf("read field name of stream entry failed: %v", err)
		}
		masterFieldNames[i] = string(name)
	}
	// read lp count of master entry
	// master entry 最后一项：本逻辑单元的 listpack entry 计数（这里丢弃）
	if _, err = dec.readListPackEntryAsString(buf, cursor); err != nil {
		return nil, fmt.Errorf("read fields end flag failed: %v", err)
	}

	// 有效 + 已删除 = listpack 中实际存在的消息槽位数
	total := count + deleted
	msgs := make([]*model.StreamMessage, 0, total)
	for i := int64(0); i < total; i++ {
		// flags：bit0=deleted，bit1=same-fields
		flag, err := dec.readListPackEntryAsInt(buf, cursor)
		if err != nil {
			return nil, fmt.Errorf("read stream item flag failed: %v", err)
		}
		// 相对 firstId 的毫秒差值；listpack 用有符号整数存储，可能为负
		ms, err := dec.readListPackEntryAsInt(buf, cursor)
		if err != nil {
			return nil, fmt.Errorf("read stream item id ms failed: %v", err)
		}
		// 相对 firstId 的序号差值，同样可能为负
		seq, err := dec.readListPackEntryAsInt(buf, cursor)
		if err != nil {
			return nil, fmt.Errorf("read stream item id seq failed: %v", err)
		}
		// ms and seq may be negative
		// 加回 firstId 得到绝对 id；差值是有符号的，所以先转 int64 再相加，
		// 最后转回 uint64
		msgId := &model.StreamId{
			Ms:       uint64(ms + int64(firstId.Ms)),
			Sequence: uint64(seq + int64(firstId.Sequence)),
		}
		fieldNum := masterFieldNum
		// 没有 same-fields 标记时，消息自带字段个数，覆盖 master 的字段数
		if flag&StreamItemFlagSameFields == 0 {
			fieldNum0, err := dec.readListPackEntryAsInt(buf, cursor)
			if err != nil {
				return nil, fmt.Errorf("read stream item field number failed: %v", err)
			}
			fieldNum = int(fieldNum0)
		}
		// 容量按 masterFieldNum 预估；实际字段数可能不同，仅影响扩容次数
		msg := &model.StreamMessage{
			Id:      msgId,
			Fields:  make(map[string]string, masterFieldNum),
			Deleted: flag&StreamItemFlagDeleted > 0,
		}

		for i := 0; i < fieldNum; i++ {
			var fieldName string
			if flag&StreamItemFlagSameFields > 0 {
				// 复用 master entry 中同位置的字段名
				fieldName = masterFieldNames[i]
			} else {
				// 字段名随消息单独存储
				fieldNameBin, err := dec.readListPackEntryAsString(buf, cursor)
				if err != nil {
					return nil, fmt.Errorf("read stream item field name failed: %v", err)
				}
				fieldName = unsafeBytes2Str(fieldNameBin)
			}
			// 字段值总是随消息存储
			fieldValue, err := dec.readListPackEntryAsString(buf, cursor)
			if err != nil {
				return nil, fmt.Errorf("read stream item field value failed: %v", err)
			}
			msg.Fields[fieldName] = unsafeBytes2Str(fieldValue)
		}
		// read lp count
		// 每条消息末尾的 lp-count，读取后丢弃
		if _, err = dec.readListPackEntryAsString(buf, cursor); err != nil {
			return nil, fmt.Errorf("read fields end flag failed: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return &model.StreamEntry{
		Fields: masterFieldNames,
		Msgs:   msgs,
	}, nil
}

// readStreamGroups 读取 Stream 的消费者组（consumer groups）以及组内消费者。
//
// 布局：
//
//	[group-count]
//	每组:
//	  [name(RDB string)][last-id]
//	  version >= 2: [entries-read]
//	  [pending-count] { [16 字节 id: 8 ms + 8 seq 大端][8 字节 delivery-time 小端][delivery-count] }
//	  [consumer-count] {
//	      [name(RDB string)][8 字节 seen-time 小端]
//	      version >= 3: [8 字节 active-time 小端]
//	      [consumer-pending-count] { [16 字节 id: 8 ms + 8 seq 大端] }
//	  }
//
// 说明：
//   - pending（PEL，Pending Entries List）记录「已投递但尚未 ACK」的消息，
//     delivery-count 表示重投递次数，delivery-time 是最后一次投递时间；
//   - consumer 的 pending 只保存 id，具体投递信息在组的 pending 列表里；
//   - version < 3 没有 active-time 字段，这里用 seen-time 作为兼容值。
func (dec *Decoder) readStreamGroups(version uint) ([]*model.StreamGroup, error) {
	// 消费者组个数
	groupCount, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	groups := make([]*model.StreamGroup, 0, int(groupCount))
	for i := uint64(0); i < groupCount; i++ {
		// 组名；注意这里忽略了 readString 的 err 变量（沿用既有逻辑）
		name, _ := dec.readString()
		if err != nil {
			return nil, err
		}
		// last-delivered-id：使用长度编码的两段 id
		lastId, err := dec.readStreamId()
		if err != nil {
			return nil, err
		}

		var entriesRead uint64
		// version 2 起才有 entries-read 统计
		if version >= 2 {
			entriesRead, _, err = dec.readLength()
			if err != nil {
				return nil, err
			}
		}

		// read pending list
		// 组的 PEL：固定 16 字节大端 id + 8 字节小端投递时间 + 长度编码的投递次数
		pendingCount, _, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		pending := make([]*model.StreamNAck, 0, int(pendingCount))
		for j := uint64(0); j < pendingCount; j++ {
			// 前 8 字节：ms，大端
			if err := dec.readFull(dec.buffer); err != nil {
				return nil, err
			}
			ms := binary.BigEndian.Uint64(dec.buffer)
			// 后 8 字节：seq，大端
			if err := dec.readFull(dec.buffer); err != nil {
				return nil, err
			}
			seq := binary.BigEndian.Uint64(dec.buffer)
			streamId := &model.StreamId{
				Ms:       ms,
				Sequence: seq,
			}
			// 8 字节投递时间：小端（毫秒时间戳）
			if err := dec.readFull(dec.buffer); err != nil {
				return nil, err
			}
			deliveryTime := binary.LittleEndian.Uint64(dec.buffer)
			// 投递次数：RDB 长度编码
			deliveryCount, _, err := dec.readLength()
			if err != nil {
				return nil, err
			}
			pending = append(pending, &model.StreamNAck{
				Id:            streamId,
				DeliveryTime:  deliveryTime,
				DeliveryCount: deliveryCount,
			})
		}

		// read consumers
		// 组内消费者个数
		consumerCount, _, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		consumers := make([]*model.StreamConsumer, 0, int(consumerCount))
		for j := uint64(0); j < consumerCount; j++ {
			consumerName, err := dec.readString()
			if err != nil {
				return nil, err
			}
			// 8 字节 seen-time，小端
			if err := dec.readFull(dec.buffer); err != nil {
				return nil, err
			}
			seenTime := binary.LittleEndian.Uint64(dec.buffer)
			activeTime := seenTime
			// version 3 起新增 active-time；旧版本用 seen-time 兜底
			if version >= 3 {
				if err := dec.readFull(dec.buffer); err != nil {
					return nil, err
				}
				activeTime = binary.LittleEndian.Uint64(dec.buffer)
			}
			// 该消费者名下的 pending 消息个数
			consumerPendingCount, _, err := dec.readLength()
			if err != nil {
				return nil, err
			}
			consumerPending := make([]*model.StreamId, 0, int(consumerPendingCount))
			for k := uint64(0); k < consumerPendingCount; k++ {
				// 消费者 pending 只存 id：8 字节 ms + 8 字节 seq，均为大端
				if err := dec.readFull(dec.buffer); err != nil {
					return nil, err
				}
				ms := binary.BigEndian.Uint64(dec.buffer)
				if err := dec.readFull(dec.buffer); err != nil {
					return nil, err
				}
				seq := binary.BigEndian.Uint64(dec.buffer)
				consumerPending = append(consumerPending, &model.StreamId{
					Ms:       ms,
					Sequence: seq,
				})
			}
			consumers = append(consumers, &model.StreamConsumer{
				SeenTime:   seenTime,
				Name:       unsafeBytes2Str(consumerName),
				Pending:    consumerPending,
				ActiveTime: activeTime,
			})
		}
		groups = append(groups, &model.StreamGroup{
			Name:        unsafeBytes2Str(name),
			LastId:      lastId,
			Pending:     pending,
			Consumers:   consumers,
			EntriesRead: entriesRead,
		})
	}
	return groups, nil
}
