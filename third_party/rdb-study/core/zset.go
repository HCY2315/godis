// zset.go 负责解析与生成 Redis 的 ZSet（有序集合）类型。
//
// 对应 RDB 类型常量（见 decoder.go）：
//   - typeZset         (3)  score 以「字符串形式的浮点数」存储（旧格式）
//   - typeZset2        (5)  score 以 8 字节 IEEE-754 二进制双精度存储（Redis 4+）
//   - typeZsetZipList  (12) ziplist：member/score 字符串成对排列
//   - typeZsetListPack (17) listpack：member/score 字符串成对排列（Redis 7+）
//
// 区别要点：typeZset 的 score 是变长字符串（还能表示 +inf/-inf/nan 的特殊标记），
// 而 typeZset2 直接写 8 字节 little-endian double，解析更快、更精确。
//
// 使用技巧：紧凑编码（ziplist/listpack）里的 score 仍是字符串，必须用 ParseFloat
// 转换；如果 score 文本格式非法，解析会直接失败。
package core

import (
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// readZSet 解析普通 ZSet（typeZset 或 typeZset2）对象。
//
// 参数 zset2 决定 score 的存储格式：
//   - true ：typeZset2，score 为 8 字节小端 IEEE-754 double（readFloat）
//   - false：typeZset，score 为带 1 字节长度的字符串；0xFF/0xFE/0xFD 分别表示
//     -inf/+inf/NaN（readLiteralFloat）
//
// 字节布局：[长度编码 N][member #0][score #0]...[member #(N-1)][score #(N-1)]
//
// 返回值：按文件顺序排列的 ZSetEntry 列表；score 的顺序不保证与 Redis 内部跳表顺序一致。
func (dec *Decoder) readZSet(zset2 bool) ([]*model.ZSetEntry, error) {
	// 成员数量
	length, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	entries := make([]*model.ZSetEntry, 0, int(length))
	for i := uint64(0); i < length; i++ {
		// member 始终是 RDB string
		member, err := dec.readString()
		if err != nil {
			return nil, err
		}
		var score float64
		if zset2 {
			// 新格式：直接 8 字节二进制 double
			score, err = dec.readFloat()
		} else {
			// 旧格式：浮点数的字符串表示（可能带 inf/nan 特殊标记）
			score, err = dec.readLiteralFloat()
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	return entries, nil
}

// readZipListZSet 解析 typeZsetZipList（RDB type 12）对象。
//
// 整个 ZSet 被编码成一个 ziplist，内部按 [member, score, member, score, ...] 交替存放；
// score 在 ziplist 里是十进制字符串（不是二进制 double），因此需要 ParseFloat。
// zllen 是 entry 总数，等于成员数的 2 倍，循环步长为 2。
//
// 返回值：成员/分值列表 + 记录原始 ziplist 字节数的 detail。
func (dec *Decoder) readZipListZSet() ([]*model.ZSetEntry, *model.ZiplistDetail, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// 解析 10 字节 ziplist 头，得到 entry 总数并跳到第一个 entry
	size := readZipListLength(buf, &cursor)
	entries := make([]*model.ZSetEntry, 0, size)
	for i := 0; i < size; i += 2 {
		member, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		// 紧凑编码中的 score 是字符串，需要显式转换
		scoreLiteral, err := dec.readZipListEntry(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		score, err := strconv.ParseFloat(unsafeBytes2Str(scoreLiteral), 64)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	detail := &model.ZiplistDetail{
		RawStringSize: len(buf),
	}
	return entries, detail, nil
}

// readListPackZSet 解析 typeZsetListPack（RDB type 17）对象。
//
// 结构与 ziplist 版本一致：整个 ZSet 是一个 listpack 字符串，
// 内部按 [member, score, member, score, ...] 交替存放，步长为 2。
// 区别只在头部和 entry 编码格式；score 同样以字符串形式保存。
func (dec *Decoder) readListPackZSet() ([]*model.ZSetEntry, *model.ListpackDetail, error) {
	buf, err := dec.readString()
	if err != nil {
		return nil, nil, err
	}
	cursor := 0
	// 解析 listpack 6 字节头，得到 entry 总数并跳到第一个 entry
	size := readListPackLength(buf, &cursor)
	entries := make([]*model.ZSetEntry, 0, size)
	for i := 0; i < size; i += 2 {
		// 整数 entry 会被格式化成字符串，因此这里可以统一按 string 处理
		member, err := dec.readListPackEntryAsString(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		scoreLiteral, err := dec.readListPackEntryAsString(buf, &cursor)
		if err != nil {
			return nil, nil, err
		}
		score, err := strconv.ParseFloat(unsafeBytes2Str(scoreLiteral), 64)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, &model.ZSetEntry{
			Member: unsafeBytes2Str(member),
			Score:  score,
		})
	}
	detail := &model.ListpackDetail{
		RawStringSize: len(buf),
	}
	return entries, detail, nil
}

// WriteZSetObject 是编码器写 ZSet 对象的统一入口。
//
// 策略与 Redis 的 zset-max-ziplist-* 配置一致：
//  1. 成员数 <= maxEntries 且所有 member 长度 <= maxValue 时，用 ziplist 编码；
//  2. 否则用 typeZset2：member 字符串 + 8 字节二进制 double。
//
// 注意：编码器总是选择 typeZset2（而不是旧的 typeZset），因为二进制 double
// 不会丢失精度，也不需要额外的文本格式化/解析。
func (enc *Encoder) WriteZSetObject(key string, entries []*model.ZSetEntry, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteZipListZSet(key, entries)
	if err != nil {
		return err
	}
	if !ok {
		// 不满足 ziplist 条件，改用二进制 double 编码
		err = enc.writeZSet2Encoding(key, entries)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

// writeZSet2Encoding 用 typeZset2 编码写 ZSet。
//
// 字节布局：
//
//	[typeZset2][key][长度编码 N][member #0][score 8字节][member #1][score 8字节]...
//
// score 由 writeFloat64 写成小端 IEEE-754 双精度，解析时用 readFloat 对称还原。
func (enc *Encoder) writeZSet2Encoding(key string, entries []*model.ZSetEntry) error {
	err := enc.write([]byte{typeZset2})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	// 成员数量，解析方据此循环
	err = enc.writeLength(uint64(len(entries)))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err = enc.writeString(entry.Member)
		if err != nil {
			return err
		}
		err = enc.writeFloat64(entry.Score)
		if err != nil {
			return err
		}
	}
	return nil
}

// tryWriteZipListZSet 尝试用 ziplist 编码写 ZSet。
//
// 前置条件：成员数不超过 maxEntries，且每个 member 长度不超过 maxValue。
// 注意 score 不参与长度判断，因为它在 ziplist 中总是以字符串形式写入。
//
// (ok, err) 语义：false 表示尚未写字节、可安全回退；true 表示已开始写、只能中止。
func (enc *Encoder) tryWriteZipListZSet(key string, entries []*model.ZSetEntry) (bool, error) {
	if len(entries) > enc.zsetZipListOpt.getMaxEntries() {
		return false, nil
	}
	maxValue := enc.zsetZipListOpt.getMaxValue()
	for _, entry := range entries {
		if len(entry.Member) > maxValue {
			return false, nil
		}
	}
	err := enc.write([]byte{typeZsetZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	// ziplist 内 member/score 交替：展开成 [m0, s0, m1, s1, ...]
	zlElements := make([]string, len(entries)*2)
	for i, entry := range entries {
		// 'f' 表示不用指数形式，-1 表示保留能精确表示该值的最少小数位；
		// 解析侧用 ParseFloat 读取，因此格式只需保证可逆。
		scoreStr := strconv.FormatFloat(entry.Score, 'f', -1, 64)
		zlElements[i*2] = entry.Member
		zlElements[i*2+1] = scoreStr
	}
	err = enc.writeZipList(zlElements)
	if err != nil {
		return true, err
	}
	return true, nil
}

// 使用技巧：
//  1. 解析旧 RDB 时会遇到 typeZset，其 score 是文本且可能为 inf/-inf/nan，
//     不要假设 score 一定是有限数；
//  2. typeZset2 与紧凑编码都可能出现在同一个 RDB 中，解码器会按 type 常量自动分派；
//  3. 需要做精确的 score 比较时，优先相信 typeZset2 的二进制值，文本格式可能被截断。
