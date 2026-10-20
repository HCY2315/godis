// list.go 负责解析与生成 Redis 的 List（列表）类型。
//
// 对应 RDB 类型常量（见 decoder.go）：
//   - typeList           (1)  普通列表：先用长度前缀给出元素个数，再逐个读取字符串元素
//   - typeListZipList    (10) 紧凑列表：整个列表被编码成一个 ziplist 字符串
//   - typeListQuickList  (14) Quicklist：长度前缀 + 多个 ziplist 节点
//   - typeListQuickList2 (18) Quicklist v2：长度前缀 + 多个节点，每个节点可以是
//     plain（单个普通字符串）或 packed（listpack 承载多个元素）
//
// 文件顶部这组常量描述的是 ziplist 单个 entry 的编码标记。虽然它们定义在 list.go，
// 但实际被 ziplist.go 的 readZipListEntry/encodeZipListEntry 使用，因此这里统一解释。
//
// 使用技巧：解析侧 readQuickList/readQuickList2 会把各节点拼接成扁平的元素切片返回，
// 同时把「节点级」信息放进 detail，便于做内存分析时还原 Redis 的真实分页结构。
package core

import (
	"encoding/binary"
	"errors"
	"github.com/hdt3213/rdb/model"
)

const (
	// ziplist entry 头部高 2 位 = 00：字符串，长度直接放在低 6 位（0~63）
	zipStr06B = 0
	// 头部高 2 位 = 01：字符串，长度 = 低 6 位<<8 | 下一字节（14 位，最长 16383）
	zipStr14B = 1
	// 头部高 2 位 = 10：字符串，随后 4 字节大端给出长度
	zipStr32B = 2

	// 头部高 4 位 = 1111 表示「4 位立即数整数」。
	// 实际取值时用 header&0x0f 再减 1，可表示 0~12（见 ziplist.go）。
	zipInt04B = 0x0f        // high 4 bits of Int 04 encoding
	zipInt08B = 0xfe        // 11111110，1 字节有符号整数
	zipInt16B = 0xc0 | 0<<4 // 11000000，2 字节小端有符号整数
	zipInt24B = 0xc0 | 3<<4 // 11110000，3 字节小端有符号整数
	zipInt32B = 0xc0 | 1<<4 // 11010000，4 字节小端有符号整数
	zipInt64B = 0xc0 | 2<<4 //11100000，8 字节小端有符号整数

	// prevlen 字段的特殊标记：当上一 entry 的总长度 >= 254 时，
	// 该字节写 0xfe，随后 4 字节小端存放真实长度；否则 prevlen 只占 1 字节。
	// 注意它与 zipInt08B 数值相同，但出现的位置不同，含义也不同。
	zipBigPrevLen = 0xfe
)

// readList 解析 typeList（普通列表）对象。
//
// 输入字节布局：
//
//	[长度编码 N][RDB string #0][RDB string #1]...[RDB string #(N-1)]
//
// 其中每个 RDB string 都自带长度前缀，且可能是整数或 LZF 特殊编码，
// 具体由 readString 负责识别。这里只关心「有几个元素」以及逐个取出来。
//
// 返回值：按 RDB 中写入顺序排列的元素列表；任一元素读取失败立即返回错误。
func (dec *Decoder) readList() ([][]byte, error) {
	// 先读元素个数。RDB 的长度编码是变长的，不能直接按固定字节数解析
	size64, _, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	size := int(size64)
	// 已知元素个数，预分配容量避免 append 期间反复扩容
	values := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		// 列表元素统一按 RDB string 读取，长度/编码信息都在各自的头部里
		val, err := dec.readString()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// readQuickList 解析 typeListQuickList（RDB type 14）对象。
//
// 字节布局：
//
//	[长度编码 N][ziplist #0][ziplist #1]...[ziplist #(N-1)]
//
// 每个节点本身是一个 RDB string，字符串内容是完整的 ziplist（10 字节头 + entries + 0xFF）。
// readZipList 会读出该节点内的全部元素。
//
// 返回值：
//   - entries：把所有节点展平后的元素序列（调用方通常只关心这个）
//   - detail ：按节点分组的原始结构，节点边界在展平后会丢失，因此单独保留
func (dec *Decoder) readQuickList() ([][]byte, *model.QuicklistDetail, error) {
	// 节点（page/ziplist）数量
	size, _, err := dec.readLength()
	if err != nil {
		return nil, nil, err
	}
	// 总元素数未知，无法预分配精确容量
	entries := make([][]byte, 0)
	detail := &model.QuicklistDetail{}
	for i := 0; i < int(size); i++ {
		// 每个节点是一个内嵌 ziplist 的字符串
		page, err := dec.readZipList()
		if err != nil {
			return nil, nil, err
		}
		// 展平到返回值，同时保留该节点的元素切片用于 detail
		entries = append(entries, page...)
		detail.ZiplistStruct = append(detail.ZiplistStruct, page)
	}
	return entries, detail, nil
}

// readQuickList2 解析 typeListQuickList2（RDB type 18，Redis 7+ 的 quicklist v2）。
//
// 字节布局：
//
//	[长度编码 N]
//	重复 N 次：
//	  [长度编码 containerType]
//	  若 containerType == 1 (Plain) ：[RDB string]        —— 节点里只有 1 个元素
//	  若 containerType == 2 (Packed)：[RDB string(listpack)] —— 节点里有多个元素
//
// 与 v1 的区别：v2 的节点不再固定为 ziplist，既可以是单个 plain 字符串，
// 也可以是更省空间的 listpack；容器类型由每个节点前的长度字段标识。
//
// 返回值：展平后的元素列表 + 节点编码/各 listpack entry 大小的元信息。
func (dec *Decoder) readQuickList2() ([][]byte, *model.Quicklist2Detail, error) {
	// 节点数量
	size, _, err := dec.readLength()
	if err != nil {
		return nil, nil, err
	}
	entries := make([][]byte, 0)
	detail := &model.Quicklist2Detail{}
	for i := 0; i < int(size); i++ {
		// 每个节点先读一个「容器类型」长度字段：1=plain，2=packed
		length, _, err := dec.readLength()
		if err != nil {
			return nil, nil, err
		}
		if length == model.QuicklistNodeContainerPlain {
			// plain：节点内容就是一个普通字符串，即单个列表元素
			entry, err := dec.readString()
			if err != nil {
				return nil, nil, err
			}
			entries = append(entries, entry)
			detail.NodeEncodings = append(detail.NodeEncodings, model.QuicklistNodeContainerPlain)
		} else if length == model.QuicklistNodeContainerPacked {
			// packed：节点内容是一个 listpack，一次读出多个元素
			// lengths 记录 listpack 内每个 entry 占用的字节数，供内存统计使用
			page, lengths, err := dec.readListPack()
			if err != nil {
				return nil, nil, err
			}
			entries = append(entries, page...)
			// 注意：当前实现这里写入的是 Plain 常量（不是 Packed）。
			// 这只是 detail 元信息，不影响上面按 Packed 分支解析出的真实元素。
			detail.NodeEncodings = append(detail.NodeEncodings, model.QuicklistNodeContainerPlain)
			detail.ListPackEntrySize = append(detail.ListPackEntrySize, lengths)
		} else {
			// 只定义了 1/2 两种容器类型，出现其它值说明数据损坏或版本不兼容
			return nil, nil, errors.New("unknown quicklist node type")
		}

	}
	return entries, detail, nil
}

// WriteListObject 是编码器写 List 对象的统一入口。
//
// 写入策略与 Redis 的 list-max-ziplist-* 配置一致：
//  1. 元素数量不超过 maxEntries 且每个元素不超过 maxValue 时，用 ziplist 编码（typeListZipList）；
//  2. 否则退回 quicklist（typeListQuickList），按 listZipListSize 把元素切成多个 ziplist 节点。
//
// beforeWriteObject 负责状态机校验并按 options 写出 TTL（目前支持 WithTTL）。
// 成功后把编码器状态置为 writtenObjectState，允许继续写下一个对象。
func (enc *Encoder) WriteListObject(key string, values [][]byte, options ...interface{}) error {
	err := enc.beforeWriteObject(options...)
	if err != nil {
		return err
	}
	ok, err := enc.tryWriteListZipList(key, values, options...)
	if err != nil {
		return err
	}
	if !ok {
		// 未满足 ziplist 条件，退化为 quicklist
		err = enc.writeQuickList(key, values, options...)
		if err != nil {
			return err
		}
	}
	enc.state = writtenObjectState
	return nil
}

// tryWriteListZipList 尝试用 ziplist 编码写列表。
//
// 返回值 (ok, err) 的语义很关键：
//   - (false, nil)：不满足 ziplist 条件（元素太多或单个元素太大），此时尚未写任何字节，
//     调用方应回退到 quicklist；
//   - (true, err) ：已经写出了 type/key 等字节，中途失败只能报错，不能再回退（否则文件会重复/错位）。
//
// options 这里不直接使用，仅保持与其他 WriteXxxObject 一致的签名。
func (enc *Encoder) tryWriteListZipList(key string, values [][]byte, options ...interface{}) (bool, error) {
	// 条件一：元素个数不能超过 ziplist 允许的最大条目数
	if len(values) > enc.listZipListOpt.getMaxEntries() {
		return false, nil
	}
	strList := make([]string, 0, len(values))
	maxValue := enc.listZipListOpt.getMaxValue()
	for _, v := range values {
		// 条件二：任一元素超过最大长度就放弃 ziplist
		if len(v) > maxValue {
			return false, nil
		}
		// 零拷贝转 string，仅用于后续编码，不修改底层字节
		strList = append(strList, unsafeBytes2Str(v))
	}
	// 从这里开始已产生输出，后续错误必须返回 true，避免调用方再写 quicklist
	err := enc.write([]byte{typeListZipList})
	if err != nil {
		return true, err
	}
	err = enc.writeString(key)
	if err != nil {
		return true, err
	}
	err = enc.writeZipList(strList)
	if err != nil {
		return true, err
	}
	return true, nil
}

// writeQuickList 以 quicklist 编码写列表。
//
// 分页规则：顺序累加元素字节数，累计值达到 listZipListSize（默认 4KB）就结束当前页；
// 注意判断发生在 append 之后，因此单页可能略微超过 4KB（一个元素不可再拆分）。
// 每页再各自编码成一个 ziplist，整体格式为：
//
//	[typeListQuickList][key][页数][ziplist #0][ziplist #1]...
func (enc *Encoder) writeQuickList(key string, values [][]byte, options ...interface{}) error {
	var pages [][]string
	pageSize := 0
	var curPage []string
	for _, value := range values {
		curPage = append(curPage, unsafeBytes2Str(value))
		pageSize += len(value)
		if pageSize >= enc.listZipListSize {
			// 当前页已达到目标大小，封页并开始新页
			pageSize = 0
			pages = append(pages, curPage)
			curPage = nil
		}
	}
	// 收尾：最后不足一页的剩余元素也要写入
	if len(curPage) > 0 {
		pages = append(pages, curPage)
	}
	err := enc.write([]byte{typeListQuickList})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	// quicklist 先写节点数量，再依次写各节点的 ziplist
	err = enc.writeLength(uint64(len(pages)))
	if err != nil {
		return err
	}
	for _, page := range pages {
		err = enc.writeZipList(page)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeZipList 把一组字符串编码成一个 ziplist 字符串。
//
// ziplist 内存布局（全部小端）：
//
//	offset 0  4 字节 zlbytes ：整个 ziplist 的字节数（含头、entries、结尾 0xFF）
//	offset 4  4 字节 zltail  ：最后一个 entry 相对起始地址的偏移
//	offset 8  2 字节 zllen   ：entry 个数（最大 65535）
//	offset 10 ...            ：连续的 entry，每个 entry 由 prevlen + encoding + data 组成
//	末尾      1 字节 0xFF    ：结束标记
//
// 每个 entry 的 prevlen 记录「前一个 entry 的总字节数」，用于支持从后向前遍历；
// 当 prevlen >= 254 时用 1 字节 0xFE + 4 字节小端真实长度表示。
//
// 最后用 writeNanString 而不是 writeString，是为了避免把整段二进制误判成「可整数编码」
// 而写成整数，ziplist 必须原样保存字节。
func (enc *Encoder) writeZipList(values []string) error {
	buf := make([]byte, 10) // reserve 10 bytes for zip list header
	zlBytes := 11           // header(10bytes) + zl end(1byte)
	zlTail := 10            // 最后一个 entry 的偏移；空表时指向结尾 0xFF
	var prevLen uint32
	for i, value := range values {
		// prevLen 传入的是上一个 entry 的总长度，编码进当前 entry 的 prevlen 字段
		entry := encodeZipListEntry(prevLen, value)
		buf = append(buf, entry...)
		prevLen = uint32(len(entry))
		zlBytes += len(entry)
		// zltail 要指向「最后一个 entry」的起点：
		// 最后一个 entry 后面没有 entry 了，所以只在 i 不是最后一个时累加
		if i < len(values)-1 {
			zlTail += len(entry)
		}
	}
	// 结尾标记，解析时据此判断 ziplist 结束
	buf = append(buf, 0xff)
	// 回填头部三个字段（小端）
	binary.LittleEndian.PutUint32(buf[0:4], uint32(zlBytes))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(zlTail))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(values)))
	return enc.writeNanString(unsafeBytes2Str(buf))
}
