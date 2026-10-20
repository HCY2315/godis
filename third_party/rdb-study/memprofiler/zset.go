package memprofiler

import (
	"github.com/hdt3213/rdb/model"
	"math"
	"math/rand"
)

// skipListOverhead 估算 zset 内部「跳表(skiplist)」的固定开销。
//
// 组成：zskiplist 结构（头/尾指针 + 长度 + 层数，约 2 个指针）+ 内部哈希表开销
// + header 节点的固定字段（2 个指针 + 16 字节的 level/backward 等）。
func skipListOverhead(size int) int {
	return 2*sizeOfPointer() + hashtableOverhead(size) + (2*sizeOfPointer() + 16)
}

// skipListEntryOverhead 估算「每个跳表节点」的额外开销。
//
// 组成：哈希表 dictEntry + 2 个指针（forward/backward）+ 8 字节 score
// + 按「随机层数的数学期望」估算的层指针数组开销。
func skipListEntryOverhead() int {
	return hashTableEntryOverhead() + 2*sizeOfPointer() + 8 + int(math.Round(float64(sizeOfPointer()+8)*MathExpectationOfRandomLevel))
}

// MathExpectationOfRandomLevel is mathematical expectation of zsetRandomLevel(), used to guarantee the stable results
//
// 跳表每个节点的层数是随机的（Redis 用 p=0.25 逐层提升），
// 这里取其数学期望 ~1.33，用来让内存估算结果「稳定可复现」，而不是每次都随机波动。
const MathExpectationOfRandomLevel = 1.33

// zsetRandomLevel 模拟 Redis 跳表的随机层数生成算法（仅用于说明，估算时不直接调用）。
// Redis 规则：从第 1 层开始，每次以 1/4 概率再升一层，最高 32 层。
func zsetRandomLevel() int {
	const maxLevel = 32
	const p = 0.25
	i := 1
	r := rand.Intn(0xFFFF)
	for r < 0xFFFF/4 {
		i++
		r = rand.Intn(0xFFFF)
		if i >= maxLevel {
			return maxLevel
		}
	}
	return i
}

// sizeOfZSetObject 估算 zset 对象的内存占用。
//
// 分两种情况：
//   - ziplist / listpack 紧凑编码：直接使用解析时记录的原始字节串大小（最省内存）；
//   - 普通编码（跳表+字典）：跳表固定开销 + 每个成员的成员字符串 + 8 字节 score + 节点开销。
func sizeOfZSetObject(o *model.ZSetObject) int {
	if o.GetEncoding() == model.ZipListEncoding {
		extra := o.Extra.(*model.ZiplistDetail)
		return extra.RawStringSize
	} else if o.GetEncoding() == model.ListPackEncoding {
		extra := o.Extra.(*model.ListpackDetail)
		return extra.RawStringSize
	}
	size := skipListOverhead(len(o.Entries))
	for _, entry := range o.Entries {
		size += sizeOfString(entry.Member) + 8 + skipListEntryOverhead() // size of score is 8 (double)
	}
	return size
}
