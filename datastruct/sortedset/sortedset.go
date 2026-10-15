// sortedset.go：有序集合（ZSET 命令底层），对应 Redis 的 ZSET。
//
// 经典“双结构”设计（与 Redis 一致）：
//   - dict（map[string]*Element）：member -> 元素，O(1) 按成员查分值；
//   - skiplist：按 (score, member) 有序，支持排名(ZRANK)/区间(ZRANGEBYSCORE)等 O(log n) 操作。
//
// 两个结构必须保持同步：任何新增/改分/删除都要同时改 dict 与 skiplist。
package sortedset

import (
	"strconv"

	"github.com/hdt3213/godis/lib/wildcard"
)

// SortedSet —— 有序集合，按分值(score)排序；member 唯一。
type SortedSet struct {
	dict     map[string]*Element // member -> 元素（快速按成员查）
	skiplist *skiplist           // 按分值有序（范围/排名查询）
}

// Make 创建一个空的有序集合（dict 与 skiplist 均为空）。
func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

// Add 插入或更新成员，返回是否“新增”（true=新增，false=已存在只是改分）。
//
// 两种情况：
//   - member 已存在：只更新分值。若分值变化，需先在跳表删旧分节点、再按新分插入（维持有序）；
//   - member 不存在：dict 记录 + 跳表插入。
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			// 分值变了：跳表里的位置也变了，删旧插新
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// Len 返回成员个数。
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// Get 按成员取元素，返回 (元素, 是否存在)。
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove 删除指定成员，返回是否存在（存在才删，两结构同步删）。
func (sortedSet *SortedSet) Remove(member string) bool {
	v, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, v.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

// GetRank 返回成员排名（0-based），不存在返回 -1；desc=true 表示按降序排名。
//
// 跳表内部 getRank 返回的是 1-based 名次，这里换算：
//   - 升序：rank-1（转成 0-based）；
//   - 降序：length - r（倒过来数）。
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	r := sortedSet.skiplist.getRank(member, element.Score)
	if desc {
		r = sortedSet.skiplist.length - r
	} else {
		r--
	}
	return r
}

// ForEachByRank 遍历排名落在 [start, stop) 的成员（0-based，升序；desc=true 则降序）。
// consumer 返回 false 时提前结束。
func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	// 定位起始节点：升序从头、降序从尾；start>0 时用 getByRank 跳过去
	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(start + 1)
		}
	}

	// 沿底层链表(升序 forward / 降序 backward)逐个访问，共 stop-start 个
	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

// RangeByRank 返回排名落在 [start, stop) 的成员切片（0-based，asc=desc 控制方向）。
func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEachByRank(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// RangeCount 统计分值落在给定区间（min,max）内的成员数量。
func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	var i int64 = 0
	// ascending order
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		gtMin := min.less(element) // greater than min
		if !gtMin {
			// 还没进入区间（比下界小），继续往后找
			return true
		}
		ltMax := max.greater(element) // less than max
		if !ltMax {
			// 已越过上界，后面都不可能命中，直接停
			return false
		}
		// gtMin && ltMax
		i++
		return true
	})
	return i
}

// ForEach 遍历分值落在 (min,max) 内的成员，先跳过 offset 个，再访问最多 limit 个（limit<0 表示不限）。
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// find start node
	// 找到区间端点：升序取区间最小节点、降序取区间最大节点
	var node *node
	if desc {
		node = sortedSet.skiplist.getLastInRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInRange(min, max)
	}

	// 跳过 offset 个
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	// A negative limit returns all elements from the offset
	// limit<0 表示从 offset 到末尾全部；否则最多 limit 个
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		// 越过区间边界即结束
		gtMin := min.less(&node.Element) // greater than min
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

// Range 返回分值落在 (min,max) 内、跳过 offset、最多 limit 个的成员列表。
// param limit: <0 表示不限制数量。
func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// RemoveRange 删除分值落在 (min,max) 内的所有成员，返回删除数量（跳表删后再同步删 dict）。
func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.RemoveRange(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

// PopMin 弹出分值最小的 count 个成员（对应 ZPOPMIN），返回被弹出的元素。
func (sortedSet *SortedSet) PopMin(count int) []*Element {
	first := sortedSet.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	// 以最小分值为起点，向右删 count 个
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := sortedSet.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

// RemoveByRank 删除排名在 [start, stop) 的成员（0-based，升序），返回删除数量。
// 跳表接口是 1-based 且含 start、不含 stop，故传入 start+1 / stop+1 做换算。
func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := sortedSet.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

// ZSetScan 扫描匹配 pattern 的成员，返回 [member, score, member, score, ...] 的扁平列表。
// 注意：当前实现忽略 cursor（一次全量扫描 dict），返回的游标恒为 0。
func (sortedSet *SortedSet) ZSetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range sortedSet.dict {
		if pattern == "*" || matchKey.IsMatch(k) {
			elem, exists := sortedSet.dict[k]
			if !exists {
				continue
			}
			result = append(result, []byte(k))
			result = append(result, []byte(strconv.FormatFloat(elem.Score, 'f', 10, 64)))
		}
	}
	return result, 0
}
