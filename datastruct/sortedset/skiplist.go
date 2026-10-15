// skiplist.go：跳表（skip list）实现，是 SortedSet（ZSET）的底层有序结构之一。
//
// 为什么用跳表：
//   - 跳表 = 有序链表 + 多级索引。最底层(level 0)是一条包含全部节点的有序链表；
//     上层是“快速通道”索引，指针可以一次跨过多个节点，从而把查找从 O(n) 降到 O(log n)。
//   - 相比平衡树（红黑树/AVL），跳表插入/删除不需要旋转与再平衡，实现简单，是 Redis ZSET 的同款选择。
//
// 双结构配合（见 sortedset.go）：
//   - dict(member -> Element)：O(1) 按成员查分值；
//   - skiplist：按 score 有序，支持排名(ZRANK)/区间(ZRANGEBYSCORE)/范围删除，均为 O(log n)。
//
// 排序规则：先按 Score 升序；Score 相同再按 Member 升序（保证全序、可稳定比较）。
package sortedset

import (
	"math/bits"
	"math/rand"
)

// maxLevel —— 跳表最大层数。节点层数在 [1, maxLevel] 之间随机取。
// 取 16 是经验值：足以支撑千万级元素仍保持 O(log n)，又不至于浪费索引空间。
const (
	maxLevel = 16
)

// Element —— 跳表元素，即“成员 member + 分值 score”对。
// 它是暴露给上层的值类型（如 ZRANGE 返回的就是 Element 列表）。
type Element struct {
	Member string
	Score  float64
}

// Level —— 跳表节点在某一层的索引信息。
type Level struct {
	forward *node // 指向同层的下一个节点（Score 更大；Score 相同则 Member 更大）
	span    int64 // 跨度：forward 指针跨过的底层(level0)节点数（含目标、不含自身），用于按排名查询
}

// node —— 跳表的内部节点。
type node struct {
	Element           // 内嵌成员与分值
	backward *node    // 后退指针：底层(level0)是一条双向链表，支持从尾向前遍历
	level    []*Level // 该节点在各层的向前索引；level[0] 是最底层，索引越高越“稀疏”
}

// skiplist —— 跳表本体。
type skiplist struct {
	header *node // 哨兵头节点：固定 maxLevel 层，本身不存有效成员，是查找/插入的入口
	tail   *node // 尾节点（底层最后一个真实节点）
	length int64 // 元素个数（真实节点数，不含 header）
	level  int16 // 当前实际使用的最高层数（1 表示只有最底层）
}

// makeNode 创建一个层数为 level 的新节点，并初始化它每层的 Level。
// 层数是“这个节点在多少层里出现”，越高越稀有（由 randomLevel 决定）。
func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

// makeSkiplist 创建一个空跳表：
//   - header 是哨兵头节点，固定有 maxLevel(16) 层，本身不存成员，
//     作为从最高层往下查找的入口（这样真实节点的随机层数都能挂上去）。
//   - level=1 表示当前只用到最底层；length/tail 为空。
func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

// randomLevel 随机生成一个新节点的层数，范围 [1, maxLevel]。
//
// 原理（几何/幂律分布）：越高层出现概率越小，约一半节点局限在 level1、1/4 到 level2……
// 每往上一层节点数大约减半，形成“金字塔”式索引，期望高度 O(log n)。
// 用 bits.Len64 取最高位来高效模拟该分布，无需循环掷硬币。
func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

// insert 把 (member, score) 插入跳表，返回新节点。
//
// 两大核心数组（算法精髓）：
//   - update[i]：第 i 层中，新节点“插入位置的前驱”，新节点将挂到它后面；
//   - rank[i]  ：从 header 走到 update[i] 所跨过的底层节点数（1-based，用于维护 span）。
//
// 流程：① 自顶向下定位每层插入点 → ② 随机层数并“长高”跳表 → ③ 逐层接入并修正 span/backward → ④ 计数。
func (skiplist *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel) // link new node with node in `update`
	rank := make([]int64, maxLevel)

	// ① 从最高层开始逐层“向右走到底、再向下一层”，定位每层最后一个比新值小的节点
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			// 本层起点排名，承接上一层已跨过的数量
			rank[i] = rank[i+1] // store rank that is crossed to reach the insert position
		}
		if node.level[i] != nil {
			// 本层尽量向前走：score 更小，或 score 相同但 member 更小
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score, different key
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// ② 随机层数；若高于当前跳表层数，多出来的层其前驱就是 header
	level := randomLevel()
	// extend skiplist level
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length // header 到末尾，跨度即当前节点总数
		}
		skiplist.level = level
	}

	// ③ 创建节点并逐层接入：先接 forward，再修正前后两段 span
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// span 维护：update[i]->node 新增跨度 = (rank[0]-rank[i])+1；
		// node->原后继 的跨度 = 原跨度减去上面这一段
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 未涉及新节点的更高层：后继被“顶远一格”，span +1
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// ④ 维护底层双向链表指针与 tail
	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

/*
 * param node: node to delete
 * param update: backward node (of target)
 */
// removeNode 从跳表中摘除已知节点 node。
// update 是每一层中 node 的前驱（由上层查找得到），用于修改这些前驱的 forward/span。
func (skiplist *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node {
			// 该层确实指向 node：跳过它，并把被删节点的跨度并入前驱（减去自身占的 1）
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			// 该层没指向 node：跨过的节点少了一个，span 减 1
			update[i].level[i].span--
		}
	}
	// 维护底层双向链表
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skiplist.tail = node.backward
	}
	// 若最高层已空，则把跳表实际层数降下来
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}

/*
 * return: has found and removed node
 */
// remove 按 (member, score) 定位并删除节点，返回是否删除成功。
// 定位方式与 insert 相同：逐层向右、比较 (score, member)。
func (skiplist *skiplist) remove(member string, score float64) bool {
	/*
	 * find backward node (of target) or last node of each level
	 * their forward need to be updated
	 */
	update := make([]*node, maxLevel)
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	// 底层前驱的下一个才是候选，需精确匹配 (score, member)
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		skiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

/*
 * return: 1 based rank, 0 means member not found
 */
// getRank 返回 member 的排名（1-based，从 1 开始），找不到返回 0。
// 通过累加各层 span 跳着计数，无需逐个遍历。
func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) { // 用 <= 把目标本身也跳过，rank 刚好是它自身名次
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		// 某层若恰好停在目标上，即可直接返回该排名
		if x.Member == member {
			return rank
		}
	}
	return 0
}

/*
 * 1-based rank
 */
// getByRank 返回排名为 rank（1-based）的节点，越界返回 nil。
// 同样靠 span 跳层：能跨就跨，直到正好跨到目标。
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		// 只要不越过 rank 就尽量往前跳
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

// hasInRange 判断区间 (min, max) 内是否存在节点。
// 先用边界交集快速否定，再用 tail/head 与边界比较排除“区间全在表外”的情况。
func (skiplist *skiplist) hasInRange(min Border, max Border) bool {
	if min.isIntersected(max) { //是有交集的，则返回false
		return false
	}

	// min > tail：区间下界都已超过表尾，说明没有
	n := skiplist.tail
	if n == nil || !min.less(&n.Element) {
		return false
	}
	// max < head：区间上界都还没到表头，说明没有
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(&n.Element) {
		return false
	}
	return true
}

// getFirstInRange 返回区间 (min, max) 内第一个（最小的）节点，无则返回 nil。
func (skiplist *skiplist) getFirstInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		// if forward is not in range than move forward
		// 本层尽量向前走，直到下一跳已进入区间
		for n.level[level].forward != nil && !min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	n = n.level[0].forward
	if !max.greater(&n.Element) {
		return nil
	}
	return n
}

// getLastInRange 返回区间 (min, max) 内最后一个（最大的）节点，无则返回 nil。
func (skiplist *skiplist) getLastInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	n := skiplist.header
	// scan from top level
	for level := skiplist.level - 1; level >= 0; level-- {
		// 本层尽量向前走，直到下一跳已超出区间上界
		for n.level[level].forward != nil && max.greater(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	if !min.less(&n.Element) {
		return nil
	}
	return n
}

/*
 * return removed elements
 */
// RemoveRange 删除区间 (min, max) 内的节点，limit>0 时最多删 limit 个。
// 返回被删除的 Element 列表（对应 ZREMRANGEBYSCORE）。
func (skiplist *skiplist) RemoveRange(min Border, max Border, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	// find backward nodes (of target range) or last node of each level
	// 找出区间起点在每层的前驱
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(&node.level[i].forward.Element) { // already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	// node is the first one within range
	// 底层起点：第一个落入区间的节点
	node = node.level[0].forward

	// remove nodes in range
	// 沿 level0 逐个删除，直到超出上界或达到 limit
	for node != nil {
		if !max.greater(&node.Element) { // already out of range
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// RemoveRangeByRank 按排名删除：删除排名在 [start, stop) 的节点（1-based，含 start、不含 stop）。
// 返回被删除的 Element 列表（对应 ZREMRANGEBYRANK）。
// 1-based rank, including start, exclude stop
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level
	// 定位到 start 前一个节点在每层的前驱
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range

	// remove nodes in range
	// 从 start 起逐个删除，直到排名到达 stop
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
