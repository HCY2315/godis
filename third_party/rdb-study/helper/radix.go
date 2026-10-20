package helper

import (
	"container/list"
	"strconv"
	"strings"
)

// radixNode 是「基数树(radix tree / 压缩前缀树)」的一个节点。
//
// 与普通前缀树不同，radix tree 会把「只有一个子节点」的链压缩成一段 path，
// 因此节点存的是「一段字符串」而不是单个字符，查询更省内存、层数更少。
//
// 例：插入 "User:1"、"User:2" 后大致为
//
//	root → "User:" → "1"
//	                → "2"
type radixNode struct {
	path      string       // 本节点代表的字符串片段（相对父节点）
	end       bool         // 是否有一个完整的 key 在此结束
	children  []*radixNode // 子节点
	totalSize int          // 该前缀下所有 key 的内存大小之和（用于前缀聚合统计）
	keyCount  int          // 该前缀下 key 的数量
	fullpath  string       // 从根到本节点拼成的完整前缀（便于输出）
}

// radixTree 是基数树，只保留一个根节点。
type radixTree struct {
	root *radixNode
}

// newRadixTree 创建一棵空的基数树（根节点 path 为空）。
func newRadixTree() *radixTree {
	return &radixTree{
		root: &radixNode{},
	}
}

// commonPrefixLen 返回两个字符串的「公共前缀长度」。
func commonPrefixLen(wordA, wordB string) int {
	var i int
	for i < len(wordA) && i < len(wordB) && wordA[i] == wordB[i] {
		i++
	}
	return i
}

// insert 把 word 插入基数树，并沿途累加 size / keyCount。
//
// 这是整个前缀统计的核心算法，分几种情况：
//  1. word 与当前节点的 path 只有部分公共前缀 → 需要「分裂」当前节点：
//     新建一个子节点承接原 path 的后半段（连同其原有 children/统计值），
//     当前节点只保留公共前缀部分。
//  2. 只要不是根节点且确实有公共前缀，就说明 word 经过此节点，累加统计值。
//  3. 若 word 已被完全匹配（i == len(word)）→ 标记 end=true，结束。
//  4. 若 word 还有剩余部分 → 看能否找到一个以相同首字符开头的子节点继续走；
//     找不到就为剩余部分新建一个叶子节点。
func (tree *radixTree) insert(word string, size int) {
	root := tree.root
	fullword := word // 保存原始完整 key，供叶子节点记录 fullpath
	node := root
walk:
	for {
		i := commonPrefixLen(word, node.path)
		if i < len(node.path) {
			// 情况1：需要分裂当前节点。
			// node.path 后半段 [i:] 连同原有 children/统计值转移到新子节点，
			// node 自身缩成公共前缀 [0:i]。
			// assert: node == root || i > 0, because it is the first loop or from `for _, child := range node.children`
			newChild := &radixNode{
				path:      node.path[i:],
				children:  node.children,
				end:       node.end,
				totalSize: node.totalSize,
				keyCount:  node.keyCount,
				fullpath:  node.fullpath,
			}
			node.children = []*radixNode{newChild}
			node.fullpath = node.fullpath[:len(node.fullpath)-(len(node.path)-i)]
			node.path = node.path[:i]
			node.end = false
		}
		// 情况2：word 经过此节点（非根且有公共前缀），累加统计值
		if i > 0 || node == root {
			node.totalSize += size
			node.keyCount++
		}
		if i == len(word) {
			// 情况3：word 在此节点结束
			// assert node.fullpath == fullword
			node.end = true
			break
		}
		if i < len(word) {
			word = word[i:]
			// 情况4：在 children 里找首字符相同的子节点，继续往下走
			c := word[0]
			for _, child := range node.children {
				if child.path[0] == c {
					node = child
					continue walk
				}
			}

			// 找不到匹配的子节点 → 为剩余部分新建叶子节点
			child := &radixNode{}
			child.path = word
			child.end = true
			child.fullpath = fullword
			child.totalSize = size
			child.keyCount = 1
			node.children = append(node.children, child)
			return
		}
	}
}

// traverse 按「广度优先(BFS)」遍历整棵树，对每个节点回调 cb(node, depth)。
// depth 从 1 开始（根节点深度为 1）。cb 返回 false 可提前终止遍历。
func (tree *radixTree) traverse(cb func(node *radixNode, depth int) bool) {
	root := tree.root
	type nodeDepth struct {
		node  *radixNode
		depth int
	}
	queue := list.New()
	queue.PushBack(&nodeDepth{root, 1})
	for queue.Len() > 0 {
		head := queue.Front()
		queue.Remove(head)
		node2 := head.Value.(*nodeDepth)
		if !cb(node2.node, node2.depth) {
			return
		}
		depth := node2.depth + 1
		for _, child := range node2.node.children {
			queue.PushBack(&nodeDepth{child, depth})
		}
	}
}

// GetSize 让 radixNode 满足 Sized 接口，从而可以放进 TopN 列表（按 totalSize 排序）。
func (n *radixNode) GetSize() int {
	return int(n.totalSize)
}

// genKey 把「db 编号 + key」拼成一个字符串（用空格分隔），作为插入基数树的完整路径。
// 这样第 1 层就能按数据库分开统计。
func genKey(db int, key string) string {
	return strconv.Itoa(db) + " " + key
}

// parseNodeKey 是 genKey 的逆操作：从 "db key" 中解析出 db 编号和真实 key。
// 空串返回 (-1, "")；若只有 db 部分（数据库根节点），key 返回空串。
func parseNodeKey(key string) (int, string) {
	if key == "" {
		return -1, ""
	}
	index := strings.Index(key, " ")
	db, _ := strconv.Atoi(key[:index])
	var realKey string
	// if key is db root, index+1 == len(key)
	if index+1 < len(key) {
		realKey = key[index+1:]
	}
	return db, realKey
}
