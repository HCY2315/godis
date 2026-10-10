// list 包：列表抽象。实现有 linked.go（双向链表）和 quicklist.go（快速列表，
// 由多个链表节点拼成，类似 Redis 的 quicklist），Redis 的 LIST 命令基于它。
package list

// Expected —— 命名含义：元素匹配判断函数，用于按值删除。
type Expected func(a interface{}) bool

// Consumer —— 命名含义：遍历回调；返回 true 继续，false 中断。
type Consumer func(i int, v interface{}) bool

// List —— 命名含义：列表接口，含增删改查、遍历、区间读取。
type List interface {
	Add(val interface{})
	Get(index int) (val interface{})
	Set(index int, val interface{})
	Insert(index int, val interface{})
	Remove(index int) (val interface{})
	RemoveLast() (val interface{})
	RemoveAllByVal(expected Expected) int
	RemoveByVal(expected Expected, count int) int
	ReverseRemoveByVal(expected Expected, count int) int
	Len() int
	ForEach(consumer Consumer)
	Contains(expected Expected) bool
	Range(start int, stop int) []interface{}
}
