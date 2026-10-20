package helper

import (
	"sort"
)

// Sized 是「有大小」的抽象：任何能返回自身大小(字节)的类型都能放进 TopN 列表。
// model.RedisObject 满足该接口（它有 GetSize() int）。
type Sized interface {
	GetSize() int
}

// topList 是一个「容量固定、始终按大小降序排列」的 TopN 容器。
// 它不一次性收集所有元素再排序，而是每来一个元素就用二分插入维护有序性，
// 超过容量就丢掉末尾（最小的）——因此内存占用只有 O(N)，适合流式处理超大 RDB。
type topList struct {
	list     []Sized // 按 GetSize() 降序排列（list[0] 最大）
	capacity int     // 最多保留多少个
}

// add 把 x 插入到合适位置，保持 list 降序，并在超容量时截断。
//
// 做法：用 sort.Search 二分找到「第一个 size ≤ x.size」的位置 index，
//
//	然后把 x 插到它前面（这样大的排在前面，降序）。
//	最后若长度超过 capacity，就切掉尾部多余元素。
func (tl *topList) add(x Sized) {
	index := sort.Search(len(tl.list), func(i int) bool {
		return tl.list[i].GetSize() <= x.GetSize()
	})
	tl.list = append(tl.list, x)
	copy(tl.list[index+1:], tl.list[index:]) // 把 [index:] 后移一位，腾出 index
	tl.list[index] = x
	if len(tl.list) > tl.capacity {
		tl.list = tl.list[:tl.capacity] // 丢掉末尾（最小的）
	}
}

// newToplist 创建一个容量为 cap 的 TopN 容器。
func newToplist(cap int) *topList {
	return &topList{
		capacity: cap,
	}
}
