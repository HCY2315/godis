/*
 * 一致性哈希。
 * 用途：在多个节点间分配 key，且节点增减时尽量少地影响已有映射。
 * 原理：把节点和 key 都哈希到一个环上，key 顺时针找到的第一个节点就是归属节点；
 * 为缓解分布不均，引入“虚拟节点”(replicas)。godis 的集群寻址用到它。
 */
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// HashFunc —— 命名含义：哈希函数（把数据映射成 uint32）。
type HashFunc func(data []byte) uint32

// Map —— 命名含义：一致性哈希环。
type Map struct {
	hashFunc HashFunc
	replicas int            // 每个节点的虚拟节点数
	keys     []int          // 环上已排序的哈希值
	hashMap  map[int]string // 哈希值 -> 节点名
}

// New creates a new Map
func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in Map
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode add the given nodes into consistent hash circle
func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// support hash tag
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// PickNode gets the closest item in the hash to the provided key.
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
