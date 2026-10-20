package model

// 本文件定义了「解析细节(Detail)」结构，它们会被挂到 BaseObject.Extra 字段上。
//
// 为什么需要它们？普通的 model 对象只保存「逻辑数据」（如 list 的所有元素），
// 但做「内存占用精确估算」时，还需要知道底层是怎么编码的（几个 ziplist、每个多大等）。
// 于是解析器在解析紧凑编码时，把内部结构细节记录到这些 Detail 里，供 memprofiler 使用。

// QuicklistDetail 记录 quicklist 编码的内部细节（旧版 quicklist = 由多个 ziplist 组成的链表）。
type QuicklistDetail struct {
	// ZiplistStruct 保存 quicklist 中的每一个 ziplist，每个 ziplist 又是一个 [][]byte 的元素列表。
	// 三层切片：quicklist → 若干 ziplist → 若干元素。
	ZiplistStruct [][][]byte
}

// Quicklist2Detail 记录 quicklist2 编码的内部细节（新版 quicklist = 由 listpack 或普通字符串节点组成）。
type Quicklist2Detail struct {
	// NodeEncodings 记录每个节点是「普通字符串(Plain)」还是「listpack 压缩(Packed)」。
	NodeEncodings []int
	// ListPackEntrySize 当节点是 Packed 时，记录该 listpack 内每个 entry 的大小（用于精确算内存）。
	ListPackEntrySize [][]uint32
}

// IntsetDetail 记录 intset 编码的细节（intset 是「有序整数集合」的紧凑编码）。
type IntsetDetail struct {
	RawStringSize int // 原始字节串大小（intset 在 RDB 中是以一整个二进制串存储的）
}

// QuicklistNodeContainerPlain 表示 quicklist2 的节点是「普通字符串」而非 listpack。
const QuicklistNodeContainerPlain = 1

// QuicklistNodeContainerPacked 表示 quicklist2 的节点是「listpack 压缩包」。
const QuicklistNodeContainerPacked = 2

// ZiplistDetail 记录 ziplist 编码的细节。
type ZiplistDetail struct {
	RawStringSize int // 原始字节串大小
}

// ListpackDetail 记录 listpack 编码的细节（listpack 是 ziplist 的替代者）。
type ListpackDetail struct {
	RawStringSize int // 原始字节串大小
}
