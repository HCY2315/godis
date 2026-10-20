package memprofiler

import "github.com/hdt3213/rdb/model"

// sizeOfStreamObject 估算 stream 对象的内存占用。
//
// 逐层累加：
//   - stream 结构体本身（2 指针 + 8 + 16）；
//   - 底层 rax（基数树）结构 + 树的节点开销；
//   - V2 及以上版本新增的字段（两个 streamID + 一个 uint64）；
//   - 每个消费者组 streamCG 的结构、entries_read 字段、PENDING 的 rax 树与 streamNACK；
//   - 每个消费者的结构、名字字符串、其 PENDING 的 rax 树。
func sizeOfStreamObject(obj *model.StreamObject) int {
	size := sizeOfPointer()*2 + 8 + 16 + // size of stream struct
		sizeOfPointer() + 8*2 + // rax struct
		sizeOfStreamRaxTree(len(obj.Entries))
	if obj.Version >= 2 {
		size += 16*2 + 8 // size of 2 new streamID and a uint64
	}
	for _, group := range obj.Groups {
		size += sizeOfPointer()*2 + 16 // size of struct streamCG
		if obj.Version >= 2 {
			size += 8 // size of new field entries_read
		}
		pendingCount := len(group.Pending)
		size += sizeOfStreamRaxTree(pendingCount) +
			pendingCount*(sizeOfPointer()+8*2) //  streamNACK
		for _, consumer := range group.Consumers {
			size += sizeOfPointer()*2 + 8 + // streamConsumer
				sizeOfString(consumer.Name) +
				sizeOfStreamRaxTree(len(consumer.Pending))
		}
	}
	return size
}

// sizeOfStreamRaxTree 估算 stream 底层 rax（基数树）按元素数量估算的内存。
//
// 说明：这是「粗略估算」。要精确就得真的构建一棵同构的基数树来数节点，
// 代价太大。这里假设节点数约为元素数的 2.5 倍（实际可能 1~3 倍），
// 再套用 Redis 源码 streamRadixTreeMemoryUsage 的经验公式：
// 每元素 16 字节 + 每节点 4 字节 + 每节点 30*sizeof(long) 字节。
func sizeOfStreamRaxTree(elementCount int) int {
	// This is a very rough estimation. The only alternative to doing an estimation,
	// is to fully build a radix tree of similar design, and elementCount the nodes.
	// There should be at least as many nodes as there are elements in the radix tree (possibly up to 3 times)
	nodeCount := int(float64(elementCount) * 2.5)
	// formula for memory estimation copied from Redis's streamRadixTreeMemoryUsage
	return 16*elementCount + 4*nodeCount + 30*sizeOfLong()*nodeCount
}
