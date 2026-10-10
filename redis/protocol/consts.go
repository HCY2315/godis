/*
 * 固定的、内容不变的简单回复（PONG/OK/QUEUED/空等）。
 * 技巧：这些回复的字节内容固定，所以要么用包级变量缓存字节（pongBytes/okBytes），
 * 要么用单例对象（theOkReply/theQueuedReply）复用，避免每次命令都重新分配。
 */
package protocol

import (
	"bytes"
	"github.com/hdt3213/godis/interface/redis"
)

// PongReply —— 命名含义：对 PING 的 "+PONG" 回复。
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

// ToBytes marshal redis.Reply
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

// OkReply —— 命名含义：通用的 "+OK" 回复（写操作成功）。用单例 theOkReply 复用。
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

// MakeOkReply returns a ok protocol
func MakeOkReply() *OkReply {
	return theOkReply
}

var nullBulkBytes = []byte("$-1\r\n")

// NullBulkReply —— 命名含义：空值批量串（"$-1"），表示键不存在。
type NullBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// MakeNullBulkReply creates a new NullBulkReply
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

var emptyMultiBulkBytes = []byte("*0\r\n")

// EmptyMultiBulkReply —— 命名含义：空数组（"*0"）。
type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

func IsEmptyMultiBulkReply(reply redis.Reply) bool {
	return bytes.Equal(reply.ToBytes(), emptyMultiBulkBytes)
}

// NoReply —— 命名含义：不回复任何内容。用于 SUBSCRIBE 等由推送机制负责响应的命令。
type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}

// QueuedReply —— 命名含义：事务入队成功回复 "+QUEUED"（单例复用）。
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// ToBytes marshal redis.Reply
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

var theQueuedReply = new(QueuedReply)

// MakeQueuedReply returns a QUEUED protocol
func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}
