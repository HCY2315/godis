/*
 * RESP（REdis Serialization Protocol）回复类型的实现。
 * 阶段定位：回复阶段——把命令结果序列化成客户端能读懂的字节流。
 *
 * RESP 用第一个字节区分类型：
 *   '+' 简单字符串(状态)   -PongReply/OkReply/StatusReply
 *   '-' 错误               -StandardErrReply 等
 *   ':' 整数               -IntReply
 *   '$' 批量字符串         -BulkReply / NullBulkReply
 *   '*' 数组               -MultiBulkReply / MultiRawReply / EmptyMultiBulkReply
 * 每个类型都实现 redis.Reply 接口（即 ToBytes）。
 * 命名规律：MakeXxxReply 是构造函数，XxxReply 是类型本身。
 */
package protocol

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/hdt3213/godis/interface/redis"
)

var (
	// CRLF —— 命名含义：Carriage Return Line Feed，即 "\r\n"，RESP 的行分隔符。
	CRLF = "\r\n"
)

/* ---- Bulk Reply ---- */

// BulkReply —— 命名含义：批量字符串回复（RESP 的 '$' 类型，二进制安全）。
// 用途：GET 等返回的 value。
type BulkReply struct {
	Arg []byte
}

// MakeBulkReply —— 命名含义：构造函数，创建 BulkReply。
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// ToBytes —— 序列化成 "$<len>\r\n<arg>\r\n"；Arg 为 nil 时返回 nullBulk（$-1）。
func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Multi Bulk Reply ---- */

// MultiBulkReply —— 命名含义：多批量回复（RESP 的 '*' 数组，元素是批量字符串）。
// 用途：MGET、LRANGE、以及客户端发来的命令本身都用它表示。
type MultiBulkReply struct {
	Args [][]byte
}

// MakeMultiBulkReply —— 命名含义：构造函数，创建 MultiBulkReply。
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer
	//Calculate the length of buffer
	argLen := len(r.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range r.Args {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	//Allocate memory
	buf.Grow(bufLen)
	//Write string step by step,avoid concat strings
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			//Write bytes,avoid slice of byte to string(slicebytetostring)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Multi Raw Reply ---- */

// MultiRawReply —— 命名含义：多“原始”回复，数组元素本身就是完整的 Reply（可嵌套）。
// 用途：元素类型不统一的复杂结构，例如 GEOPOS 返回 [经度,纬度] 的嵌套数组。
type MultiRawReply struct {
	Replies []redis.Reply
}

// MakeMultiRawReply creates MultiRawReply
func MakeMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Status Reply ---- */

// StatusReply —— 命名含义：状态回复（RESP 的 '+' 简单字符串）。
// 用途：非二进制安全的一句状态，如 "+Background saving started"。
type StatusReply struct {
	Status string
}

// MakeStatusReply —— 命名含义：构造函数，创建 StatusReply。
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IsOKReply returns true if the given protocol is +OK
func IsOKReply(reply redis.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

/* ---- Int Reply ---- */

// IntReply —— 命名含义：整数回复（RESP 的 ':' 类型）。
// 用途：INCR、DEL 返回的计数等。
type IntReply struct {
	Code int64
}

// MakeIntReply creates int protocol
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply —— 命名含义：既是一个 Go error，又是一个 redis.Reply。
// 用途：命令出错时既能当错误返回给上层逻辑，又能序列化成 '-' 开头的响应发给客户端。
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply —— 命名含义：标准错误回复（RESP 的 '-' 类型）。
type StandardErrReply struct {
	Status string
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given protocol is error
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

func Try2ErrorReply(reply redis.Reply) error {
	str := string(reply.ToBytes())
	if len(str) == 0 {
		return errors.New("empty reply")
	}
	if str[0] != '-' {
		return nil
	}
	return errors.New(str[1:])
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}
