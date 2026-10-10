/*
 * 各类错误回复（RESP 的 '-' 类型），都实现 ErrorReply（Error + ToBytes）。
 * 命名规律：XxxErrReply 表示某类错误，MakeXxxErrReply 是构造函数。
 * 注意：像 unknownErrBytes/syntaxErrBytes 这样的字节切片是包级共享的常量，
 * 复用以避免每次分配（这也是后续做性能优化时的方向之一）。
 */
package protocol

// UnknownErrReply —— 命名含义：未知错误回复。命令执行 panic 等兜底时使用。
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

// ToBytes marshals redis.Reply
func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func (r *UnknownErrReply) Error() string {
	return "Err unknown"
}

// ArgNumErrReply —— 命名含义：参数个数错误。Cmd 保存出错的命令名。
type ArgNumErrReply struct {
	Cmd string
}

// ToBytes marshals redis.Reply
func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

// MakeArgNumErrReply represents wrong number of arguments for command
func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// SyntaxErrReply —— 命名含义：语法错误（参数意外/不合法）。
type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

// MakeSyntaxErrReply creates syntax error
func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

// ToBytes marshals redis.Reply
func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

// WrongTypeErrReply —— 命名含义：类型不匹配错误（对错误类型执行操作，如对 list 用 GET）。
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// ProtocolErr

// ProtocolErrReply —— 命名含义：协议解析错误（请求字节不符合 RESP 格式）。
type ProtocolErrReply struct {
	Msg string
}

// ToBytes marshals redis.Reply
func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error '" + r.Msg + "' command"
}
