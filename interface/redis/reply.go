package redis

// Reply —— 命名含义：Redis 协议里的“回复(Response)”。
// 用途：所有命令的执行结果（+OK、:整数、-错误、$批量串、*数组等）都实现这个接口，
// 通过唯一的 ToBytes() 把结果序列化成 RESP 字节流，统一写回给客户端。
// 设计要点：接口只暴露 ToBytes()，是为了让上层（server / aof / 主从复制 / 集群）
// 完全不关心回复的具体类型，只用“它能变成字节”这一件事。
type Reply interface {
	ToBytes() []byte
}
