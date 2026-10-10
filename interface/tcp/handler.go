package tcp

/*
 * 网络接入层的抽象：把“通用 TCP 服务器”和“具体业务处理”解耦。
 * 阶段定位：连接接入阶段（accept 之后）。
 */

import (
	"context"
	"net"

)

// HandleFunc —— 命名含义：处理单个连接的函数。
// 用途：函数式写法，方便把任意处理逻辑适配成 Handler。
type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler —— 命名含义：连接的“处理器”。
// 用途：tcp 包只负责 accept 和生命周期，真正怎么读/写/执行命令由实现者决定。
// 在 godis 中由 redis/server/std 的 Handler 实现（std 版网络层）。
type Handler interface {
	// Handle 处理一个已完成三次握手的连接，通常内部会阻塞循环读取命令
	Handle(ctx context.Context, conn net.Conn)
	// Close 优雅关闭，负责关闭所有活跃连接
	Close() error
}
