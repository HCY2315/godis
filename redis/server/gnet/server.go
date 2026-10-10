/*
 * gnet 网络层：用第三方事件驱动框架 gnet 实现 redis 服务端。
 * 阶段定位：连接接入 + 解析 + 执行 + 回复，但不由 goroutine 驱动，而是由 epoll 事件回调驱动。
 * 核心思想：gnet 内部维护 N 个 event-loop（每核一个 epoll），收到事件后回调你重写的四个方法：
 *   OnBoot（引擎就绪）/ OnOpen（新连接）/ OnTraffic（有数据）/ OnClose（断开）。
 * 它自己完全不碰 epoll，网络多路复用全在 gnet 内部完成。
 * 开关：配置 use-gnet yes 时 main.go 才会走到这里。
 */
package gnet

import (
	"context"
	"sync/atomic"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/panjf2000/gnet/v2"
)

// GnetServer —— 命名含义：基于 gnet 的服务端。
// 用途：内嵌 gnet.BuiltinEventEngine（提供四个回调的空实现，只重写需要的），
// 并持有存储引擎 db 与在线连接计数。
type GnetServer struct {
	gnet.BuiltinEventEngine             // 事件引擎的默认实现（空方法），便于只覆盖部分回调
	eng                     gnet.Engine // 引擎句柄，用于关闭
	connected               int32       // 在线连接数（原子操作）
	db                      database.DB // 存储引擎，与 std 版共用
}

// NewGnetServer —— 用途：仅注入 db，真正的监听在 Run 里。
func NewGnetServer(db database.DB) *GnetServer {
	return &GnetServer{
		db: db,
	}
}

// Run —— 用途：启动引擎并开始监听。
// WithMulticore(true) 表示开启多 event-loop（每个 CPU 核一个 epoll）。
func (s *GnetServer) Run(listenAddr string) error {
	return gnet.Run(s, "tcp://"+listenAddr, gnet.WithMulticore(true))
}

// OnBoot —— 事件：引擎就绪。保存引擎句柄，供 Close 时停止。
func (s *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	return
}

// OnOpen —— 事件：新连接建立。
// 关键点：gnet.Conn 同时实现了 io.Reader 和 net.Conn，
// 所以能直接复用 godis 原来的 connection.NewConn，无需改动命令逻辑。
// c.SetContext 把业务连接对象挂到该连接的上下文槽，供后续回调取回。
func (s *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	client := connection.NewConn(c)
	c.SetContext(client)
	atomic.AddInt32(&s.connected, 1)
	return
}

// OnClose —— 事件：连接断开。取回业务连接通知 db 清理，并减少计数。
func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	atomic.AddInt32(&s.connected, -1)
	conn := c.Context().(redis.Connection)
	s.db.AfterClientClose(conn)
	return
}

// OnTraffic —— 事件：socket 有数据可读（对应 epoll 的 EPOLLIN）。
// 这是 gnet 版一次请求周期的落点：解析 → 执行 → 写回，全程同步、无 channel。
// 对比 std 版的 Handle：这里没有 for 循环，每批数据触发一次。
func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	// 取回 OnOpen 时挂上的业务连接
	conn := c.Context().(redis.Connection)
	// 【解析阶段】直接从 gnet.Conn 读并解析出命令参数
	cmdLine, err := parser.ParseV2(c)
	if err != nil {
		logger.Infof("parse command line failed: %v", err)
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	// 【执行阶段】交给存储引擎
	result := s.db.Exec(conn, cmdLine)
	// 【回复阶段】写回；gnet 内部会处理写缓冲与可写事件
	buffer := result.ToBytes()
	if len(buffer) > 0 {
		c.Write(buffer)
	}
	return gnet.None
}

// Close —— 用途：停止引擎（关闭所有 event-loop 与连接）。
func (s *GnetServer) Close() {
	s.eng.Stop(context.Background())
}
