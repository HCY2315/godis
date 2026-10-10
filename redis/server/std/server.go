/*
 * std 网络层：实现 tcp.Handler，把“redis 协议 + 命令执行”接到通用 tcp 服务器上。
 * 阶段定位：连接接入的后半段 + 解析 + 执行 + 回复，全程由每连接一个 goroutine 驱动。
 * 与 gnet 版的区别：这里用 parser.ParseStream（channel 异步）+ goroutine；
 * gnet 版用事件回调（OnTraffic）+ parser.ParseV2。二者共用 connection/db 层。
 */
package std

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/tcp"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler —— 命名含义：redis 服务(命令)处理器，实现 tcp.Handler。
// 用途：持有存储引擎 db 和全部活跃连接，是 std 版请求处理的中心。
type Handler struct {
	activeConn sync.Map      // 活跃连接集合：*connection.Connection -> 占位符{}（用于关闭时遍历）
	db         idatabase.DB  // 存储引擎（单机或集群），命令最终交给它执行
	closing    atomic.Boolean // 是否正在关闭：关闭后拒绝新连接/新请求
}

// MakeHandler —— 用途：根据配置构造存储引擎，并包装成 Handler。
// 注意：这一步决定“单机 or 集群”，与网络层无关。
func MakeHandler() *Handler {
	var db idatabase.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster() // 集群模式
	} else {
		db = database.NewStandaloneServer() // 单机模式
	}
	return &Handler{
		db: db,
	}
}

// Serve —— 用途：把 Handler 挂到通用 tcp 服务器上并开始监听。
func Serve(addr string, handler *Handler) error {
	return tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: addr,
	}, handler)
}

// closeClient —— 用途：连接关闭时的统一清理：关连接、通知 db、从活跃集合移除。
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle —— 命名含义：处理一条连接（tcp.Handler 接口方法）。
// 用途：这是 std 版一次完整请求周期的落点，由 tcp 层每连接一个 goroutine 调用。
// 流程：包装连接 → 解析出命令 → 执行 → 写回，循环直到连接断开。
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	// 正在关闭：直接拒绝新连接
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	// 把裸 net.Conn 包装成带会话状态的 connection.Connection，并登记为活跃连接
	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	// 【解析阶段】ParseStream 内部起一个 goroutine，把每条解析结果通过 channel 送出来
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			// 连接正常/异常关闭：清理并结束
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// 协议错误：回复错误信息后继续读
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 命令必须是多批量（数组）协议
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		// 【执行阶段】交给存储引擎（单机/集群）
		result := h.db.Exec(client, r.Args)
		// 【回复阶段】把结果序列化写回客户端
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
