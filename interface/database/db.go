/*
 * 存储引擎抽象层：隔离“网络/协议”与“数据/命令执行”。
 * 阶段定位：命令执行阶段。无论成 std 还是 gnet 网络层，最终都调用同一个 DB。
 */

package database

import (
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/rdb/core"
)

// CmdLine —— 命名含义：命令行(Command Line)，如 ["set","k","v"]。
// 用途：类型别名（= 而非 type 定义），形容词的 [][]byte 可读性太差，起个语义名。
type CmdLine = [][]byte

// DB —— 命名含义：Database，redis 风格存储引擎。
// 用途：最核心的抽象，网络层只依赖这三个方法即可完成一次请求；
// 单机实现是 database.Server，集群实现是 cluster.Cluster（都实现 DB）。
// 阶段定位：这是“解析之后、回复之前”的执行入口。
type DB interface {
	// Exec 执行一条命令（cmdLine 含命令名和参数），返回可直接写回客户端的 Reply
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	// AfterClientClose 连接关闭后的清理（如退订、事务状态），网络层在断开时调用
	AfterClientClose(c redis.Connection)
	// Close 优雅关闭存储引擎（停复制、落盘等）
	Close()
}

// KeyEventCallback —— 命名含义：键事件回调。
// 用途：键插入/删除时被触发（如 pubsub 通知、集群迁移监听）。注意可能被并发调用。
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

// DBEngine —— 命名含义：DB Engine，在 DB 基础上暴露更多能力的“增强型引擎”。
// 用途：集群、事务(TCC)、RDB 加载等复杂场景需要 ForEach / 键锁 / 撤销日志等能力，
// 单机 Server 实现了它；普通命令路径只依赖上面的小接口 DB。
type DBEngine interface {
	DB
	LoadRDB(dec *core.Decoder) error
	ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply
	ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
	SetKeyInsertedCallback(cb KeyEventCallback)
	SetKeyDeletedCallback(cb KeyEventCallback)
}

// DataEntity —— 命名含义：Data Entity，键对应的“数据实体”。
// 用途：一个键只对应一个 DataEntity，内部 Data 用 interface{} 装具体类型
// （string 是 []byte，list 是 *list.QuickList/...，hash/set/sortedset 各不同），
// 通过运行时类型断言分派到对应的命令实现。
type DataEntity struct {
	Data interface{}
}
