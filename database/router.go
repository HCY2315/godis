/*
 * 命令注册表（路由）。
 * 阶段定位：命令执行阶段的“入口分派”——按命令名找到对应的执行函数。
 * 命名说明：
 *   command      —— 一条命令的元信息（执行函数、键提取函数、回滚函数、参数个数等）
 *   cmdTable     —— 命令名 -> *command 的全局表，各文件用 init() 注册
 *   ExecFunc     —— 真正干活的函数，签名 func(db, args) Reply
 *   PreFunc      —— 执行前分析命令涉及哪些 key（用于加锁 / 集群路由）
 *   UndoFunc     —— 为事务回滚生成逆操作
 *   arity        —— 参数个数约束，负数表示“至少”
 */
package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strings"
)

// cmdTable —— 命名含义：command table，命令查找表（全局单例）。
var cmdTable = make(map[string]*command)

// command —— 命名含义：一条命令的注册信息。
type command struct {
	name     string   // 命令名（小写）
	executor ExecFunc // 执行函数
	// prepare 返回该命令涉及的写 key 和读 key（用于加锁、集群路由）
	prepare PreFunc
	// undo 生成撤销日志，供事务回滚使用
	undo UndoFunc
	// arity 允许的参数个数；arity<0 表示 len(args) >= -arity。
	// 例如 get 的 arity=2，mget 的 arity=-2
	arity int
	flags int
	extra *commandExtra
}

// commandExtra —— 命名含义：命令的额外元信息（面向 COMMAND GETKEYS 等自省命令）。
type commandExtra struct {
	signs    []string // 命令标志（readonly/write/...，字符串形式）
	firstKey int      // 第一个 key 的位置
	lastKey  int      // 最后一个 key 的位置
	keyStep  int      // key 的步长
}

const flagWrite = 0

const (
	flagReadOnly = 1 << iota
	flagSpecial            // 命名含义：特殊命令，在主 Server.Exec 里直接处理，不走 DB.execNormalCommand
)

// registerCommand —— 用途：注册一条“普通命令”（只读写有限个 key 的命令）。
// 各命令文件（string.go/list.go/...）在各自的 init() 里调用它。
func registerCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}

// registerSpecialCommand registers a special command, such as publish, select, keys, flushAll
func registerSpecialCommand(name string, arity int, flags int) *command {
	name = strings.ToLower(name)
	flags |= flagSpecial
	cmd := &command{
		name:  name,
		arity: arity,
		flags: flags,
	}
	cmdTable[name] = cmd
	return cmd
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagReadOnly > 0
}

func (cmd *command) toDescReply() redis.Reply {
	args := make([]redis.Reply, 0, 6)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(cmd.extra.firstKey)),
			protocol.MakeIntReply(int64(cmd.extra.lastKey)),
			protocol.MakeIntReply(int64(cmd.extra.keyStep)),
		)
	}
	return protocol.MakeMultiRawReply(args)
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}
