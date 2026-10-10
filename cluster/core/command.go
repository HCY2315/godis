/*
 * 集群命令的分派与路由。
 * 阶段定位：集群模式下的命令执行入口。
 * 关键：集群里命令的执行函数签名多了一个 cluster 参数（CmdFunc），
 * 因为需要先算出 key 属于哪个 slot、该 slot 归谁，再决定本地执行还是转发。
 */
package core

import (
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine —— 命令行别名。
type CmdLine = [][]byte

// CmdFunc —— 命名含义：集群命令处理函数（比单机的 ExecFunc 多一个 *Cluster 参数）。
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// commands —— 集群命令表：命令名 -> CmdFunc。由 cluster/commands 包 init() 注册。
var commands = make(map[string]CmdFunc)

// RegisterCmd —— 用途：注册集群命令。
func RegisterCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	commands[name] = cmd
}

// Exec —— 用途：集群版命令执行入口，实现 interface/database.DB。
// 先处理 auth/ping/info 等本地命令，其余按命令名取出 CmdFunc 执行（内部再做 slot 路由）。
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	// Record the start time of command execution
	GodisExecCommandStartUnixTime := time.Now()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if cmdName == "ping" {
		return database.Ping(c, cmdLine[1:])
	}
	if cmdName == "dbsize" {
		dbsize, _ := cluster.db.GetDBSize(0)
		return protocol.MakeIntReply(int64(dbsize))
	}

	if cmdName == "info" {
		if server, ok := cluster.db.(*database.Server); ok {
			return database.Info(server, cmdLine[1:])
		}
	}

	if cmdName == "slowlog" {
		return cluster.slogLogger.HandleSlowlogCommand(cmdLine)
	}

	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	cmdFunc, ok := commands[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	exec := cmdFunc(cluster, c, cmdLine)
	cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.Name())
	return exec

}

// isAuthenticated —— 用途：校验连接是否已通过密码认证。
func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func RegisterDefaultCmd(name string) {
	RegisterCmd(name, DefaultFunc)
}

// DefaultFunc —— 用途：默认路由函数。按 key 算 slot → 找负责节点 →
// 若就是本节点则本地执行，否则通过 Relay 转发给对端。
func DefaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	slotId := cluster.GetSlot(key)
	peer := cluster.PickNode(slotId)
	if peer == cluster.SelfID() {
		// to self db
		//return cluster.db.Exec(c, cmdLine)
		return cluster.db.Exec(c, args)
	}
	return cluster.Relay(peer, c, args)
}
