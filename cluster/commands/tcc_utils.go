// tcc_utils.go：多键命令在集群里的“按节点分组”工具，供 TCC 分布式事务使用。
package commands

import (
	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine —— 命令行别名。
type CmdLine = [][]byte

// RouteMap —— 命名含义：路由表，节点 -> 该节点上需要处理的 key 列表。
type RouteMap map[string][]string

func getRouteMap(cluster *core.Cluster, keys []string) RouteMap {
	m := make(RouteMap)
	for _, key := range keys {
		slot := cluster.GetSlot(key)
		node := cluster.PickNode(slot)
		m[node] = append(m[node], key)
	}
	return m
}

type TccTx struct {
	rawCmdLine CmdLine
	routeMap   RouteMap
	cmdLines   map[string]CmdLine // node -> CmdLine
}

// execute tcc
// returns node->result map
func doTcc(cluster *core.Cluster, c redis.Connection, tx *TccTx) (map[string]redis.Reply, protocol.ErrorReply) {
	txId := utils.RandString(6)

	// send prepare request
	for node, cmdLine := range tx.cmdLines {
		prepareCmd := utils.ToCmdLine("prepare", txId)
		prepareCmd = append(prepareCmd, cmdLine...)
		reply := cluster.Relay(node, c, prepareCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txId, tx.routeMap)
			return nil, protocol.MakeErrReply("prepare failed: " + err.Error())
		}
	}

	// send commit request
	commiteCmd := utils.ToCmdLine("commit", txId)
	result := make(map[string]redis.Reply)
	for node := range tx.routeMap {
		reply := cluster.Relay(node, c, commiteCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txId, tx.routeMap)
			return nil, protocol.MakeErrReply("commit failed: " + err.Error())
		}
		result[node] = reply
	}

	return result, nil
}

func requestRollback(cluster *core.Cluster, c redis.Connection, txId string, routeMap RouteMap) {
	rollbackCmd := utils.ToCmdLine("rollback", txId)
	for node := range routeMap {
		cluster.Relay(node, c, rollbackCmd)
	}
}
