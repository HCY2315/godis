/*
 * 集群入口。
 * 特点：这是一个“服务端集群”，对客户端透明——连任意节点都能访问全集群的数据。
 * 阶段定位：集群模式下的启动阶段（main.go 里 cluster-enable 打开时走这里）。
 * 组成：底层的分布式一致由 raft 提供（只复制元数据，不复制用户数据）；数据按 1024 个 slot 分片（见 core/utils.go 的 SlotCount，注意 Redis 官方是 16384）；节点间用内部命令通信。
 * 命名说明：Cluster 是 core.Cluster 的别名，MakeCluster 是构造+启动函数。
 */
package cluster

import (
	"os"
	"path"

	_ "github.com/hdt3213/godis/cluster/commands" // register commands
	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
)

type Cluster = core.Cluster

// MakeCluster —— 用途：用当前配置创建并启动一个集群节点。
// 若没有本地状态：作为种子节点则引导全新集群，否则向种子节点发起加入请求。
func MakeCluster() *Cluster {
	raftPath := path.Join(config.Properties.Dir, "raft")
	err := os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	cluster, err := core.NewCluster(&core.Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: config.Properties.AnnounceAddress(),
			RaftListenAddr:     config.Properties.RaftListenAddr,
			RaftAdvertiseAddr:  config.Properties.RaftAnnounceAddress(),
			Dir:                raftPath,
		},
		StartAsSeed: config.Properties.ClusterAsSeed,
		JoinAddress: config.Properties.ClusterSeed,
		Master:      config.Properties.MasterInCluster,
	})
	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}
	return cluster
}
