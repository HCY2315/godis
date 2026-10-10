/*
 * 集群核心实现（Cluster 类型）。
 * 阶段定位：集群模式下的“命令执行阶段”。它同样实现 interface/database.DB，
 * 但执行命令前会先根据 key 算出所属 slot，再路由到负责该 slot 的节点；
 * 若目标就是本节点则落到本地 db，否则通过内部客户端转发给对端。
 * 命名说明：
 *   raftNode        —— 集群元数据的一致性存储（slot 归属、节点列表等走 raft 复制）
 *   slotsManager    —— slot 与节点的映射管理
 *   rebalanceManger —— 负载均衡/迁移管理
 *   transactions    —— 分布式事务(TCC)管理
 *   replicaManager  —— 集群内副本管理
 */
package core

import (
	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/config"
	dbimpl "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	rdbcore "github.com/hdt3213/rdb/core"
)

// Cluster —— 命名含义：集群节点。
type Cluster struct {
	raftNode    *raft.Node        // raft 一致性节点
	db          database.DBEngine // 本地存储引擎（单机 Server）
	connections ConnectionFactory // 节点间连接的工厂
	config      *Config

	slotsManager    *slotsManager       // slot 分配
	rebalanceManger *rebalanceManager   // 迁移/均衡
	transactions    *TransactionManager // 分布式事务
	replicaManager  *replicaManager     // 副本管理

	closeChan chan struct{}

	// 路由实现可注入（测试时替换）；默认见 defaultGetSlotImpl / defaultPickNodeImpl
	getSlotImpl  func(key string) uint32
	pickNodeImpl func(slotID uint32) string
	id_          string // 仅测试用

	// 慢日志
	slogLogger *dbimpl.SlowLogger
}

// Config —— 命名含义：集群启动配置（内嵌 raft 配置 + 加入集群相关参数）。
type Config struct {
	raft.RaftConfig
	StartAsSeed    bool              // 是否作为种子引导新集群
	JoinAddress    string            // 加入集群时联系的seed地址
	Master         string            // 以副本身份加入时指向的主节点
	connectionStub ConnectionFactory // 测试用
	noCron         bool              // 测试用：不启动后台定时任务
}

// SelfID —— 用途：返回本节点 ID（优先取 raft 配置里的）。
func (c *Cluster) SelfID() string {
	if c.raftNode == nil {
		return c.id_
	}
	return c.raftNode.Cfg.ID()
}

// NewCluster —— 用途：创建集群节点：建本地 db、启动 raft、初始化各管理器，
// 然后根据是否已有本地状态决定“引导新集群 / 加入已有集群 / 作为副本加入”。
func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionStub != nil {
		connections = cfg.connectionStub
	} else {
		connections = newDefaultClientFactory()
	}
	db := dbimpl.NewStandaloneServer()
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		raftNode:        raftNode,
		db:              db,
		connections:     connections,
		config:          cfg,
		rebalanceManger: newRebalanceManager(),
		slotsManager:    newSlotsManager(),
		transactions:    newTransactionManager(),
		replicaManager:  newReplicaManager(),
		closeChan:       make(chan struct{}),
	}
	cluster.pickNodeImpl = func(slotID uint32) string {
		return defaultPickNodeImpl(cluster, slotID)
	}
	cluster.getSlotImpl = func(key string) uint32 {
		return defaultGetSlotImpl(cluster, key)
	}
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	cluster.registerOnFailover()

	// setup
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		if cfg.StartAsSeed {
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// join cluster
			conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
			if err != nil {
				return nil, err
			}
			defer connections.ReturnPeerClient(conn)
			joinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
			if cfg.Master != "" {
				joinCmdLine = append(joinCmdLine, []byte(cfg.Master))
			}
			logger.Infof("send join cluster request to %s", cfg.JoinAddress)
			result := conn.Send(joinCmdLine)
			if err := protocol.Try2ErrorReply(result); err != nil {
				return nil, err
			}
		}
	} else {
		masterAddr := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
		if masterAddr != "" {
			err := cluster.SlaveOf(masterAddr)
			if err != nil {
				panic(err)
			}
		}
	}

	// record slow log
	cluster.slogLogger = dbimpl.NewSlowLogger(config.Properties.SlowLogMaxLen, config.Properties.SlowLogSlowerThan)

	go cluster.clusterCron()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) Close() {
	close(cluster.closeChan)
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		panic(err)
	}
}

// LoadRDB real implementation of loading rdb file
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}
