/*
 * Raft 一致性协议封装（基于 hashicorp/raft）。
 * 阶段定位：集群的“元数据一致性阶段”。集群不复制用户数据，只用 raft 复制“元数据”：
 * slot 归属、节点列表、迁移事件等，保证所有节点对集群拓扑达成一致。
 * 命名说明：
 *   Node      —— 本节点上的 raft 实例
 *   FSM       —— 状态机（Finite State Machine），即“元数据的当前状态 + 变更应用逻辑”
 *   watcher   —— 监听 leader 变化以触发故障转移
 *   RaftConfig—— raft 运行配置
 */
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// Node —— 命名含义：本节点的 raft 实例及配套设施（日志、快照、传输、状态机）。
type Node struct {
	Cfg           *RaftConfig
	inner         *raft.Raft     // hashicorp/raft 实例
	FSM           *FSM           // 状态机
	logStore      raft.LogStore  // 日志存储（boltdb）
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
	watcher       watcher
}

// watcher —— 命名含义：leader 观察者，用于主节点故障时触发 failover。
type watcher struct {
	watch         func(*FSM)
	currentMaster string
	onFailover    func(newMaster string)
}

// RaftConfig —— 命名含义：raft 配置。
type RaftConfig struct {
	RedisAdvertiseAddr string // 同时用作节点 ID
	RaftListenAddr     string
	RaftAdvertiseAddr  string
	Dir                string
}

func (cfg *RaftConfig) ID() string {
	return cfg.RedisAdvertiseAddr
}

var Leader = raft.Leader
var Follower = raft.Follower
var Candidate = raft.Candidate

func StartNode(cfg *RaftConfig) (*Node, error) {
	if cfg.RaftAdvertiseAddr == "" {
		cfg.RaftAdvertiseAddr = cfg.RaftListenAddr
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(cfg.ID())
	if config.LocalID == "" {
		// cfg.ID() actually is  cfg.RedisAdvertiseAddr
		return nil, errors.New("redis address is required")
	}
	leaderNotifyCh := make(chan bool, 10)
	config.NotifyCh = leaderNotifyCh

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftListenAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	// todo: mkdir if possible
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.Dir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.Dir, "raft.db"),
	})
	if err != nil {
		return nil, err
	}

	storage := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}

	logStore := boltDB
	stableStore := boltDB
	inner, err := raft.NewRaft(config, storage, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	node := &Node{
		Cfg:           cfg,
		inner:         inner,
		FSM:           storage,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		transport:     transport,
	}
	node.setupWatch()
	return node, nil
}

func (node *Node) HasExistingState() (bool, error) {
	return raft.HasExistingState(node.logStore, node.stableStore, node.snapshotStore)
}

// BootstrapCluster creates a raft cluster, and returns after self makes leader
func (node *Node) BootstrapCluster(slotCount int) error {
	future := node.inner.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(node.Cfg.ID()),
				Address: node.transport.LocalAddr(),
			},
		},
	})
	err := future.Error()
	if err != nil {
		return fmt.Errorf("BootstrapCluster failed: %v", err)
	}
	// wait self leader
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		return fmt.Errorf("wait for leader timeout")
	}
	// init fsm
	_, err = node.Propose(&LogEntry{Event: EventSeedStart, InitTask: &InitTask{
		Leader:    node.Cfg.ID(),
		SlotCount: slotCount,
	}})
	return err
}

func (node *Node) Close() error {
	future := node.inner.Shutdown()
	if future.Error() == nil {
		return nil
	}
	return fmt.Errorf("raft shutdown %v", future.Error())
}

// AddToRaft handles join request, node must be leader
func (node *Node) AddToRaft(redisAddr, raftAddr string) error {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}
	id := raft.ServerID(redisAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == id {
			return errors.New("already in cluster")
		}
	}
	future := node.inner.AddVoter(id, raft.ServerAddress(raftAddr), 0, 0)
	return future.Error()
}

func (node *Node) HandleEvict(redisAddr string) error {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}
	id := raft.ServerID(redisAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == id {
			err := node.inner.RemoveServer(srv.ID, 0, 0).Error()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (node *Node) Propose(event *LogEntry) (uint64, error) {
	bin, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("marshal event failed: %v", err)
	}
	future := node.inner.Apply(bin, 0)
	err = future.Error()
	if err != nil {
		return 0, fmt.Errorf("raft propose failed: %v", err)
	}
	return future.Index(), nil
}

// setupWatch will be called after any changes of FSM within lock.
func (node *Node) setupWatch() {
	node.watcher.watch = func(f *FSM) {
		newMaster := f.SlaveMasters[node.Self()]
		if newMaster != node.watcher.currentMaster {
			node.watcher.currentMaster = newMaster
			if node.watcher.onFailover != nil {
				node.watcher.onFailover(newMaster)
			}
		}
	}
}

// SetOnFailover sets onFailover callback
// After a failover, onFailover will receive the new master
func (node *Node) SetOnFailover(fn func(newMaster string)) {
	node.watcher.currentMaster = node.FSM.GetMaster(node.Self())
	node.watcher.onFailover = fn
}
