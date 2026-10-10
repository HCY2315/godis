/*
 * 集群元数据状态机（FSM）。
 * 用途：保存所有节点一致认同的“集群地图”，包括：
 *   - 哪个节点负责哪些 slot（路由的依据）
 *   - 正在进行的迁移任务
 *   - 主从关系、故障转移任务
 * 这些数据通过 raft 日志在各节点间复制；Apply() 在每个节点上重放同样的日志，
 * 从而保证所有节点看到相同的集群拓扑。
 * 迁移期间的请求路由规则见下方英文注释（指向迁移中 slot 的请求会被路由到源节点）。
 */
package raft

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM —— 命名含义：有限状态机（这里存集群元数据）。
type FSM struct {
	mu           sync.RWMutex
	Node2Slot    map[string][]uint32       // 节点 -> 它负责的 slot 列表
	Slot2Node    map[uint32]string         // slot -> 负责节点（路由核心表）
	Migratings   map[string]*MigratingTask // taskId -> 迁移任务
	MasterSlaves map[string]*MasterSlave   // masterId -> 主从关系
	SlaveMasters map[string]string         // slaveId -> masterId
	Failovers    map[string]*FailoverTask  // taskId -> 故障转移任务
	changed      func(*FSM)                // 状态变化回调（持读锁时调用）
}

// MasterSlave —— 命名含义：一个主节点及其从节点列表。
type MasterSlave struct {
	MasterId string
	Slaves   []string
}

// MigratingTask is a running migrating task
// It is immutable
type MigratingTask struct {
	ID         string
	SrcNode    string
	TargetNode string

	// Slots stores slots to migrate in this event
	Slots []uint32
}

// InitTask assigns all slots to seed node while starting a new cluster
// It is designed to init the FSM for a new cluster
type InitTask struct {
	Leader    string
	SlotCount int
}

// FailoverTask represents a failover or joinery/quitting of slaves
type FailoverTask struct {
	ID          string
	OldMasterId string
	NewMasterId string
}

type JoinTask struct {
	NodeId string
	Master string
}

// implements FSM.Apply after you created a new raft event
const (
	EventStartMigrate = iota + 1
	EventFinishMigrate
	EventSeedStart
	EventFinishFailover
	EventJoin
)

// LogEntry is an entry in raft log, stores a change of cluster
type LogEntry struct {
	Event         int
	MigratingTask *MigratingTask `json:"MigratingTask,omitempty"`
	InitTask      *InitTask
	FailoverTask  *FailoverTask
	JoinTask      *JoinTask
}

// Apply —— 用途：raft 日志被多数节点提交后回调，按事件类型更新集群元数据。
// 这里是“所有节点对集群拓扑保持一致”的关键：同一份日志在每个节点重放出同样状态。
// 事件类型：EventStartMigrate/EventFinishMigrate（迁移）、EventSeedStart（初始化）、EventFinishFailover（故障转移）等。
func (fsm *FSM) Apply(log *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	entry := &LogEntry{}
	err := json.Unmarshal(log.Data, entry)
	if err != nil {
		panic(err)
	}

	/// event handler
	if entry.Event == EventStartMigrate {
		task := entry.MigratingTask
		fsm.Migratings[task.ID] = task
	} else if entry.Event == EventFinishMigrate {
		task := entry.MigratingTask
		delete(fsm.Migratings, task.ID)
		fsm.addSlots(task.TargetNode, task.Slots)
		fsm.removeSlots(task.SrcNode, task.Slots)
	} else if entry.Event == EventSeedStart {
		slots := make([]uint32, int(entry.InitTask.SlotCount))
		for i := 0; i < entry.InitTask.SlotCount; i++ {
			fsm.Slot2Node[uint32(i)] = entry.InitTask.Leader
			slots[i] = uint32(i)
		}
		fsm.Node2Slot[entry.InitTask.Leader] = slots
		fsm.addNode(entry.InitTask.Leader, "")
	} else if entry.Event == EventFinishFailover {
		task := entry.FailoverTask
		// change route
		fsm.failover(task.OldMasterId, task.NewMasterId)
		slots := fsm.Node2Slot[task.OldMasterId]
		fsm.addSlots(task.NewMasterId, slots)
		fsm.removeSlots(task.OldMasterId, slots)
		delete(fsm.Failovers, task.ID)
	} else if entry.Event == EventJoin {
		task := entry.JoinTask
		fsm.addNode(task.NodeId, task.Master)
	}
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}

// FSMSnapshot stores necessary data to restore FSM
type FSMSnapshot struct {
	Slot2Node    map[uint32]string // slotID -> nodeID
	Migratings   map[string]*MigratingTask
	MasterSlaves map[string]*MasterSlave
}

func (snapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		_, err = sink.Write(data)
		if err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (snapshot *FSMSnapshot) Release() {}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	slot2Node := make(map[uint32]string)
	for k, v := range fsm.Slot2Node {
		slot2Node[k] = v
	}
	migratings := make(map[string]*MigratingTask)
	for k, v := range fsm.Migratings {
		migratings[k] = v
	}
	masterSlaves := make(map[string]*MasterSlave)
	for k, v := range fsm.MasterSlaves {
		masterSlaves[k] = v
	}
	return &FSMSnapshot{
		Slot2Node:    slot2Node,
		Migratings:   migratings,
		MasterSlaves: masterSlaves,
	}, nil
}

func (fsm *FSM) Restore(src io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	snapshot := &FSMSnapshot{}
	err = json.Unmarshal(data, snapshot)
	if err != nil {
		return err
	}
	fsm.Slot2Node = snapshot.Slot2Node
	fsm.Migratings = snapshot.Migratings
	fsm.MasterSlaves = snapshot.MasterSlaves
	fsm.Node2Slot = make(map[string][]uint32)
	for slot, node := range snapshot.Slot2Node {
		fsm.Node2Slot[node] = append(fsm.Node2Slot[node], slot)
	}
	for master, slaves := range snapshot.MasterSlaves {
		for _, slave := range slaves.Slaves {
			fsm.SlaveMasters[slave] = master
		}
	}
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}
