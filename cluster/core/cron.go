// cron.go：集群后台定时任务。leader 每秒触发一次，负责探测负载并决定是否发起迁移等。
package core

import (
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/cluster/raft"
)

// clusterCron —— 用途：集群周期任务循环；只有 raft leader 会执行实际操作。
func (cluster *Cluster) clusterCron() {
	if cluster.config.noCron {
		return
	}
	ticker := time.NewTicker(time.Second)
	var running int32
	for {
		select {
		case <-ticker.C:
			if cluster.raftNode.State() == raft.Leader {
				if atomic.CompareAndSwapInt32(&running, 0, 1) {
					// Disable parallelism
					go func() {
						cluster.doFailoverCheck()
						cluster.doRebalance()
						atomic.StoreInt32(&running, 0)
					}()
				}
			} else {
				cluster.sendHearbeat()
			}
		case <-cluster.closeChan:
			ticker.Stop()
			return
		}
	}
}
