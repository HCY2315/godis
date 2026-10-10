/*
 * pubsub 包：发布订阅。
 * Hub 是中枢：维护“频道 -> 订阅者列表”的映射。
 * 订阅(SUBSCRIBE)把连接加入某频道；发布(PUBLISH)时向该频道所有订阅者推送消息。
 * 命名说明：Hub 中枢，subs 订阅关系表（channel -> 订阅者链表）。
 */
package pubsub

import (
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/lock"
)

// Hub —— 命名含义：发布订阅中枢（全局单例，挂在 Server 上）。
type Hub struct {
	// subs —— 频道 -> 订阅者链表
	subs dict.Dict
	// subsLocker —— 按频道加锁，保证并发订阅安全
	subsLocker *lock.Locks
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
