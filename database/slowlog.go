package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SlowLogEntry —— 命名含义：一条慢日志记录。
type SlowLogEntry struct {
	ID        int64     // 自增 ID
	Timestamp time.Time // 执行开始时间
	Duration  int64     // 耗时（微秒）
	Command   [][]byte  // 原始命令
	PeerId    string    // 来源客户端
}

// SlowLogger —— 命名含义：慢查询记录器。
// 用途：命令执行超过 threshold（微秒）就记一条，用环形数组保存最近 maxEntries 条。
// 注意：Record 在每条命令后都会调用（含一次 time.Since），是命令路径的固定开销之一。
type SlowLogger struct {
	mu         sync.RWMutex
	entries    []*SlowLogEntry // 环形数组
	count      int             // 当前条数
	nextIdx    int             // 环形数组写指针
	maxEntries int             // 容量

	threshold   int64    // 慢日志阈值（微秒）
	nextID      int64    // 下一条日志 ID
	logCommands [][]byte // 需要记录日志的命令白名单
}

func NewSlowLogger(maxEntries int, threshold int64) *SlowLogger {
	entries := make([]*SlowLogEntry, maxEntries)
	return &SlowLogger{
		entries:     entries,
		maxEntries:  maxEntries,
		threshold:   threshold,
		nextID:      1,
		logCommands: [][]byte{},
	}
}

// Record —— 用途：记录一次命令耗时；未超阈值则直接返回。
func (sl *SlowLogger) Record(start time.Time, args [][]byte, client string) {
	if sl == nil || len(sl.entries) == 0 {
		return
	}
	duration := time.Since(start)
	micros := duration.Microseconds()

	if micros < sl.threshold {
		return
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := &SlowLogEntry{
		ID:        sl.nextID,
		Timestamp: start,
		Duration:  micros,
		Command:   args,
		PeerId:    client,
	}

	sl.nextID++

	// 插入到环形数组
	sl.entries[sl.nextIdx] = entry
	sl.nextIdx = (sl.nextIdx + 1) % sl.maxEntries

	// 更新条目计数
	if sl.count < sl.maxEntries {
		sl.count++
	}
}

func (sl *SlowLogger) GetEntries(count int) []*SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if count <= 0 || sl.count == 0 {
		return []*SlowLogEntry{}
	}

	// 获取实际返回数量
	if count > sl.count {
		count = sl.count
	}

	result := make([]*SlowLogEntry, 0, count)
	startIdx := (sl.nextIdx - 1 + sl.maxEntries) % sl.maxEntries

	for i := 0; i < count; i++ {
		idx := (startIdx - i + sl.maxEntries) % sl.maxEntries
		result = append(result, sl.entries[idx])
	}

	return result
}

func (sl *SlowLogger) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.count
}

func (sl *SlowLogger) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 重置环形数组状态
	sl.entries = make([]*SlowLogEntry, sl.maxEntries)
	sl.count = 0
	sl.nextIdx = 0
	sl.nextID = 1
}

// HandleSlowlogCommand Process SLOWLOG command
func (sl *SlowLogger) HandleSlowlogCommand(args [][]byte) redis.Reply {
	argsLen := len(args)
	if argsLen <= 1 || argsLen > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'SLOWLOG' command")
	}

	subCmd := strings.ToLower(string(args[1]))

	switch subCmd {
	case "get":
		count := 10
		if argsLen == 3 {
			n, err := strconv.Atoi(string(args[2]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if n < 0 {
				return protocol.MakeEmptyMultiBulkReply()
			}
			count = n
		}
		entries := sl.GetEntries(count)
		return formatSlowlogEntries(entries)
	case "len":
		return protocol.MakeIntReply(int64(sl.Len()))
	case "reset":
		sl.Reset()
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for  '" + subCmd + "'")
	}
}

func formatSlowlogEntries(entries []*SlowLogEntry) redis.Reply {
	result := make([]redis.Reply, 0, len(entries))
	for _, log := range entries {
		logList := make([]redis.Reply, 0)
		logList = append(logList, protocol.MakeIntReply(log.ID),
			protocol.MakeIntReply(log.Timestamp.Unix()),
			protocol.MakeIntReply(int64(log.Duration)),
			protocol.MakeMultiBulkReply(log.Command),
			protocol.MakeBulkReply([]byte(log.PeerId)))
		result = append(result, protocol.MakeMultiRawReply(logList))
	}
	return protocol.MakeMultiRawReply(result)
}
