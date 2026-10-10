// Package database is a memory database with redis compatible interface
package database

import (
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

// DB —— 命名含义：单个数据库（redis 有 16 个，用下标区分）。
// 用途：存数据、执行命令、维护过期时间与 WATCH 版本号。
// 三个 dict 都用 ConcurrentDict（分片并发哈希），使不同 key 可并行访问。
type DB struct {
	index int
	// data —— 主数据表：key -> *DataEntity
	data *dict.ConcurrentDict
	// ttlMap —— 过期表：key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict
	// versionMap —— 版本表：key -> version(uint32)，用于 WATCH 的乐观锁
	versionMap *dict.ConcurrentDict

	// addAof —— 命名含义：将命令追加到 AOF 的函数（未开启 AOF 时是空函数）。
	addAof func(CmdLine)

	// 键事件回调：键插入/删除时触发
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB create DB instance only with basic abilities.
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec —— 用途：在“单个数据库”内执行命令。
// 先拦截事务控制命令（MULTI/DISCARD/EXEC/WATCH），若处于 MULTI 中则改为入队，
// 否则走普通命令执行路径。
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// 事务控制命令及其它不能放入事务队列的命令
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	return db.execNormalCommand(cmdLine)
}

// execNormalCommand —— 用途：执行一条普通命令的完整流程。
// 流程：查表 → 校验参数 → 提取读写 key → 更新版本号 → 按 key 加锁 → 执行 → 解锁。
// 【性能关键点】这里的 addVersion + 构造写 key 的 map 锁是每条命令的固定开销。
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0])) // 注意：每条命令一次 ToLower 分配
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	// prepare 解析出该命令要写/读哪些 key
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	// 为 WATCH 机制递增写 key 的版本号
	db.addVersion(write...)
	// 按 key 加读写锁（分片锁，不同 key 可并行，热点 key 会串行）
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	// 真正执行命令
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
// deprecated
// for test only
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

/* --- add version --- */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
