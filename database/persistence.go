/*
 * RDB 持久化与 AOF 挂钩：启动加载 + 命令写入 AOF 的绑定。
 * 阶段定位：持久化阶段。RDB 是“数据快照”，AOF 是“命令日志”，二者互补。
 * 命名说明：loadRdbFile/LoadRDB 负责读快照，bindPersister 把每个 DB 的 addAof 接到 Persister。
 */
package database

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	HashSet "github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
)

// loadRdbFile —— 用途：从配置的 RDB 文件加载数据（启动时调用）。
func (server *Server) loadRdbFile() error {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		return fmt.Errorf("open rdb file failed " + err.Error())
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return fmt.Errorf("load rdb file failed " + err.Error())
	}
	return nil
}

// LoadRDB —— 用途：真正解析 RDB 流并按类型还原成 DataEntity 存入对应库。
func (server *Server) LoadRDB(dec *core.Decoder) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := server.mustSelectDB(o.GetDBIndex())
		var entity *database.DataEntity
		switch o.GetType() {
		case rdb.StringType:
			// Redis STRING：RDB 解码器给出 StringObject.Value（[]byte）
			// godis 直接把字节串存入 DataEntity.Data（Redis 字符串是二进制安全的）
			str := o.(*rdb.StringObject)
			entity = &database.DataEntity{
				Data: str.Value,
			}
		case rdb.ListType:
			// Redis LIST：RDB 还原成 [][]byte（按顺序）
			// godis 用 QuickList（链表+分页切片）来表示列表
			listObj := o.(*rdb.ListObject)
			list := List.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v) // 每个元素是 []byte
			}
			entity = &database.DataEntity{
				Data: list,
			}
		case rdb.HashType:
			// Redis HASH：RDB 给出 map[string][]byte
			// godis 用 dict.MakeSimple() 构造内存字典（单线程恢复，不要并发）
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{
				Data: hash,
			}
		case rdb.SetType:
			// Redis SET：RDB 给出 Members [][]byte（去重）
			// godis 的 Set 底层是 dict（key=成员，value 为空），所以转 string 存入
			setObj := o.(*rdb.SetObject)
			set := HashSet.Make()
			for _, mem := range setObj.Members {
				set.Add(string(mem))
			}
			entity = &database.DataEntity{
				Data: set,
			}
		case rdb.ZSetType:
			// Redis ZSET（有序集合）：每个成员带一个分数 score，按 score 排序
			// RDB 给出 Entries，每项含 Member(string) + Score(float64)
			// godis 的 SortedSet = dict(member->元素, O(1)查分) + skiplist(按分有序, 排名/范围查询)
			zsetObj := o.(*rdb.ZSetObject)
			zSet := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{
				Data: zSet,
			}
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), entity)
			if o.GetExpiration() != nil {
				db.Expire(o.GetKey(), *o.GetExpiration())
			}
			// add to aof
			db.addAof(aof.EntityToCmd(o.GetKey(), entity).Args)
		}
		return true
	})
}

// NewPersister —— 用途：包装 aof.NewPersister，并注入“临时库工厂”。
// 临时库（MakeAuxiliaryServer）在 AOF 重写时用来承载快照数据，所以需要数据库包提供一个构造方式。
func NewPersister(db database.DBEngine, filename string, load bool, fsync string) (*aof.Persister, error) {
	return aof.NewPersister(db, filename, load, fsync, func() database.DBEngine {
		return MakeAuxiliaryServer()
	})
}

// AddAof —— 用途：把命令追加到 AOF（未开启持久化时 persister 为 nil，静默忽略）。
func (server *Server) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

// bindPersister —— 用途：绑定持久化器，并把每个 DB 的 addAof 回调接到 Persister 上。
// 这是“写命令后自动落 AOF”的连接点。
func (server *Server) bindPersister(persister *aof.Persister) {
	server.persister = persister
	// bind SaveCmdLine
	for _, db := range server.dbSet {
		singleDB := db.Load().(*DB)
		singleDB.addAof = func(line CmdLine) {
			if config.Properties.AppendOnly { // config may be changed during runtime
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}
}

// MakeAuxiliaryServer —— 命名含义：辅助 Server，只有基本能力。
// 用途：AOF 重写等场景需要一个临时空库来承载数据，不需要复制/慢日志等完整功能。
func MakeAuxiliaryServer() *Server {
	mdb := &Server{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}
