/*
 * AOF（Append Only File）持久化。
 * 阶段定位：持久化阶段。每执行一条写命令，就把命令本身追加到 AOF 文件；
 * 重启时重放这些命令即可恢复数据。
 * 命名说明：
 *   Persister —— 持久化器，负责把命令写入文件
 *   payload   —— 投递给写盘 goroutine 的“一条待落盘命令”
 *   Listener  —— 监听者，命令落盘后可转发给从节点等
 * 三点核心机制：aofChan 异步队列、pausingAof 暂停锁（供重写用）、支持 always/everysec 两种 fsync。
 */
package aof

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"

	"github.com/hdt3213/godis/config"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20
)

/*
 * AOF 落盘策略（对应配置 appendfsync）。
 * 背景：file.Write() 只是写进操作系统的页缓存(内存)，不等于落盘；file.Sync()(即 fsync)
 * 才强制刷到物理磁盘。区别在于何时调用这个较慢的 fsync：
 *   always   —— 每条写命令后都 fsync（同步）。进程崩、断电都不丢，但每条命令都要等磁盘，最慢。
 *   everysec —— 后台每秒 fsync 一次。性能接近不刷盘，崩溃最多丢约 1 秒的写命令（推荐默认）。
 *   no       —— 从不主动 fsync，交给 OS 决定。最快，但断电可能丢较多数据。
 * 注意：进程崩溃时三种都不会丢（数据还在 OS 缓存）；fsync 主要防的是断电/系统崩溃。
 */
const (
	// FsyncAlways —— 每条命令都 fsync：最安全，但受磁盘 IOPS 限制，是性能杀手。
	FsyncAlways = "always"
	// FsyncEverySec —— 每秒 fsync 一次：性能与安全的折中（Redis 默认）。
	FsyncEverySec = "everysec"
	// FsyncNo —— 交给操作系统决定何时 fsync：最快，最不安全。
	FsyncNo = "no"
)

// payload —— 命名含义：一条待写入 AOF 的命令载荷。
type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister —— 命名含义：AOF 持久化器。
// 用途：接收命令（SaveCmdLine）→ 入 aofChan 队列 → 后台 goroutine 串行写文件。
// 关键字段说明见下方注释。
type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.DBEngine        // 存储引擎（重写/加载时用）
	tmpDBMaker func() database.DBEngine // 生成临时库的工厂（重写时用）
	//1<<20	1,048,576	1 Mi ≈ 100 万
	// aofChan —— 命名含义：接收待落盘命令的通道（listenCmd 从这里读）。缓冲极大(1<<20)
	aofChan chan *payload
	// aofFile —— AOF 文件句柄
	aofFile *os.File
	// aofFilename —— AOF 文件路径
	aofFilename string
	// aofFsync —— fsync 策略（always/everysec/no）
	aofFsync string
	// aofFinished —— 关闭时用来等待写盘 goroutine 处理完剩余命令
	aofFinished chan struct{}
	// pausingAof —— 重写 AOF 期间暂停正常写入用的锁
	pausingAof sync.Mutex
	currentDB  int
	// listeners —— 命令落盘后的回调方（如主从复制的 master 向 slave 转发）
	listeners map[Listener]struct{}
	// buffer —— 复用的命令缓冲，减少分配
	buffer []CmdLine
}

// NewPersister —— 用途：创建一个 AOF 持久化器，是启用 AOF 的入口。
// 参数：
//
//	db          存储引擎（加载时重放命令、重写时用）
//	filename    AOF 文件路径
//	load        true=构造时立即重放已有 AOF 恢复数据（启动时用 true）
//	fsync       落盘策略：always/everysec/no
//	tmpDBMaker  生成临时库的工厂（AOF 重写时用）
//
// 流程：先（可选）LoadAof 重放旧命令 → 以追加方式打开文件 → 起后台 goroutine 串行写盘 →
// 若是 everysec 策略再起一个每秒 fsync 的定时任务。
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// 【加载阶段】重放已有 AOF：把文件里的命令逐条 db.Exec 一遍，恢复全部数据
	if load {
		persister.LoadAof(0)
	}
	// 以“追加 + 创建 + 读写”方式打开文件（不存在则新建）
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize) // 写命令的异步队列
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// 【写入阶段】后台 goroutine：从 aofChan 取命令并串行写入文件（见 listenCmd/writeAof）
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener removes a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine —— 用途：把一条命令交给 AOF 处理。
// always 策略：直接同步写（调用方阻塞）；其它策略：投进 channel 由后台 goroutine 写。
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// 加载 AOF 期间 aofChan 被临时置 nil，避免加载的命令又写回文件
	if persister.aofChan == nil {
		return
	}

	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p) // 同步写
		return
	}

	// 异步：交给 listenCmd goroutine 串行写
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

// listenCmd —— 用途：后台 goroutine，从 channel 取命令并写盘，直到 channel 关闭。
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// writeAof —— 用途：真正把一条命令写入 AOF 文件。
// 写前若库号变化，先补写一条 SELECT 命令；写后触发 listeners 并视策略 fsync。
func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // 复用底层数组，减少分配
	persister.pausingAof.Lock()             // 阻止其它 goroutine 在此期间暂停/重写 AOF
	defer persister.pausingAof.Unlock()
	// 保证 AOF 文件里当前库号正确
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof —— 用途：启动时读取 AOF 文件并重放命令来恢复数据。
// 只能在 listenCmd 启动前调用。文件开头可能是一段 RDB（混合持久化），先按 RDB 解析，
// 失败则从头按命令流解析。
func (persister *Persister) LoadAof(maxBytes int) {
	// 【步骤1】临时把 aofChan 置 nil：
	// 重放命令会调用 persister.db.Exec，而 Exec 又可能触发 db.addAof -> SaveCmdLine；
	// 若 aofChan 还在，就会把“正在加载的命令”又写回 AOF 造成重复。置 nil 后 SaveCmdLine 直接返回。
	// defer 保证函数结束时恢复该 channel（此后进入正常写盘阶段）。
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	// 【步骤2】以只读方式打开 AOF 文件
	file, err := os.Open(persister.aofFilename)
	if err != nil {
		// 首次启动文件不存在(PathError)：没有历史数据，直接返回
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// 【步骤3】先尝试把文件开头当作 RDB 解析（混合持久化的 RDB 前缀）
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// 不是 RDB 前缀（纯命令流）：把读指针拨回文件开头，从头按命令流解析
		file.Seek(0, io.SeekStart)
	} else {
		// 确实是 RDB 前缀：跳过已消费的 RDB 部分，从后面的命令流继续
		// +1 是跳过 RDB 结尾的分隔字节；并相应减少本次允许读取的字节数
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}

	// 【步骤4】确定命令流的读取范围：
	// maxBytes>0 表示只加载文件前 maxBytes 字节（AOF 重写增量场景）；否则读整个文件
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	// 【步骤5】把 reader 解析成一条条命令（channel 方式异步产出）
	ch := parser.ParseStream(reader)
	// fakeConn：重放时只需要用它承载“当前选中哪个库(SELECT)”，不是真实客户端连接
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	// 【步骤6】逐条取出命令并重放
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF { // 正常读完，结束
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 命令必须是多批量协议
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		// 【步骤6.1】真正重放：交给存储引擎执行这条命令，效果等同于当初执行它
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		// 【步骤6.2】维护当前库号：文件里用 SELECT 命令切换库，这里同步记录，
		// 因为后续继续写 AOF 时需要知道从哪个库接着写
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync —— 用途：把 AOF 文件从 OS 页缓存刷到物理磁盘（everysec/no 策略会调用它）。
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister == nil {
		return
	}
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond —— 用途：everysec 策略的定时器，每秒 fsync 一次。
// 对性能影响很小（一次/秒），代价是崩溃时最多丢约 1 秒的写命令。
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	// rewrite aof tmpFile
	tmpFile := ctx.tmpFile
	// load aof tmpFile
	tmpAof := persister.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
