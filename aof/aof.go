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

// AOF 落盘策略。
const (
	// FsyncAlways —— 每条命令都 fsync，最安全但最慢（性能杀手）。
	FsyncAlways = "always"
	// FsyncEverySec —— 每秒 fsync 一次，性能与安全的折中。
	FsyncEverySec = "everysec"
	// FsyncNo —— 交给操作系统决定何时 fsync。
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
	db         database.DBEngine           // 存储引擎（重写/加载时用）
	tmpDBMaker func() database.DBEngine    // 生成临时库的工厂（重写时用）
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

// NewPersister creates a new aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
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
	// persister.db.Exec may call persister.AddAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync flushes aof file to disk
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

// fsyncEverySecond fsync aof file every second
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
