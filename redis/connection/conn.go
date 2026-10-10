/*
 * connection.Connection —— interface/redis.Connection 的具体实现。
 * 阶段定位：贯穿“接入 → 解析 → 执行 → 回复”。
 * 设计要点：std 与 gnet 两个网络层共用这一个类型——
 *   - std 传入 net.Conn；
 *   - gnet 传入 gnet.Conn（它同样实现了 net.Conn）。
 * 因此换网络库不影响命令逻辑。
 */
package connection

import (
	"net"
	"sync"
	"time"

	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
)

// 用位标志表示连接状态，节省空间。1<<iota 让每个常量占一个二进制位。
const (
	// flagSlave —— 命名含义：该连接对端是一个 slave 节点。
	flagSlave = uint64(1 << iota)
	// flagMaster —— 命名含义：该连接对端是本节点的 master 节点。
	flagMaster
	// flagMulti —— 命名含义：该连接处于 MULTI 事务队列中。
	flagMulti
)

// Connection —— 命名含义：与一个 redis-cli（或 slave/master）的连接。
// 用途：把“底层网络连接 conn”和“本次会话的所有状态”打包在一起。
type Connection struct {
	conn net.Conn // 底层网络连接（std 为 *net.TCPConn，gnet 为 gnet.Conn）

	// sendingData —— 用途：优雅关闭时等待“正在发送的数据”发送完，避免半包。
	sendingData wait.Wait

	// mu —— 用途：写响应/改订阅时的互斥锁；flags 是上面三个位标志的组合。
	mu    sync.Mutex
	flags uint64

	// subs —— 命名含义：subscriptions，该连接订阅的频道集合。
	subs map[string]bool

	// password —— 认证密码，存连接上是因为 CONFIG 命令运行时可改。
	password string

	// 事务(MULTI)相关：入队命令、WATCH 的键及版本号、入队期语法错误。
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// selectedDB —— 当前选中的库（SELECT 命令修改）。
	selectedDB int
}

// connPool —— 命名含义：连接对象池。
// 用途：用 sync.Pool 复用 Connection 对象，连接关闭时不真正释放，降低 GC 压力。
var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// Close —— 用途：断开连接。先等待未发完的数据（最多 10s），再重置会话状态并归还对象池。
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	if c.conn != nil { // 测试时可能是 fake conn
		_ = c.conn.Close()
	}
	// 重置全部会话状态，保证对象被复用时是干净的
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c) // 归还对象池
	return nil
}

// NewConn —— 用途：从对象池取一个 Connection 并绑定底层 net.Conn。
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}


// Write —— 用途：把响应写回客户端。用 sendingData 计数标记“正在发送”，
// 让 Close 能等待发送完成，避免客户端收到半截响应。
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

// SetMaster marks c as a connection with master
func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
