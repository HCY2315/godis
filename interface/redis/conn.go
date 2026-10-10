/*
 * Connection —— 命名含义：与一个 redis 客户端之间的“连接”。
 * 用途：命令处理过程中，凡是“属于这条连接”的状态都挂在这里：
 *   - 输出：Write；身份：密码认证、主/从标记；
 *   - 会话：订阅的频道、事务(MULTI)队列与 WATCH、当前选中的 DB。
 * 设计要点：它把“网络连接”和“会话状态”合并，网络层（std/gnet）都复用同一实现，
 * 因此换网络库时命令逻辑一行不用改。
 * 阶段定位：贯穿“接入 → 解析 → 执行 → 回复”全程。
 */
type Connection interface {
	// Write 把响应字节写回客户端
	Write([]byte) (int, error)
	// Close 关闭连接
	Close() error
	// RemoteAddr 客户端地址（也用作连接名、慢日志里的来源标识）
	RemoteAddr() string

	// —— 认证相关 ——
	// SetPassword/GetPassword：AUTH 所需的密码，运行时可能被 CONFIG 改动所以存在连接上

	// —— 发布订阅 ——
	// Subscribe/UnSubscribe/SubsCount/GetChannels：该连接订阅了哪些频道

	// —— 事务(MULTI/EXEC/WATCH) ——
	// InMultiState/SetMultiState：是否处于 MULTI 队列中；
	// GetQueuedCmdLine/EnqueueCmd/ClearQueuedCmds：队列中的命令；
	// GetWatching：WATCH 的键及版本号；AddTxError/GetTxErrors：入队期间的语法错误

	// —— 选库 ——
	// GetDBIndex/SelectDB：当前选中第几号库

	// —— 主从角色 ——
	// SetSlave/IsSlave、SetMaster/IsMaster：标记这条连接的对端是 slave/master

	// Name 连接名（默认用远程地址）
	Name() string

	SetPassword(string)
	GetPassword() string

	// client should keep its subscribing channels
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
	AddTxError(err error)
	GetTxErrors() []error

	GetDBIndex() int
	SelectDB(int)

	SetSlave()
	IsSlave() bool

	SetMaster()
	IsMaster() bool

	Name() string
}
