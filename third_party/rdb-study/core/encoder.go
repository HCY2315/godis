package core

import (
	"encoding/binary"
	"fmt"
	"github.com/hdt3213/rdb/crc64jones"
	"hash"
	"io"
)

// Encoder 用于「生成」RDB 文件（与 Decoder 方向相反）。
//
// 它必须严格按 RDB 格式、且按固定顺序写入，否则 Redis 无法加载：
//
//	WriteHeader → [WriteAux...] → WriteDBHeader → [对象...] → WriteEnd
//
// 内部用一套「状态机」约束调用顺序，写错顺序会直接报错。
//
// 典型用法：
//
//	f, _ := os.Create("dump.rdb")
//	enc := core.NewEncoder(f)
//	enc.WriteHeader()
//	enc.WriteDBHeader(0, 1, 0)          // db0, 1 个 key, 0 个带 ttl
//	enc.WriteStringObject("k", []byte("v"))
//	enc.WriteEnd()
type Encoder struct {
	writer   io.Writer         // 输出目标（文件/网络等）
	buffer   []byte            // 复用的 8 字节暂存区，写定长整数时用
	crc      hash.Hash64       // CRC64 累加器：每次 write 都同步更新，最后写入文件尾
	existDB  map[uint]struct{} // 已写过的 db 编号集合，防止同一个 db 写两次
	compress bool              // 是否启用 LZF 压缩（注意：作者标注「有未修复的 bug，别开」）
	state    string            // 当前状态机状态

	// 三种紧凑编码的取舍参数：当「单个值长度」和「元素个数」都没超过阈值时，
	// 才用小体积的 ziplist 编码；否则退回普通编码。对应 Redis 配置项 list-max-ziplist-* 等。
	listZipListOpt  *zipListOpt
	hashZipListOpt  *zipListOpt
	zsetZipListOpt  *zipListOpt
	listZipListSize int
}

// zipListOpt 是「是否使用 ziplist 紧凑编码」的判定阈值。
type zipListOpt struct {
	maxValue   int // 任一元素长度超过该值 → 放弃 ziplist 编码
	maxEntries int // 元素个数超过该值 → 放弃 ziplist 编码
}

const (
	defaultZipListMaxValue   = 64  // 默认单值上限 64 字节
	defaultZipListMaxEntries = 512 // 默认元素个数上限 512
)

// getMaxValue 返回单值长度阈值；未配置（nil 或 0）时取默认值 64。
func (zop *zipListOpt) getMaxValue() int {
	if zop == nil || zop.maxValue == 0 {
		return defaultZipListMaxValue
	}
	return zop.maxValue
}

// getMaxEntries 返回元素个数阈值；未配置（nil 或 0）时取默认值 512。
func (zop *zipListOpt) getMaxEntries() int {
	if zop == nil || zop.maxEntries == 0 {
		return defaultZipListMaxEntries
	}
	return zop.maxEntries
}

// 状态机的各个状态。名称大致对应「上一步写了什么」。
const (
	startState           = "Start"         // 初始：还没写任何东西
	writtenHeaderState   = "WrittenHeader" // 已写文件头
	writtenDBHeaderState = "writtenHeader" // 已写 db 头（切库 + 扩容提示）
	writtenAuxState      = "WrittenAux"    // 已写 aux 元信息
	writtenTTLState      = "WrittenTTL"    // 已写 TTL（下一步必须写对象）
	writtenObjectState   = "WrittenObject" // 已写一个对象
	writtenEndState      = "WritingEnd"    // 已写结束标记
)

var placeholder = struct{}{}

// stateChanges 描述状态机的合法迁移：map[当前状态] → 允许到达的下一状态集合。
// 例如：刚写完 Header 时，只能去写 Aux / DBHeader / End，不能直接写对象。
// 注意 writtenDBHeaderState 下不允许直接写 End（否则会产生一个「空 db」，Redis 不接受）。
var stateChanges = map[string]map[string]struct{}{ // state -> allow next states
	startState: {
		writtenHeaderState: placeholder,
	},
	writtenHeaderState: {
		writtenAuxState:      placeholder,
		writtenDBHeaderState: placeholder,
		writtenEndState:      placeholder,
	},
	writtenAuxState: {
		writtenAuxState:      placeholder,
		writtenDBHeaderState: placeholder,
		writtenEndState:      placeholder,
	},
	writtenDBHeaderState: { // do not allow empty db
		writtenTTLState:    placeholder,
		writtenObjectState: placeholder,
	},
	writtenTTLState: {
		writtenObjectState: placeholder,
	},
	writtenObjectState: {
		writtenTTLState:      placeholder,
		writtenObjectState:   placeholder,
		writtenDBHeaderState: placeholder, // start another db
		writtenEndState:      placeholder,
	},
	writtenEndState: {},
}

// NewEncoder 创建一个 RDB 编码器，初始状态为 startState（必须先写 Header）。
func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		writer:          writer,
		crc:             crc64jones.New(),
		buffer:          make([]byte, 8),
		state:           startState,
		existDB:         make(map[uint]struct{}),
		listZipListSize: 4 * 1024,
	}
}

// SetListZipListOpt 设置 list 使用 ziplist 编码的阈值（对应 Redis 的
// list-max-ziplist-value / list-max-ziplist-entries）。
// 返回 *Encoder 以支持链式调用。
func (enc *Encoder) SetListZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.listZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// SetHashZipListOpt 设置 hash 使用 ziplist 编码的阈值（对应 hash-max-ziplist-*）。
func (enc *Encoder) SetHashZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.hashZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// SetZSetZipListOpt 设置 zset 使用 ziplist 编码的阈值（对应 zset-max-ziplist-*）。
func (enc *Encoder) SetZSetZipListOpt(maxValue, maxEntries int) *Encoder {
	enc.zsetZipListOpt = &zipListOpt{
		maxValue:   maxValue,
		maxEntries: maxEntries,
	}
	return enc
}

// EnableCompress 开启 LZF 压缩。
// ⚠️ 作者注释「remain unfixed bugs, don't open」——存在未修复的 bug，生产环境不要开启。
func (enc *Encoder) EnableCompress() *Encoder {
	enc.compress = true
	return enc
}

// write 是所有输出最终经过的唯一入口：
// 它把数据写到 writer，同时把同样的数据喂给 CRC64 累加器。
// 这样最后 WriteEnd 时就能把校验和写进文件尾部，保证前后一致。
func (enc *Encoder) write(p []byte) error {
	_, err := enc.writer.Write(p)
	if err != nil {
		return fmt.Errorf("write data failed: %v", err)
	}
	_, err = enc.crc.Write(p)
	if err != nil {
		return fmt.Errorf("update crc table failed: %v", err)
	}
	return nil
}

// rdbHeader 是固定文件头："REDIS" + 版本号 "0011"（即 RDB version 11）。
var rdbHeader = []byte("REDIS0011")

// validateStateChange 判断从当前状态能否合法迁移到 toState。
func (enc *Encoder) validateStateChange(toState string) bool {
	_, ok := stateChanges[enc.state][toState]
	return ok
}

// WriteHeader 写入 RDB 文件头（"REDIS0011"）。必须是第一个调用。
func (enc *Encoder) WriteHeader() error {
	if !enc.validateStateChange(writtenHeaderState) {
		return fmt.Errorf("cannot writing header at state: %s", enc.state)
	}
	err := enc.write(rdbHeader)
	if err != nil {
		return err
	}
	enc.state = writtenHeaderState
	return nil
}

// WriteAux 写入一条 aux 元信息键值对（如 redis-ver、redis-bits、aof-preamble）。
// 可调用多次；必须在写 DBHeader 之前（状态机允许 Header→Aux 或 Aux→Aux）。
func (enc *Encoder) WriteAux(key, value string) error {
	if !enc.validateStateChange(writtenAuxState) {
		return fmt.Errorf("cannot writing aux at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeAux})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeString(value)
	if err != nil {
		return err
	}
	enc.state = writtenAuxState
	return nil
}

// WriteDBHeader 写入「数据库头」：切库操作码 + db 编号，以及扩容提示操作码 + key 数 + ttl 数。
//
// 参数：
//
//	dbIndex  : 数据库编号（0~15 之类）
//	keyCount : 该 db 中 key 的总数（仅作提示，用于 Redis 预分配哈希表）
//	ttlCount : 该 db 中带过期时间的 key 数（同样只是提示）
//
// 每个 db 只能写一次（重复会报错）；且写完 db 头后不能直接写 End（不允许空 db）。
func (enc *Encoder) WriteDBHeader(dbIndex uint, keyCount, ttlCount uint64) error {
	if !enc.validateStateChange(writtenDBHeaderState) {
		return fmt.Errorf("cannot writing db header at state: %s", enc.state)
	}
	if _, ok := enc.existDB[dbIndex]; ok {
		return fmt.Errorf("db %d existed", dbIndex)
	}
	enc.existDB[dbIndex] = struct{}{}
	err := enc.write([]byte{opCodeSelectDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(dbIndex))
	if err != nil {
		return err
	}
	err = enc.write([]byte{opCodeResizeDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(keyCount)
	if err != nil {
		return err
	}
	err = enc.writeLength(ttlCount)
	if err != nil {
		return err
	}
	enc.state = writtenDBHeaderState
	return nil
}

// WriteEnd 写入结束标记：EOF 操作码(0xFF) + 8 字节 CRC64 校验和 + 一个换行符(0x0A)。
//
// 注意 CRC 校验和是「前面所有写入内容」的 CRC64 累加结果，所以必须最后写；
// 写好后再补一个 LF，这是 RDB 文件格式的约定。
func (enc *Encoder) WriteEnd() error {
	if !enc.validateStateChange(writtenEndState) {
		return fmt.Errorf("cannot writing end at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeEOF})
	if err != nil {
		return err
	}
	checkSum := enc.crc.Sum(nil)
	_, err = enc.writer.Write(checkSum)
	if err != nil {
		return fmt.Errorf("write crc sum failed: %v", err)
	}
	enc.writer.Write([]byte{0x0a}) // write LF
	enc.state = writtenEndState
	return nil
}

// writeTTL 写入过期时间：先写「毫秒级过期」操作码，再写 8 字节小端时间戳。
// 它必须紧邻在「被设置过期时间的那个对象」之前写出。
func (enc *Encoder) writeTTL(expiration uint64) error {
	if !enc.validateStateChange(writtenTTLState) {
		return fmt.Errorf("cannot write string object at state: %s", enc.state)
	}
	err := enc.write([]byte{opCodeExpireTimeMs})
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(enc.buffer, expiration)
	err = enc.write(enc.buffer)
	if err != nil {
		return err
	}
	enc.state = writtenTTLState
	return nil
}

// TTLOption 表示「给对象设置过期时间」的可选参数（毫秒级 Unix 时间戳）。
type TTLOption uint64

// WithTTL 构造一个 TTL 选项，用法：
//
//	enc.WriteStringObject("k", []byte("v"), core.WithTTL(uint64(time.Now().Add(time.Hour).UnixMilli())))
func WithTTL(expirationMs uint64) TTLOption {
	return TTLOption(expirationMs)
}

// beforeWriteObject 是所有 WriteXxxObject 的前置钩子：
// 先校验状态机（当前是否允许写对象），再依次应用可选参数（如 TTL）。
// 这样各种对象写入函数开头都能复用同样的「顺序校验 + TTL 写入」逻辑。
func (enc *Encoder) beforeWriteObject(options ...interface{}) error {
	if !enc.validateStateChange(writtenObjectState) {
		return fmt.Errorf("cannot write object at state: %s", enc.state)
	}
	for _, opt := range options {
		switch o := opt.(type) {
		case TTLOption:
			err := enc.writeTTL(uint64(o))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
