// Package core 是 RDB 解析/生成的内核实现。
//
// RDB 是 Redis 的持久化快照文件格式，整体结构大致为：
//
//	"REDIS" + 4字节版本号            —— 文件头(magic + version)
//	[aux 字段]                       —— 元信息，如 redis-ver / redis-bits
//	[SELECTDB 操作码 + db号]          —— 切换到某个数据库
//	[RESIZEDB 操作码 + key数 + ttl数] —— 哈希表扩容提示
//	[EXPIRE 操作码 + 过期时间]         —— 可选，紧邻下一个 key
//	[type 字节 + key + value]         —— 一条条键值对
//	... 可重复多个 db ...
//	0xFF(EOF 操作码)                  —— 数据结束
//	8字节 CRC64 校验和                —— 文件完整性校验
//
// 本包把上述字节流解析成 model 包里的对象，并通过回调交给调用方。
package core

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/memprofiler"
	"github.com/hdt3213/rdb/model"
	"io"
	"strconv"
	"time"
)

// Decoder 是 RDB 解析过程的实例。
// 它持有一个带缓冲的字节流读取器，并按 RDB 格式逐个解析出对象。
//
// 典型用法：
//
//	f, _ := os.Open("dump.rdb")
//	defer f.Close()
//	dec := core.NewDecoder(f)
//	err := dec.Parse(func(obj model.RedisObject) bool {
//	    fmt.Println(obj.GetType(), obj.GetKey())
//	    return true // 返回 true 继续解析下一条，false 提前终止
//	})
type Decoder struct {
	input     *bufio.Reader // 带缓冲的字节流；bufio 把「逐字节读」变成「每 4KB 一次底层读」
	readCount int           // 已读取的字节数，用于统计/调试（见 GetReadCount）
	buffer    []byte        // 复用的 8 字节暂存区，避免频繁分配（读定长整数时用）

	withSpecialOpCode bool                            // 是否把 aux/resizedb 等元信息也回调给调用方
	withSpecialTypes  map[string]ModuleTypeHandleFunc // 自定义 Redis Module 类型的解析器注册表
}

// NewDecoder 创建一个 RDB 解码器。
//
// 参数 reader 可以是任意 io.Reader（*os.File、net.Conn、bytes.Reader 等）。
// 内部用 bufio.NewReader 包一层缓冲，从而把大量小读取合并成少量系统调用。
// 注意：此函数不会立即读取任何数据，只是准备好缓冲区和状态；
// 真正的读取发生在随后调用 Parse 时（懒加载）。
func NewDecoder(reader io.Reader) *Decoder {
	parser := new(Decoder)
	parser.input = bufio.NewReader(reader)
	parser.buffer = make([]byte, 8)
	parser.withSpecialTypes = make(map[string]ModuleTypeHandleFunc)
	return parser
}

// WithSpecialOpCode 开启后，解析器会把 aux 元信息和 resizedb 提示也作为对象回调给调用方。
// 默认关闭（这些内容通常与业务数据无关，跳过以提升性能）。
//
// 用法：dec := core.NewDecoder(f).WithSpecialOpCode()
func (dec *Decoder) WithSpecialOpCode() *Decoder {
	dec.withSpecialOpCode = true
	return dec
}

// WithSpecialType 为某种 Redis Module 类型注册自定义解析函数。
// Module 类型是 Redis 扩展模块（如 RedisJSON、RedisBloom）写入的数据，
// 其二进制格式由模块自己定义，rdb 库无法内置解析，因此需要调用方自行提供 handler。
//
// 用法：
//
//	dec.WithSpecialType("RedisJSON", func(dec *core.Decoder) (interface{}, error) { ... })
func (dec *Decoder) WithSpecialType(moduleType string, f ModuleTypeHandleFunc) *Decoder {
	dec.withSpecialTypes[moduleType] = f
	return dec
}

// magicNumber 是 RDB 文件开头的魔数，用来快速判断「这是不是一个 RDB 文件」。
var magicNumber = []byte("REDIS")

const (
	minVersion = 1  // 支持的最小编码版本
	maxVersion = 12 // 支持的最大编码版本（对应 Redis 7.x）
)

// RDB 文件中的「操作码」(opcode)：用 1 个字节表示接下来要做什么，而不是数据类型。
// 它们的取值集中在 247~255，与数据类型（0~20 左右）区分开。
const (
	opCodeModuleAux    = 247 /* Module auxiliary data. 模块辅助数据 */
	opCodeIdle         = 248 /* LRU idle time. LRU 空闲时间（用于 LRU 淘汰） */
	opCodeFreq         = 249 /* LFU frequency. LFU 访问频率（用于 LFU 淘汰） */
	opCodeAux          = 250 /* RDB aux field. 元信息键值对，如 redis-ver */
	opCodeResizeDB     = 251 /* Hash table resize hint. 哈希表扩容提示（key 数/ttl 数） */
	opCodeExpireTimeMs = 252 /* Expire time in milliseconds. 过期时间（毫秒时间戳） */
	opCodeExpireTime   = 253 /* Old expire time in seconds. 旧格式过期时间（秒时间戳） */
	opCodeSelectDB     = 254 /* DB number of the following keys. 切换到指定 db */
	opCodeEOF          = 255 /* 数据结束标记 */
)

// 数据类型标识：RDB 在每条键值对前写 1 个字节表示 value 的「逻辑类型 + 物理编码」。
// 注意 iota 会因中间的下划线而留空，取值必须与 Redis 官方定义严格一致，不能随意增删。
const (
	typeString = iota
	typeList
	typeSet
	typeZset
	typeHash
	typeZset2 /* ZSET version 2 with doubles stored in binary. 用二进制 double 存 score */
	typeModule
	typeModule2 // Module value parser should be registered with Decoder.WithSpecialType
	_
	typeHashZipMap
	typeListZipList
	typeSetIntSet
	typeZsetZipList
	typeHashZipList
	typeListQuickList
	typeStreamListPacks
	typeHashListPack
	typeZsetListPack
	typeListQuickList2
	typeStreamListPacks2
	typeSetListPack
	typeStreamListPacks3
)

// encodingMap 把「类型字节」映射成人类可读的「物理编码名」，写入 model.BaseObject.Encoding。
// 注意：多种不同的 type 可能映射到同一种 Encoding（如 typeZsetZipList 与 typeHashZipList 都是 ziplist）。
var encodingMap = map[int]string{
	typeString:           model.StringEncoding,
	typeList:             model.ListEncoding,
	typeSet:              model.SetEncoding,
	typeZset:             model.ZSetEncoding,
	typeHash:             model.HashEncoding,
	typeZset2:            model.ZSet2Encoding,
	typeHashZipMap:       model.ZipMapEncoding,
	typeListZipList:      model.ZipListEncoding,
	typeSetIntSet:        model.IntSetEncoding,
	typeZsetZipList:      model.ZipListEncoding,
	typeHashZipList:      model.ZipListEncoding,
	typeListQuickList:    model.QuickListEncoding,
	typeStreamListPacks:  model.ListPackEncoding,
	typeStreamListPacks2: model.ListPackEncoding,
	typeHashListPack:     model.ListPackEncoding,
	typeZsetListPack:     model.ListPackEncoding,
	typeListQuickList2:   model.QuickList2Encoding,
	typeSetListPack:      model.ListPackEncoding,
}

// checkHeader 校验 RDB 文件头：前 5 字节必须是 "REDIS"，后 4 字节是版本号（1~12）。
// 校验失败说明文件损坏或不是 RDB 文件，直接返回错误。
func (dec *Decoder) checkHeader() error {
	header := make([]byte, 9)
	err := dec.readFull(header)
	if err == io.EOF {
		return errors.New("empty file")
	}
	if err != nil {
		return fmt.Errorf("io error: %v", err)
	}
	if !bytes.Equal(header[0:5], magicNumber) {
		return errors.New("file is not a RDB file")
	}
	version, err := strconv.Atoi(string(header[5:]))
	if err != nil {
		return fmt.Errorf("%s is not valid version number", string(header[5:]))
	}
	if version < minVersion || version > maxVersion {
		return fmt.Errorf("cannot parse version: %d", version)
	}
	return nil
}

// readObject 根据「类型字节 flag」分派到对应的解析函数，产出统一的 model 对象。
//
// 关键点：同一种「逻辑类型」可能对应多种「物理编码」，例如 list 可能是
// typeList（普通数组）、typeListZipList（ziplist 压缩）、typeListQuickList（quicklist）。
// 无论哪种物理编码，最终都会还原成同一种 model 对象（如 *model.ListObject），
// 这样调用方就不必关心底层是怎么存的。
//
// 参数 base 已由调用方填好 DB/Key/过期时间，这里再补 Encoding，并可能补 Extra（内存估算细节）。
func (dec *Decoder) readObject(flag byte, base *model.BaseObject) (model.RedisObject, error) {
	base.Encoding = encodingMap[int(flag)]
	switch flag {
	case typeString:
		bs, err := dec.readString()
		if err != nil {
			return nil, err
		}
		return &model.StringObject{
			BaseObject: base,
			Value:      bs,
		}, nil
	case typeList:
		list, err := dec.readList()
		if err != nil {
			return nil, err
		}
		return &model.ListObject{
			BaseObject: base,
			Values:     list,
		}, nil
	case typeSet:
		set, err := dec.readSet()
		if err != nil {
			return nil, err
		}
		return &model.SetObject{
			BaseObject: base,
			Members:    set,
		}, nil
	case typeSetIntSet:
		set, extra, err := dec.readIntSet()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.SetObject{
			BaseObject: base,
			Members:    set,
		}, nil
	case typeHash:
		hash, err := dec.readHashMap()
		if err != nil {
			return nil, err
		}
		return &model.HashObject{
			BaseObject: base,
			Hash:       hash,
		}, nil
	case typeListZipList:
		list, err := dec.readZipList()
		if err != nil {
			return nil, err
		}
		return &model.ListObject{
			BaseObject: base,
			Values:     list,
		}, nil
	case typeListQuickList:
		list, extra, err := dec.readQuickList()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.ListObject{
			BaseObject: base,
			Values:     list,
		}, nil
	case typeListQuickList2:
		list, extra, err := dec.readQuickList2()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.ListObject{
			BaseObject: base,
			Values:     list,
		}, nil
	case typeHashZipMap:
		m, err := dec.readZipMapHash()
		if err != nil {
			return nil, err
		}
		return &model.HashObject{
			BaseObject: base,
			Hash:       m,
		}, nil
	case typeHashZipList:
		m, extra, err := dec.readZipListHash()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.HashObject{
			BaseObject: base,
			Hash:       m,
		}, nil
	case typeHashListPack:
		m, extra, err := dec.readListPackHash()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.HashObject{
			BaseObject: base,
			Hash:       m,
		}, nil
	case typeZset:
		entries, err := dec.readZSet(false)
		if err != nil {
			return nil, err
		}
		return &model.ZSetObject{
			BaseObject: base,
			Entries:    entries,
		}, nil
	case typeZset2:
		entries, err := dec.readZSet(true)
		if err != nil {
			return nil, err
		}
		return &model.ZSetObject{
			BaseObject: base,
			Entries:    entries,
		}, nil
	case typeZsetZipList:
		entries, extra, err := dec.readZipListZSet()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.ZSetObject{
			BaseObject: base,
			Entries:    entries,
		}, nil
	case typeZsetListPack:
		entries, extra, err := dec.readListPackZSet()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.ZSetObject{
			BaseObject: base,
			Entries:    entries,
		}, nil
	case typeStreamListPacks, typeStreamListPacks2, typeStreamListPacks3:
		var version uint = 1
		if flag == typeStreamListPacks2 {
			version = 2
		} else if flag == typeStreamListPacks3 {
			version = 3
		}
		stream, err := dec.readStreamListPacks(version)
		if err != nil {
			return nil, err
		}
		stream.BaseObject = base
		return stream, nil
	case typeModule2:
		moduleType, val, err := dec.readModuleType()
		if err != nil {
			return nil, err
		}
		return &model.ModuleTypeObject{
			BaseObject: base,
			ModuleType: moduleType,
			Value:      val,
		}, nil
	case typeSetListPack:
		set, extra, err := dec.readListPackSet()
		if err != nil {
			return nil, err
		}
		base.Extra = extra
		return &model.SetObject{
			BaseObject: base,
			Members:    set,
		}, nil
	}
	return nil, fmt.Errorf("unknown type flag: %b", flag)
}

// parse 是 RDB 解析的主循环（状态机）。
//
// 思路：不断从字节流里读「1 个字节」，根据它判断接下来是什么：
//   - 如果是「操作码」(247~255)，就处理对应的元信息（切库、过期时间、扩容提示等），
//     然后 continue 读下一个字节；
//   - 否则这个字节就是「数据类型」，说明接下来是一条真正的键值对：
//     依次读 key、value，组装成 model 对象后回调给调用方 cb。
//
// 两个「粘性状态」变量：
//   - dbIndex：当前所处的数据库编号，遇 SELECTDB 时更新，后续所有 key 都归属它；
//   - expireMs：暂存的过期时间戳。EXPIRE 操作码总是紧挨着它要作用的那个 key，
//     所以读到后先存起来，等解析完下一条键值对再绑定上去并清零。
//
// cb 返回 false 表示调用方要求提前终止解析（例如只取前 N 条）。
func (dec *Decoder) parse(cb func(object model.RedisObject) bool) error {
	var dbIndex int
	var expireMs int64
	for {
		// 读 1 个字节：可能是操作码，也可能是数据类型标识
		b, err := dec.readByte()
		if err != nil {
			return err
		}
		if b == opCodeEOF {
			// 0xFF：数据区结束，跳出循环（后面还有 8 字节 CRC 校验和）
			break
		} else if b == opCodeSelectDB {
			// 切换数据库：后面跟一个长度编码表示 db 编号
			dbIndex64, _, err := dec.readLength()
			if err != nil {
				return err
			}
			dbIndex = int(dbIndex64)
			continue
		} else if b == opCodeExpireTime {
			// 旧格式过期时间：4 字节「秒级」时间戳（小端），转成毫秒存入 expireMs
			err = dec.readFull(dec.buffer[:4])
			if err != nil {
				return err
			}
			expireMs = int64(binary.LittleEndian.Uint32(dec.buffer)) * 1000
			continue
		} else if b == opCodeExpireTimeMs {
			// 新格式过期时间：8 字节「毫秒级」时间戳（小端）
			err = dec.readFull(dec.buffer)
			if err != nil {
				return err
			}
			expireMs = int64(binary.LittleEndian.Uint64(dec.buffer))
			continue
		} else if b == opCodeResizeDB {
			// 扩容提示：key 总数 + 带 ttl 的 key 数（仅提示，用于预分配哈希表）
			keyCount, _, err := dec.readLength()
			if err != nil {
				return err
			}
			ttlCount, _, err := dec.readLength()
			if err != nil {
				// 注意：这里 break 会跳出循环且不向上传递 err（原库的既有行为），
				// 结果是读不到真实数据但 Parse 仍返回 nil。阅读时需留意。
				err = errors.New("Parse Aux value failed: " + err.Error())
				break
			}
			if dec.withSpecialOpCode {
				// 仅当开启了 WithSpecialOpCode 才把这个元信息回调出去
				obj := &model.DBSizeObject{
					BaseObject: &model.BaseObject{},
				}
				obj.DB = dbIndex
				obj.KeyCount = keyCount
				obj.TTLCount = ttlCount
				tbc := cb(obj)
				if !tbc {
					break
				}
			}
			continue
		} else if b == opCodeAux {
			// 元信息键值对，如 "redis-ver" -> "7.0.0"、"redis-bits" -> "64"
			key, err := dec.readString()
			if err != nil {
				return err
			}
			value, err := dec.readString()
			if err != nil {
				err = errors.New("Parse Aux value failed: " + err.Error())
				break
			}
			if dec.withSpecialOpCode {
				obj := &model.AuxObject{
					BaseObject: &model.BaseObject{},
				}
				obj.Key = unsafeBytes2Str(key)
				obj.Value = unsafeBytes2Str(value)
				tbc := cb(obj)
				if !tbc {
					break
				}
			}
			continue
		} else if b == opCodeFreq {
			// LFU 访问频率：1 字节，与业务数据无关，跳过
			_, err = dec.readByte()
			if err != nil {
				return err
			}
			continue
		} else if b == opCodeIdle {
			// LRU 空闲时长：一个长度编码，跳过
			_, _, err = dec.readLength()
			if err != nil {
				return err
			}
			continue
		} else if b == opCodeModuleAux {
			// 模块辅助数据：交由模块解析逻辑处理
			_, _, err = dec.readModuleType()
			if err != nil {
				return err
			}
			continue
		}

		// 走到这里说明 b 不是操作码，而是一个「数据类型标识」——开始解析一条键值对
		key, err := dec.readString()
		if err != nil {
			return err
		}
		base := &model.BaseObject{
			DB:  dbIndex,
			Key: unsafeBytes2Str(key),
		}
		// 若前面暂存了过期时间，绑定到本条对象上（过期时间总是紧邻其 key）
		if expireMs > 0 {
			expiration := time.Unix(0, expireMs*int64(time.Millisecond))
			base.Expiration = &expiration
			expireMs = 0 // 用掉后清零，避免污染下一条
		}
		obj, err := dec.readObject(b, base)
		if err != nil {
			return err
		}
		// 估算该对象在 Redis 中占用的内存大小，并补齐 Type 字段
		base.Size = memprofiler.SizeOfObject(obj)
		base.Type = obj.GetType()
		tbc := cb(obj)
		if !tbc {
			// 调用方要求停止
			break
		}
	}
	// 数据区结束后是 8 字节 CRC64 校验和；这里直接读掉（不校验）
	_ = dec.readFull(dec.buffer)
	return nil
}

// Parse 解析整个 RDB 文件，并对每个对象调用回调 cb。
//
// cb 返回 true 表示继续解析，返回 false 表示停止（用于只取部分数据）。
// 函数内部用 defer+recover 兜住解析过程中的 panic，转成 error 返回，
// 避免因文件损坏导致整个程序崩溃。
func (dec *Decoder) Parse(cb func(object model.RedisObject) bool) (err error) {
	defer func() {
		if err2 := recover(); err2 != nil {
			err = fmt.Errorf("panic: %v", err2)
		}
	}()
	err = dec.checkHeader()
	if err != nil {
		return err
	}
	return dec.parse(cb)
}

// GetReadCount 返回到目前为止已读取的字节数（含缓冲区内已读部分），可用于统计解析进度。
func (dec *Decoder) GetReadCount() int {
	return dec.readCount
}
