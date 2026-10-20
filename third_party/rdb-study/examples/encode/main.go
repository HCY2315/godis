// Package main 演示如何用 encoder 包从零生成一个 RDB 文件。
//
// Encoder 内部维护一个状态机，写入顺序必须是：
//
//	WriteHeader
//	  -> WriteAux（0 次或多次）
//	  -> WriteDBHeader（每个 db 一次）
//	  -> 若干对象写入（WriteStringObject/WriteListObject/...）
//	  -> 如需写入下一个 db，再次 WriteDBHeader
//	  -> WriteEnd（最后一步）
//
// 违反顺序时对应方法会返回 "cannot writing ... at state: ..." 错误。
// 具体约束：
//   - WriteHeader 只能调用一次，且必须是第一个写操作；
//   - WriteAux 只能在 header 之后、db header/结束之前调用，可以重复；
//   - WriteDBHeader 必须在对象写入之前调用，同一个 dbIndex 不能重复；
//   - WithTTL 只能紧邻对象写入使用（先写过期时间，再写对象）；
//   - WriteEnd 必须最后调用，它会写入 EOF 操作码、CRC64 校验和与换行符。
package main

import (
	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"os"
	"time"
)

// main 生成一个名为 dump.rdb 的示例文件，覆盖 string/list/set/hash/zset
// 五种常用类型，并演示如何为对象设置 TTL。
//
// 使用技巧：
//   - 每个写方法都返回 error，生产代码中应立即检查（这里为示例使用 panic）；
//   - 对象方法（如 WriteListObject）返回后，编码器状态变为 WrittenObject，
//     此时可以继续写同库对象、写下一个库，或调用 WriteEnd；
//   - TTL 的单位是毫秒级 Unix 时间戳（不是相对时长），见下方 expirationMs；
//   - 若希望小集合使用 ziplist/listpack 等紧凑编码，可在 NewEncoder 后调用
//     SetListZipListOpt / SetHashZipListOpt / SetZSetZipListOpt 设置阈值。
func main() {
	// 创建输出文件（已存在会被截断）。RDB 是二进制格式，必须用 Create/OpenFile。
	rdbFile, err := os.Create("dump.rdb")
	if err != nil {
		panic(err)
	}
	// defer 确保函数返回时关闭文件。
	defer rdbFile.Close()
	// NewEncoder 接收任意 io.Writer（文件、bytes.Buffer、网络连接均可）。
	enc := encoder.NewEncoder(rdbFile)
	// 第一步：写入 9 字节魔数与版本号（本库固定写 "REDIS0011"，即 RDB v11）。
	err = enc.WriteHeader()
	if err != nil {
		panic(err)
	}
	// 第二步：写入辅助字段（aux）。这些键值对用于记录生成环境信息，
	// 如 Redis 版本、位宽、是否带 AOF preamble 等；可写入 0 到多个。
	auxMap := map[string]string{
		"redis-ver":    "4.0.6",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			panic(err)
		}
	}

	// 第三步：写入 db 头。参数依次为：
	//   dbIndex  —— 数据库编号（对应 SELECT 的索引）；
	//   keyCount —— 该库 key 数量的提示值（RDB_OPCODE_RESIZEDB）；
	//   ttlCount —— 该库带过期时间 key 数量的提示值。
	// 注意：同一 dbIndex 只能写一次，重复写会返回 "db %d existed"。
	err = enc.WriteDBHeader(0, 5, 1)
	if err != nil {
		panic(err)
	}
	// 计算过期时间：time.Now().Add(8h) 得到绝对时间，Unix() 取秒，
	// 再乘 1000 转成毫秒级时间戳（RDB 的毫秒过期时间格式）。
	expirationMs := uint64(time.Now().Add(time.Hour*8).Unix() * 1000)
	// 第四步：写入对象。
	// string 对象：WithTTL 作为可变参数传入，会在对象前写入过期时间操作码；
	// 因此 TTL 选项只对紧随其后的这一个对象生效。
	err = enc.WriteStringObject("hello", []byte("world"), encoder.WithTTL(expirationMs))
	if err != nil {
		panic(err)
	}
	// list 对象：按切片顺序写入元素。
	err = enc.WriteListObject("list", [][]byte{
		[]byte("123"),
		[]byte("abc"),
		[]byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
	// set 对象：成员顺序在语义上无关，编码时按给定切片顺序写入。
	err = enc.WriteSetObject("set", [][]byte{
		[]byte("123"),
		[]byte("abc"),
		[]byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
	// hash 对象：field -> value 的映射。
	err = enc.WriteHashMapObject("list", map[string][]byte{
		"1":  []byte("123"),
		"a":  []byte("abc"),
		"la": []byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
	// zset 对象：每个元素由 Member（成员）和 Score（分值）组成。
	err = enc.WriteZSetObject("list", []*model.ZSetEntry{
		{
			Score:  1.234,
			Member: "a",
		},
		{
			Score:  2.71828,
			Member: "b",
		},
	})
	if err != nil {
		panic(err)
	}
	// 第五步：写入文件结束标记。WriteEnd 会依次写入 EOF 操作码、
	// 对已写内容计算的 CRC64 校验和，以及一个换行符；调用后不能再写对象。
	err = enc.WriteEnd()
	if err != nil {
		panic(err)
	}
}
