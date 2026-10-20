// Package main 演示如何用 parser 包解析（解码）一个 RDB 文件。
//
// 核心 API 只有两步：
//  1. parser.NewDecoder(io.Reader) 创建解码器；
//  2. decoder.Parse(callback) 从头到尾遍历 RDB，每读到一个对象就回调一次。
//
// Parse 的回调返回 true 表示继续遍历，返回 false 表示提前停止。
// 回调参数是接口类型 parser.RedisObject，需要根据 GetType() 做类型断言，
// 才能访问具体类型的字段（Value/Values/Hash/Entries 等）。
package main

import (
	"github.com/hdt3213/rdb/parser"
	"os"
)

// main 打开当前目录下的 example.rdb，并逐个打印其中支持的 Redis 对象。
//
// 使用技巧：
//   - 生产代码中应检查 os.Open 与 Parse 的错误，这里为示例简洁使用 panic；
//   - 如果想把 Aux/DBSize 等元数据也交给回调，需要额外调用
//     decoder.WithSpecialOpCode()；
//   - 如果要解析 Redis Module 自定义类型，需要调用
//     decoder.WithSpecialType(moduleType, handler) 注册处理器；
//   - 类型断言 o.(*parser.StringObject) 之所以安全，是因为 GetType() 已保证类型；
//     若不确定类型，请使用带 ok 的断言 `v, ok := o.(*parser.StringObject)`。
func main() {
	// 打开待解析的 RDB 文件；文件内容由 Redis 的 BGSAVE/SAVE 生成。
	rdbFile, err := os.Open("example.rdb")
	if err != nil {
		panic("open example.rdb failed")
	}
	// defer 确保函数退出时关闭文件。
	defer func() {
		_ = rdbFile.Close()
	}()
	// NewDecoder 接收任意 io.Reader（文件、网络流、bytes.Buffer 均可），
	// 返回的 decoder 可以连续调用 Parse。
	decoder := parser.NewDecoder(rdbFile)
	// Parse 会顺序读取 RDB 中的每个对象并调用回调，直到文件结束或回调返回 false。
	err = decoder.Parse(func(o parser.RedisObject) bool {
		// GetType 返回对象的 Redis 类型字符串（string/list/set/hash/zset/stream 等）。
		switch o.GetType() {
		case parser.StringType:
			// 字符串对象：Key 是键名，Value 是 []byte 值。
			str := o.(*parser.StringObject)
			println(str.Key, str.Value)
		case parser.ListType:
			// 列表对象：Values 按顺序保存所有元素（[]([]byte)）。
			list := o.(*parser.ListObject)
			println(list.Key, list.Values)
		case parser.HashType:
			// 哈希对象：Hash 是 field -> value 的映射（map[string][]byte）。
			hash := o.(*parser.HashObject)
			println(hash.Key, hash.Hash)
		case parser.ZSetType:
			// 有序集合对象：Entries 保存 member 与 score，顺序即 RDB 中的顺序。
			zset := o.(*parser.ZSetObject)
			println(zset.Key, zset.Entries)
		case parser.StreamType:
			// Stream 对象：Entries 是消息条目，Groups 是消费者组信息。
			stream := o.(*parser.StreamObject)
			println(stream.Entries, stream.Groups)
		}
		// return true to continue, return false to stop the iteration
		// 返回 true 继续读取下一个对象；返回 false 会立即结束 Parse。
		return true
	})
	if err != nil {
		panic(err)
	}
}
