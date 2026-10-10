// dict 包：键值字典抽象。核心实现是 ConcurrentDict（分片锁定，见 concurrent.go），
// SimpleDict（单锁，见 simple.go）。godis 的数据表、TTL 表、版本表都基于它。
package dict

// Consumer —— 命名含义：遍历字典时的回调；返回 false 表示提前终止遍历。
type Consumer func(key string, val interface{}) bool

// Dict —— 命名含义：字典接口。
// 用途：统一 Get/Put/Remove/遍历/随机取键/Scan 等操作，屏蔽底层是并发还是简单实现。
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (val interface{}, result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
