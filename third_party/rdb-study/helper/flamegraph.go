package helper

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/d3flame"
	"github.com/hdt3213/rdb/model"
	"os"
	"strconv"
	"strings"
)

// TrimThreshold 是「启用裁剪(trim)」的 key 数量阈值：
// 当 key 数量 >= 1000 时，说明叶子节点太多，火焰图会过于细碎，
// 于是把「小于 1MB 的叶子节点」合并成 "others"，让图更易读。
var TrimThreshold = 1000

// FlameGraph 解析 rdb 并按 key 前缀构建「火焰图」数据，然后在网页中展示（用于分析内存分布）。
//
// 参数：
//
//	rdbFilename : 源 rdb 文件
//	port        : web 服务端口，0 表示用默认 16379
//	separators  : key 的分隔符列表（如 [":", "_"]），用于把 key 拆成层级
//
// 返回一个 channel：向它发送任意值即可停止 web 服务。
//
// 用法（命令行）：
//
//	rdb -c flamegraph -port 16379 -sep : dump.rdb
//
// 原理：把 key 按分隔符拆成多段，插入一棵「前缀树」（每个节点记录累计大小 Value），
// 再用 d3-flame-graph 渲染成火焰图——方块越宽表示该前缀占用内存越多。
func FlameGraph(rdbFilename string, port int, separators []string, options ...interface{}) (chan<- struct{}, error) {
	if rdbFilename == "" {
		return nil, errors.New("src file path is required")
	}
	if port == 0 {
		port = 16379 // default port
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return nil, fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return nil, err
	}
	root := &d3flame.FlameItem{
		Name:     "root",
		Children: make(map[string]*d3flame.FlameItem),
	}
	var count int
	err = dec.Parse(func(object model.RedisObject) bool {
		count++
		addObject(root, separators, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	// 根节点的 Value 由所有直接子节点累加得到（即总内存）
	totalSize := 0
	for _, v := range root.Children {
		totalSize += v.Value
	}
	root.Value = totalSize
	// key 太多时裁剪掉「又小又碎」的叶子，合并为 others
	if count >= TrimThreshold {
		trimData(root)
	}
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return d3flame.Web(data, port), nil
}

// split 按分隔符把 key 拆成层级数组。
//
// 支持多个分隔符：以第一个为准，其余分隔符先统一替换成第一个，再统一 Split。
// 例如 separators=[":","_"]、s="a:b_c" → 先把 "_" 换成 ":" 得 "a:b:c" → 拆成 ["a","b","c"]。
func split(s string, separators []string) []string {
	sep := ":"
	if len(separators) > 0 {
		sep = separators[0]
	}
	for i := 1; i < len(separators); i++ {
		s = strings.ReplaceAll(s, separators[i], sep)
	}
	return strings.Split(s, sep)
}

// addObject 把一个对象按其 key 的层级路径插入火焰图树，并沿途累加内存大小。
//
// 路径最前面会自动插入一个 "db:N" 节点，从而实现「按库分组」。
// 每到一个节点就 node.Value += 该对象大小，所以父节点天然是子树之和。
func addObject(root *d3flame.FlameItem, separators []string, object model.RedisObject) {
	node := root
	parts := split(object.GetKey(), separators)
	parts = append([]string{"db:" + strconv.Itoa(object.GetDBIndex())}, parts...)
	for _, part := range parts {
		if node.Children[part] == nil {
			n := &d3flame.FlameItem{
				Name:     part,
				Children: make(map[string]*d3flame.FlameItem),
			}
			node.AddChild(n)
		}
		node = node.Children[part]
		node.Value += object.GetSize()
	}
}

// bigNodeThreshold 是「大节点」阈值（1MB）。
// 裁剪时，小于该值的叶子节点会被合并进 "others"，避免火焰图出现海量小方块。
var bigNodeThreshold = 1024 * 1024 // 1MB

// trimData 用 BFS 遍历火焰图树，做「长尾裁剪」：
// 对每个节点，把它那些「无子节点且小于 1MB」的叶子子节点删掉，
// 把这些小叶子的大小汇总成一个名为 "others" 的子节点。
// 这样既保留了「大 key」的可见性，又不会让图被无数小 key 撑爆。
func trimData(root *d3flame.FlameItem) {
	// trim long tail
	queue := []*d3flame.FlameItem{
		root,
	}
	for len(queue) > 0 {
		// Aggregate leaf nodes
		node := queue[0]
		queue = queue[1:]
		leafSum := 0
		for key, child := range node.Children {
			if len(child.Children) == 0 && child.Value < bigNodeThreshold { // child is a leaf node
				delete(node.Children, key) // 删除过小的叶子 key
				leafSum += child.Value
			}
			queue = append(queue, child) // 保留大 key 继续遍历
		}
		if leafSum > 0 {
			n := &d3flame.FlameItem{
				Name:  "others",
				Value: leafSum,
			}
			node.AddChild(n)
		}
	}
}
