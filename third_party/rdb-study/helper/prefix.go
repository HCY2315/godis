package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// PrefixAnalyse 读取 rdb 文件，按 key 的前缀（如 "User:"、"Stat:Post:"）聚合统计内存占用，
// 找出「最占内存的 topN 个前缀」，结果以 CSV 写入 output。
//
// 参数：
//
//	topN     : 输出前多少个前缀；0 表示不限制（内部转为 MaxInt）
//	maxDepth : 前缀树的最大深度；0 表示不限制。注意内部会 +2，
//	           因为树的第 1 层是根、第 2 层是「数据库」根，真正的前缀从第 3 层开始
//	output   : 输出目标，由调用方负责关闭
//
// 用法（命令行）：
//
//	rdb -c prefix -n 10 -max-depth 2 -o prefix.csv dump.rdb
//
// 输出 CSV 列：database,prefix,size,size_readable,key_count
//
// 实现原理：把每个 key 按分隔符切成若干段插入「基数树(radix tree)」，
// 树节点累计子树的总大小与 key 数量，最后遍历树取出最大的前缀节点。
func PrefixAnalyse(rdbFilename string, topN int, maxDepth int, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if topN < 0 {
		return errors.New("n must greater than 0")
	} else if topN == 0 {
		topN = math.MaxInt
	}
	if maxDepth == 0 {
		maxDepth = math.MaxInt
	} else {
		maxDepth += 2 // for root(depth==1) and database root(depth==2)
	}

	// decode rdb file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}

	// prefix tree
	// 把每个 key 以「db + key」的形式插入基数树，节点会累加 totalSize / keyCount
	tree := newRadixTree()
	err = dec.Parse(func(object model.RedisObject) bool {
		key := genKey(object.GetDBIndex(), object.GetKey())
		tree.insert(key, object.GetSize())
		return true
	})
	if err != nil {
		return err
	}

	// get top list
	// 遍历前缀树，跳过根与「数据库根」两层，其余节点都参与 TopN 竞争
	toplist := newToplist(topN)
	tree.traverse(func(node *radixNode, depth int) bool {
		if depth > maxDepth {
			return false
		}
		if depth <= 2 {
			// skip root and database root
			return true
		}
		toplist.add(node)
		return true
	})

	// write into csv
	_, err = output.WriteString("database,prefix,size,size_readable,key_count\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	printNode := func(node *radixNode) error {
		db, key := parseNodeKey(node.fullpath)
		dbStr := strconv.Itoa(db)
		return csvWriter.Write([]string{
			dbStr,
			key,
			strconv.Itoa(node.totalSize),
			bytefmt.FormatSize(uint64(node.totalSize)),
			strconv.Itoa(node.keyCount),
		})
	}
	for _, n := range toplist.list {
		node := n.(*radixNode)
		err := printNode(node)
		if err != nil {
			return err
		}
	}

	return nil
}
