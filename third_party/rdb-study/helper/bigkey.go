package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// FindBiggestKeys 读取 rdb 文件，找出「体积最大的 N 个 key」，结果以 CSV 写入 output。
//
// 参数：
//
//	rdbFilename : 源 rdb 文件路径
//	topN        : 要取前多少个（必须 > 0）
//	output      : 输出目标；注意「由调用方负责关闭」，本函数不会关闭它
//	options     : 透传给解码器的选项（如正则过滤、是否保留过期 key）
//
// 用法（命令行）：
//
//	rdb -c bigkey -n 10 dump.rdb
//
// 输出 CSV 列：database,key,type,size,size_readable,element_count
func FindBiggestKeys(rdbFilename string, topN int, output *os.File, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if topN <= 0 {
		return errors.New("n must greater than 0")
	}
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
	// top 是一个「只保留最大的 topN 个」的有序容器（见 toplist.go）
	top := newToplist(topN)
	err = dec.Parse(func(object model.RedisObject) bool {
		top.add(object) // 每解析一个对象就尝试放入 TopN
		return true
	})
	if err != nil {
		return err
	}
	_, err = output.WriteString("database,key,type,size,size_readable,element_count\n")
	if err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	csvWriter := csv.NewWriter(output)
	defer csvWriter.Flush()
	for _, o := range top.list {
		object := o.(model.RedisObject)
		err = csvWriter.Write([]string{
			strconv.Itoa(object.GetDBIndex()),
			object.GetKey(),
			object.GetType(),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
		})
		if err != nil {
			return fmt.Errorf("csv write failed: %v", err)
		}
	}
	return nil
}
