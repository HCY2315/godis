package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"os"
	"strconv"
	"time"
)

// MemoryProfile 读取 rdb 文件，逐个统计每个 key 的内存占用，输出为 CSV 报告。
//
// 用法（命令行）：
//
//	rdb -c memory -o memory.csv dump.rdb
//
// 输出 CSV 列：database,key,type,size,size_readable,element_count,encoding,expiration
//
// 说明：size 是「估算值」，由 memprofiler 根据对象的编码方式（ziplist/listpack/intset 等）
// 按 Redis 内部结构近似计算，用于找出内存大户，而非精确到字节。
func MemoryProfile(rdbFilename string, csvFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if csvFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	csvFile, err := os.Create(csvFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", csvFilename, err)
	}
	defer func() {
		_ = csvFile.Close()
	}()

	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}

	_, err = csvFile.WriteString("database,key,type,size,size_readable,element_count,encoding,expiration\n")
	if err != nil {
		return fmt.Errorf("write csv failed: %v", err)
	}
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	// formatExpiration 把过期时间格式化成 RFC3339 字符串；无过期时间则输出空串
	formatExpiration := func(o model.RedisObject) string {
		expiration := o.GetExpiration()
		if expiration == nil {
			return ""
		}
		return expiration.Format(time.RFC3339)
	}
	return dec.Parse(func(object model.RedisObject) bool {
		err = csvWriter.Write([]string{
			strconv.Itoa(object.GetDBIndex()),
			object.GetKey(),
			object.GetType(),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
			object.GetEncoding(),
			formatExpiration(object),
		})
		if err != nil {
			fmt.Printf("csv write failed: %v", err)
			return false
		}
		return true
	})
}
