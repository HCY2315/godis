package helper

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"os"
)

// jsonEncoder 用 sonic（高性能 JSON 库）的默认配置。
// 注释里提到「enable SortMapKeys to ensure same result」：对 map 排序能让输出稳定可复现，
// 这里使用 ConfigDefault，若需固定顺序可换成 sonic.Config{SortedMapKeys:true}。
var jsonEncoder = sonic.ConfigDefault

// ToJsons 读取 rdb 文件，把其中每个对象转换成一行 JSON，输出成 JSON 数组文件。
//
// 用法：
//
//	helper.ToJsons("dump.rdb", "dump.json")
//
// 实现技巧：为了支持「大文件流式输出」（不把所有对象堆在内存里），
// 它先写 "[\n"，然后每解析一个对象就写 "对象JSON,\n"，
// 最后用 Seek(-2, 2) 回退两个字节（去掉最后一个逗号和换行），再补 "\n]" 收尾。
func ToJsons(rdbFilename string, jsonFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if jsonFilename == "" {
		return errors.New("output file path is required")
	}
	// open file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	jsonFile, err := os.Create(jsonFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", jsonFilename, err)
	}
	defer func() {
		_ = jsonFile.Close()
	}()
	// create decoder
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	// parse rdb
	_, err = jsonFile.WriteString("[\n")
	if err != nil {
		return fmt.Errorf("write json  failed, %v", err)
	}
	empty := true
	err = dec.Parse(func(object model.RedisObject) bool {
		data, err := jsonEncoder.Marshal(object) // enable SortMapKeys to ensure same result
		if err != nil {
			fmt.Printf("json marshal failed: %v", err)
			return true
		}
		data = append(data, ',', '\n')
		_, err = jsonFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		empty = false
		return true
	})
	if err != nil {
		return err
	}
	// finish json
	if !empty {
		_, err = jsonFile.Seek(-2, 2)
		if err != nil {
			return fmt.Errorf("error during seek in file: %v", err)
		}
	}
	_, err = jsonFile.WriteString("\n]")
	if err != nil {
		return fmt.Errorf("error during write in file: %v", err)
	}
	return nil
}

// ToAOF 读取 rdb 文件，转换成 AOF 文件（AOF = 以 RESP 协议记录的一条条写命令）。
//
// 用法：
//
//	helper.ToAOF("dump.rdb", "dump.aof")
//
// 原理：RDB 存的是「数据快照」，而 AOF 存的是「重建这些数据的命令」。
// 所以这里把每个对象还原成对应的写命令（如 SET / RPUSH / HMSET / SADD / ZADD），
// 再按 RESP 协议编码写出去。
func ToAOF(rdbFilename string, aofFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if aofFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	aofFile, err := os.Create(aofFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", aofFilename, err)
	}
	defer func() {
		_ = aofFile.Close()
	}()

	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	return dec.Parse(func(object model.RedisObject) bool {
		cmdLines := ObjectToCmd(object, options...)
		data := CmdLinesToResp(cmdLines)
		_, err = aofFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		return true
	})
}
