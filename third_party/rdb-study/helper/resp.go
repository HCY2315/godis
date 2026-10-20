package helper

import (
	"bytes"
	"github.com/hdt3213/rdb/model"
	"io"
	"sort"
	"strconv"
)

// crlf 是 RESP 协议的行结束符（Redis Serialization Protocol 规定用 \r\n）。
const crlf = "\r\n"

// CmdLine is alias for [][]byte, represents a command line
//
// 一条命令用 [][]byte 表示：第一个元素是命令名，后面依次是参数。
// 例如 SET k v → [][]byte{[]byte("SET"), []byte("k"), []byte("v")}。
type CmdLine = [][]byte

// lexOrder 是一个「选项标记类型」，用于让 ObjectToCmd 对 hash 字段按字典序排序输出。
// 用法：helper.ObjectToCmd(obj, lexOrder{}) —— 这样生成的结果可复现（map 默认无序）。
type lexOrder struct{}

// makeMultiBulkResp 把一条命令编码成 RESP 的「多批量回复」格式：
//
//	*<参数个数>\r\n
//	$<参数1长度>\r\n<参数1>\r\n
//	$<参数2长度>\r\n<参数2>\r\n
//	...
//
// 其中 nil 参数编码为 $-1\r\n（表示空值）。Redis 客户端/服务端都用这种格式收发命令。
func makeMultiBulkResp(args [][]byte) []byte {
	argLen := len(args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + crlf)
	for _, arg := range args {
		if arg == nil {
			buf.WriteString("$-1" + crlf)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + crlf + string(arg) + crlf)
		}
	}
	return buf.Bytes()
}

var setCmd = []byte("SET")

// stringToCmd 把 string 对象转成 SET 命令。
func stringToCmd(obj *model.StringObject) CmdLine {
	cmdLine := make([][]byte, 3)
	cmdLine[0] = setCmd
	cmdLine[1] = []byte(obj.Key)
	cmdLine[2] = obj.Value
	return cmdLine
}

var rPushAllCmd = []byte("RPUSH")

// listToCmd 把 list 对象转成一条 RPUSH 命令（一次性把全部元素追加进去，保持顺序）。
func listToCmd(obj *model.ListObject) CmdLine {
	cmdLine := make([][]byte, 2+obj.GetElemCount())
	cmdLine[0] = rPushAllCmd
	cmdLine[1] = []byte(obj.Key)
	for i, val := range obj.Values {
		cmdLine[2+i] = val
	}
	return cmdLine
}

var sAddCmd = []byte("SADD")

// setToCmd 把 set 对象转成一条 SADD 命令。
func setToCmd(obj *model.SetObject) CmdLine {
	cmdLine := make([][]byte, 2+obj.GetElemCount())
	cmdLine[0] = sAddCmd
	cmdLine[1] = []byte(obj.GetKey())
	for i, val := range obj.Members {
		cmdLine[2+i] = val
	}
	return cmdLine
}

var hMSetCmd = []byte("HMSET")

// hashToCmd 把 hash 对象转成一条 HMSET 命令（field1 value1 field2 value2 ...）。
// 若 useLexOrder 为 true，则先按 field 字典序排序，保证输出顺序稳定可复现。
func hashToCmd(obj *model.HashObject, useLexOrder bool) CmdLine {
	cmdLine := make([][]byte, 2+obj.GetElemCount()*2)
	cmdLine[0] = hMSetCmd
	cmdLine[1] = []byte(obj.GetKey())
	i := 0
	if useLexOrder {
		entries := make([][2][]byte, 0, 2*len(obj.Hash))
		for field, val := range obj.Hash {
			entries = append(entries, [2][]byte{
				[]byte(field), val,
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			return string(entries[i][0]) < string(entries[j][0])
		})
		for _, entry := range entries {
			cmdLine[2+i*2] = entry[0]
			cmdLine[3+i*2] = entry[1]
			i++
		}
	} else {
		for field, val := range obj.Hash {
			cmdLine[2+i*2] = []byte(field)
			cmdLine[3+i*2] = val
			i++
		}
	}
	return cmdLine
}

var zAddCmd = []byte("ZADD")

// zSetToCmd 把 zset 对象转成一条 ZADD 命令（score1 member1 score2 member2 ...）。
// 注意 score 用 strconv.FormatFloat(..., 'f', -1, 64) 格式化：'-1' 表示用最少的位数表示，
// 避免出现 1.2000000000000002 这种多余的浮点误差尾巴。
func zSetToCmd(obj *model.ZSetObject) CmdLine {
	cmdLine := make([][]byte, 2+obj.GetElemCount()*2)
	cmdLine[0] = zAddCmd
	cmdLine[1] = []byte(obj.GetKey())
	for i, e := range obj.Entries {
		value := strconv.FormatFloat(e.Score, 'f', -1, 64)
		cmdLine[2+i*2] = []byte(value)
		cmdLine[3+i*2] = []byte(e.Member)
	}
	return cmdLine
}

var pExpireAtBytes = []byte("PEXPIREAT")

// MakeExpireCmd generates command line to set expiration for the given key
func makeExpireCmd(obj model.RedisObject) CmdLine {
	expireAt := obj.GetExpiration()
	if expireAt == nil {
		return nil
	}
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(obj.GetKey())
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return args
}

// ObjectToCmd 把一个 Redis 对象转换成「重建它所需的命令列表」。
//
// 大多数对象只需 1 条命令；但带过期时间的对象会额外追加一条 PEXPIREAT 命令，
// 所以返回值是 []CmdLine（命令的切片）。
//
// 支持的类型与命令对应关系：
//
//	string → SET      list → RPUSH    hash → HMSET
//	set    → SADD     zset → ZADD     有 TTL → 追加 PEXPIREAT
func ObjectToCmd(obj model.RedisObject, opts ...interface{}) []CmdLine {
	if obj == nil {
		return nil
	}
	useLexOrder := false
	for _, o := range opts {
		switch o.(type) {
		case lexOrder:
			useLexOrder = true
		}
	}
	cmdLines := make([]CmdLine, 0)
	switch obj.GetType() {
	case model.StringType:
		strObj := obj.(*model.StringObject)
		cmdLines = append(cmdLines, stringToCmd(strObj))
	case model.ListType:
		listObj := obj.(*model.ListObject)
		cmdLines = append(cmdLines, listToCmd(listObj))
	case model.HashType:
		hashObj := obj.(*model.HashObject)
		cmdLines = append(cmdLines, hashToCmd(hashObj, useLexOrder))
	case model.SetType:
		setObj := obj.(*model.SetObject)
		cmdLines = append(cmdLines, setToCmd(setObj))
	case model.ZSetType:
		zsetObj := obj.(*model.ZSetObject)
		cmdLines = append(cmdLines, zSetToCmd(zsetObj))
	}
	if obj.GetExpiration() != nil {
		cmdLines = append(cmdLines, makeExpireCmd(obj))
	}
	return cmdLines
}

// CmdLinesToResp 把多条命令编码成连续的 RESP 字节流（可直接写入 AOF 文件）。
func CmdLinesToResp(cmds []CmdLine) []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	for _, cmdLine := range cmds {
		resp := makeMultiBulkResp(cmdLine)
		buf.Write(resp)
	}
	return buf.Bytes()
}

// WriteObjectToResp 把对象对应的命令以 RESP 格式写入 w。
//
// 与 CmdLinesToResp 的区别：这里是「边拼边写」，对每个参数分段 Write，
// 避免把很大的 key/value 一次性复制进一个大 buffer，内存更省。
func WriteObjectToResp(w io.Writer, obj model.RedisObject) error {
	var err error
	cmdLines := ObjectToCmd(obj)

	for _, cmdLine := range cmdLines {
		argLen := len(cmdLine)
		respBytes := make([][]byte, 0)
		respBytes = append(respBytes, []byte("*"+strconv.Itoa(argLen)+crlf))

		for _, arg := range cmdLine {
			if arg == nil {
				respBytes = append(respBytes, []byte("$-1"+crlf))
				continue
			}
			respBytes = append(respBytes, []byte("$"+strconv.Itoa(len(arg))+crlf))
			// the arg may be a large key or value
			respBytes = append(respBytes, arg)
			respBytes = append(respBytes, []byte(crlf))
		}

		for _, bytes := range respBytes {
			if _, err = w.Write(bytes); err != nil {
				return err
			}
		}
	}
	return nil
}
