/*
 * 协议解析器（v2，基于缓冲的同步解析）。
 * 阶段定位：解析阶段，供 gnet 网络层在 OnTraffic 回调中使用。
 * 与 parser.go 的区别：这里同步返回 [][]byte，不走 channel、不额外起 goroutine，
 * 更适合事件回调模型。注意本实现逐字节 io.ReadFull 读取，是后续零拷贝优化的对象。
 */
package parser

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

// ParseV2 —— 用途：解析一条命令，返回其参数列表（cmdLine）。
// 支持两套协议：以 '*' 开头的 RESP 数组协议，以及纯文本命令（如 "PING\r\n"）。
func ParseV2(r io.Reader) ([][]byte, error) {
	// 读取起始字符 '*'
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	if buf[0] != '*' {
		// try text protocol
		buf2, err := readLine(r)
		if err != nil {
			return nil, err
		}
		buf = append(buf, buf2...)
		line := strings.Split(string(buf), " ")
		result := make([][]byte, len(line))
		for i, s := range line {
			result[i] = []byte(s)
		}
		return result, nil
	}

	// 读取参数数量
	count, err := readInteger(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}

	// 读取每个参数
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		// 读取类型前缀
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}

		switch buf[0] {
		case '$': // Bulk String
			strLen, err := readInteger(r)
			if err != nil {
				return nil, err
			}
			if strLen < 0 {
				result[i] = nil // Null Bulk String
				continue
			}

			data := make([]byte, strLen+2)
			_, err = io.ReadFull(r, data)
			if err != nil {
				return nil, err
			}
			result[i] = data[:strLen]

		// case '+', ':': // Simple String or Integer
		// 	simpleStr, err := readLine(r)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	result[i] = []byte(simpleStr)

		// case '-': // Error
		// 	errStr, err := readLine(r)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	result[i] = []byte(errStr)

		default:
			return nil, errors.New("unsupported RESP type")
		}
	}

	return result, nil
}

func readInteger(r io.Reader) (int, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(line))
}

func readLine(r io.Reader) ([]byte, error) {
	var line []byte
	buf := make([]byte, 1)

	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}

		switch buf[0] {
		case '\r':
			_, err := io.ReadFull(r, buf)
			if err != nil {
				return nil, errors.New("unexpected EOF after \\r")
			}
			if buf[0] != '\n' {
				return nil, errors.New("expected \\n after \\r")
			}
			return line, nil
		default:
			line = append(line, buf[0])
		}
	}
}
