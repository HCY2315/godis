package core

import (
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"unsafe"
)

// readBytes 从内存缓冲区 buf 的当前游标 *cursor 处读取 size 个字节。
// 它返回的是 buf 的「子切片」（零拷贝，与原 buf 共享底层数组），并把 *cursor 向前推进 size。
// 典型用途：解析 ziplist/listpack 这类「已整体读入内存」的紧凑结构时，在内存里滑动游标读取。
// 若越界（*cursor+size > len(buf)）则返回错误，避免 panic。
func readBytes(buf []byte, cursor *int, size int) ([]byte, error) {
	if cursor == nil {
		return nil, errors.New("cursor is nil")
	}
	if *cursor+size > len(buf) {
		return nil, errors.New("cursor out of range")
	}
	end := *cursor + size
	result := buf[*cursor:end]
	*cursor += int(size)
	return result, nil
}

// readByte 从内存缓冲区 buf 的当前游标处读取 1 个字节，并令 *cursor 自增。
// 这是「在内存里按字节解析」的工具函数，与 Decoder.readByte（从流里读）区别开来。
func readByte(buf []byte, cursor *int) (byte, error) {
	if cursor == nil {
		return 0, errors.New("cursor is nil")
	}
	if *cursor >= len(buf) {
		return 0, errors.New("cursor out of range")
	}
	b := buf[*cursor]
	*cursor++
	return b, nil
}

// readZipListLength 跳过 ziplist 的 10 字节头部，并读取其中的 zllen 字段（元素个数）。
//
// ziplist 头部布局（共 10 字节）：
//
//	[0:4]  zlbytes —— 整个 ziplist 占用的字节数（uint32，小端）
//	[4:8]  zltail  —— 最后一个 entry 的偏移量（uint32，小端）
//	[8:10] zllen   —— entry 个数（uint16，小端）
//
// 注意：当 zllen == 0xFFFF 时表示「元素个数超过 65535，需遍历才能得到真实数量」，
// 这里直接返回 uint16 读到的值，调用方需自行处理该边界。
// 读取后把 *cursor 推进 10 字节，即越过头部，指向第一个 entry。
func readZipListLength(buf []byte, cursor *int) int {
	start := *cursor + 8
	end := start + 2
	// zip list buf: [0, 4] -> zlbytes, [4:8] -> zltail, [8:10] -> zllen
	size := int(binary.LittleEndian.Uint16(buf[start:end]))
	*cursor += 10
	return size
}

// readByte 从底层字节流（dec.input，一个 bufio.Reader）读取 1 个字节。
// 注意：bufio 内部有 4KB 缓冲，所以这里大多数调用只是「从内存缓冲区取 1 字节」，
// 只有缓冲区耗尽时才会真正触发一次底层 Read（可能是 syscall）。
// readCount 用于统计已读字节数（GetReadCount 可查询）。
func (dec *Decoder) readByte() (byte, error) {
	b, err := dec.input.ReadByte()
	if err != nil {
		return 0, err
	}
	dec.readCount++
	return b, nil
}

// readFull 从字节流中「读满」buf 指定长度的数据（io.ReadFull 保证要么读满、要么返回错误）。
// 用于读取定长字段，如 4/8 字节的整数、长度已知的字符串内容等。
// 读到的字节数累加到 readCount。
func (dec *Decoder) readFull(buf []byte) error {
	n, err := io.ReadFull(dec.input, buf)
	if err != nil {
		return err
	}
	dec.readCount += n
	return nil
}

// letters 是 RandString 使用的字符集（大小写字母 + 数字）。
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString 生成一个长度为 n 的随机字符串（仅用于测试/压测造数，非加密安全）。
// 用法：key := core.RandString(8) // 例如 "aB3xK9zQ"
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// unsafeBytes2Str 用 unsafe 把 []byte「零拷贝」转成 string，避免一次内存分配与复制。
// 这是性能优化手段（RDB 解析时 key/value 大量转换）。
//
// ⚠️ 使用陷阱：转换后的 string 与切片 b 共享同一块底层内存。
//   - 如果之后修改了 b 的内容，这个 string 的内容也会跟着变；
//   - 因此只能用于「b 之后不会再被修改」的场景（例如 b 是刚读出来、独占的缓冲区）。
//   - 若需要长期持有或 b 会被复用，请改用 string(b) 做一次真正的拷贝。
func unsafeBytes2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
