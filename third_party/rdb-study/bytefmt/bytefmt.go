// Package bytefmt 提供「字节数」与「人类可读字符串」之间的相互转换。
//
// 典型用法：
//
//	n, err := bytefmt.ParseSize("1.5M") // n == 1572864
//	s := bytefmt.FormatSize(1572864)    // s == "1.5M"
//
// 本包采用二进制进制（1K = 1024），与 Redis 的 maxmemory 等配置项习惯一致，
// 而不是十进制（1K = 1000）。支持的单位从 B 一直到 E（Exabyte）。
package bytefmt

// from: github.com/cloudfoundry/bytefmt by Apache License

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

const (
	// 利用 iota 依次生成 1024 的各次幂：
	// sizeByte = 1 << (10*0) = 1
	// sizeKilo = 1 << (10*1) = 1024
	// sizeMega = 1 << (10*2) = 1024^2
	// ……以此类推，直到 1024^6。
	// 左移 10 位等价于乘以 1024，因此这里使用的是二进制（1024 进制）而非十进制。
	sizeByte = 1 << (10 * iota)
	sizeKilo
	sizeMega
	sizeGiga
	sizeTera
	sizePeta
	sizeExa
)

// 当输入格式不合法（缺少单位、数值非正、单位无法识别等）时返回该错误。
var errInvalidByteQuantity = errors.New("byte quantity must be a positive integer with a unit of measurement like M, MB, MiB, G, GiB, or GB")

// FormatSize returns a human-readable byte string of the form 10M, 12.5K, and so forth.  The following units are available:
//
//	E: Exabyte
//	P: Petabyte
//	T: Terabyte
//	G: Gigabyte
//	M: Megabyte
//	K: Kilobyte
//	B: Byte
//
// The unit that results in the smallest number greater than or equal to 1 is always chosen.
func FormatSize(bytes uint64) string {
	unit := ""
	value := float64(bytes)

	switch {
	case bytes >= sizeExa:
		unit = "E"
		value = value / sizeExa
	case bytes >= sizePeta:
		unit = "P"
		value = value / sizePeta
	case bytes >= sizeTera:
		unit = "T"
		value = value / sizeTera
	case bytes >= sizeGiga:
		unit = "G"
		value = value / sizeGiga
	case bytes >= sizeMega:
		unit = "M"
		value = value / sizeMega
	case bytes >= sizeKilo:
		unit = "K"
		value = value / sizeKilo
	case bytes >= sizeByte:
		unit = "B"
	case bytes == 0:
		return "0"
	}

	result := strconv.FormatFloat(value, 'f', 1, 64)
	result = strings.TrimSuffix(result, ".0")
	return result + unit
}

// ParseSize parses a string formatted by FormatSize as bytes. Note binary-prefixed and SI prefixed units both mean a base-2 units
// KB = K = KiB = 1024
// MB = M = MiB = 1024 * K
// GB = G = GiB = 1024 * M
// TB = T = TiB = 1024 * G
// PB = P = PiB = 1024 * T
// EB = E = EiB = 1024 * P
func ParseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	i := strings.IndexFunc(s, unicode.IsLetter)

	if i == -1 {
		return 0, errInvalidByteQuantity
	}

	bytesString, multiple := s[:i], s[i:]
	bytes, err := strconv.ParseFloat(bytesString, 64)
	if err != nil || bytes <= 0 {
		return 0, errInvalidByteQuantity
	}

	switch multiple {
	case "E", "EB", "EIB":
		return uint64(bytes * sizeExa), nil
	case "P", "PB", "PIB":
		return uint64(bytes * sizePeta), nil
	case "T", "TB", "TIB":
		return uint64(bytes * sizeTera), nil
	case "G", "GB", "GIB":
		return uint64(bytes * sizeGiga), nil
	case "M", "MB", "MIB":
		return uint64(bytes * sizeMega), nil
	case "K", "KB", "KIB":
		return uint64(bytes * sizeKilo), nil
	case "B":
		return uint64(bytes), nil
	default:
		return 0, errInvalidByteQuantity
	}
}
