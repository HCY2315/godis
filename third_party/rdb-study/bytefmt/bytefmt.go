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
//
// 中文说明：把字节数格式化为便于阅读的字符串，例如 1024 -> "1K"、1536 -> "1.5K"。
// 单位从大到小依次尝试，选择能让结果不小于 1 的最小单位；
// 最终最多保留 1 位小数，并且会去掉多余的 ".0"（如 "1.0K" 变成 "1K"）。
// 注意：0 字节是特例，直接返回 "0"（不带单位）。
func FormatSize(bytes uint64) string {
	unit := ""
	// 先转换为 float64，便于后续除以 1024 的幂并保留小数部分。
	value := float64(bytes)

	// 从最大的单位开始向下比较，命中后立即用对应幂次做除法。
	switch {
	case bytes >= sizeExa:
		// 1E = 1024^6 字节
		unit = "E"
		value = value / sizeExa
	case bytes >= sizePeta:
		// 1P = 1024^5 字节
		unit = "P"
		value = value / sizePeta
	case bytes >= sizeTera:
		// 1T = 1024^4 字节
		unit = "T"
		value = value / sizeTera
	case bytes >= sizeGiga:
		// 1G = 1024^3 字节
		unit = "G"
		value = value / sizeGiga
	case bytes >= sizeMega:
		// 1M = 1024^2 字节
		unit = "M"
		value = value / sizeMega
	case bytes >= sizeKilo:
		// 1K = 1024 字节
		unit = "K"
		value = value / sizeKilo
	case bytes >= sizeByte:
		// 不足 1K，直接以字节为单位。
		unit = "B"
	case bytes == 0:
		// 0 字节没有合适的单位，按惯例只返回 "0"。
		return "0"
	}

	// 'f' 表示定点格式，精度 1 表示四舍五入到 1 位小数（不是向上取整）。
	result := strconv.FormatFloat(value, 'f', 1, 64)
	// 整数结果会带 ".0" 后缀，去掉它以保持简洁。
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
