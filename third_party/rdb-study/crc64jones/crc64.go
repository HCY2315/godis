// Package crc64jones implements a 64-bit cyclic redundancy check, or CRC-64,
// checksum. Specifically the Jones flavour of it which is used by Redis.
//
// 中文说明：本包实现 Redis RDB 文件末尾 8 字节校验和所使用的 CRC-64/Jones 变体。
// 解析 RDB 时，读取完全部数据后可以用它重新计算校验和，并与文件尾部保存的值比较，
// 从而判断文件是否完整、是否被截断或篡改。
//
// 该变体的关键参数如下：
//   - 名称：crc-64-jones
//   - 位宽：64 位
//   - 多项式：0xad93d23594c935a9（正常位序）
//   - 输入反射：是（Reflected In = true）
//   - 初始异或值：0xffffffffffffffff（Xor_In）
//   - 输出反射：是（Reflected Out = true）
//   - 输出异或值：0x0（Xor_Out）
//   - 标准测试向量 Check("123456789") = 0xe9c6d914c4b8d9ca
//
// Specification of this CRC64 variant follows:
// - Name: crc-64-jones
// - Width: 64 bites
// - Poly: 0xad93d23594c935a9
// - Reflected In: True
// - Xor_In: 0xffffffffffffffff
// - Reflected_Out: True
// - Xor_Out: 0x0
// - Check("123456789"): 0xe9c6d914c4b8d9ca
package crc64jones

import (
	"hash"
	"hash/crc64"
	"sync"
)

// Predefined polynomials.
// 预定义多项式。Jones 是 Redis 使用的 CRC-64 多项式。
const (
	// The Jones polynomial.
	Jones = 0xad93d23594c935a9
)

// table 是标准的 256 项 CRC 查表。
//
// Go 标准库 hash/crc64 的 MakeTable 接收的是“反射位序”的多项式（例如它内置的
// ECMA 常量就是反射后的形式），而 Jones 常量给出的是正常位序，因此这里先调用
// reflect 将多项式逐位反转，再交给 MakeTable 生成查表。
//
// 查表法原理：CRC 每处理一个字节，等价于把当前余数右移 8 位，再与
// (当前余数低 8 位 XOR 输入字节) 对应的表项异或。表项本身是“该字节在 8 次
// 移位过程中对余数的贡献”预先算好的结果，因此一次查表即可替代 8 次逐位运算。
var table = crc64.MakeTable(reflect(Jones))

// reflect reverses the bit order of the given polynomial.
//
// reflect 将 64 位整数的二进制位顺序完全反转（第 0 位变第 63 位，依此类推）。
// 由于 CRC 规范给出的多项式是正常位序，而 Go 的 crc64.MakeTable 使用反射位序，
// 所以需要在这里做一次位反转。
func reflect(poly uint64) uint64 {
	x := poly & 1
	for i := 1; i < 64; i++ {
		poly >>= 1
		x <<= 1
		x |= poly & 1
	}
	return x
}

var (
	// slicing8TablesBuildOnce 保证 slicing-by-8 所需的 8 张表只构建一次，
	// 即使多个 goroutine 并发调用 update 也是安全的。
	slicing8TablesBuildOnce sync.Once
	// slicing8TableJones 是 Jones 多项式对应的 slicing-by-8 表组。
	slicing8TableJones *[8]crc64.Table
)

// buildSlicing8TablesOnce 使用 sync.Once 惰性初始化 slicing-by-8 表。
// 只有第一次调用时才会真正执行建表逻辑，后续调用直接返回。
func buildSlicing8TablesOnce() {
	slicing8TablesBuildOnce.Do(buildSlicing8Tables)
}

// buildSlicing8Tables 为默认的 Jones 查表构建 slicing-by-8 表组。
func buildSlicing8Tables() {
	slicing8TableJones = makeSlicingBy8Table(table)
}

// makeSlicingBy8Table 基于一张 256 项基础表，推导出一次处理 8 字节所需的 8 张表。
//
// helperTable[0] 就是基础表；helperTable[j][i] 表示“字节 i 经过 j+1 次
// 逐字节更新后的贡献”。这样在处理 8 个字节时，可以分别查 8 张表再异或，
// 把 8 次串行查表合并成 8 次可并行的查表，显著提高大块数据的吞吐。
func makeSlicingBy8Table(t *crc64.Table) *[8]crc64.Table {
	var helperTable [8]crc64.Table
	helperTable[0] = *t
	for i := 0; i < 256; i++ {
		crc := t[i]
		for j := 1; j < 8; j++ {
			// 用零字节继续推进一次，得到再往后一个字节位置的表项。
			crc = t[crc&0xff] ^ (crc >> 8)
			helperTable[j][i] = crc
		}
	}
	return &helperTable
}

// digest represents the partial evaluation of a checksum.
//
// digest 保存 CRC 计算的中间状态，实现 hash.Hash64 接口。
// crc 是当前累计的余数，tab 指向所使用的查表。
type digest struct {
	crc uint64
	tab *crc64.Table
}

// New creates a new hash.Hash64 computing the CRC-64 checksum using the
// Jones polynomial. Its Sum method will lay the value out in little-endian
// byte order.
//
// New 创建一个使用 Jones 多项式的 hash.Hash64。注意：Sum 方法返回的 8 字节
// 是小端序（低字节在前），这与 Redis RDB 文件尾部保存校验和的方式一致。
//
// 用法示例：
//
//	h := crc64jones.New()
//	_, _ = h.Write(data)
//	sum := h.Sum64() // 直接拿 uint64
//	// 或者：raw := h.Sum(nil) // 8 字节小端序
func New() hash.Hash64 { return &digest{0, table} }

// Size 返回校验和的字节数，CRC-64 固定为 8 字节。
func (d *digest) Size() int { return crc64.Size }

// BlockSize 返回建议的写入块大小。CRC 按字节流处理，这里返回 1，
// 表示没有额外的块对齐要求。
func (d *digest) BlockSize() int { return 1 }

// Reset 将累计的 CRC 状态清零，使 digest 可以重新用于计算另一段数据。
func (d *digest) Reset() { d.crc = 0 }

// update 是 CRC-64 的核心更新函数：把 p 中的字节追加到当前 crc 之后，
// 返回新的 crc 值。tab 为所使用的 256 项基础查表。
func update(crc uint64, tab *crc64.Table, p []byte) uint64 {
	buildSlicing8TablesOnce()
	// Table comparison is somewhat expensive, so avoid it for small sizes
	//
	// 比较两张表是否相同有一定开销，因此只有数据量足够大（>=64 字节）时才
	// 尝试启用 slicing-by-8；小数据直接走后面的逐字节循环。
	for len(p) >= 64 {
		var helperTable *[8]crc64.Table
		if *tab == slicing8TableJones[0] {
			// 使用的是内置 Jones 表，直接复用已经构建好的 8 张表。
			helperTable = slicing8TableJones
		} else if len(p) >= 2048 {
			// According to the tests between various x86 and arm CPUs, 2k is a reasonable
			// threshold for now. This may change in the future.
			//
			// 对于自定义表，只有数据量 >=2048 字节时才值得临时构建 8 张表；
			// 否则建表成本可能超过收益。2KB 是根据 x86/ARM 实测得到的经验阈值。
			helperTable = makeSlicingBy8Table(tab)
		} else {
			// 数据量不够大且不是内置表，退回逐字节处理。
			break
		}
		// Update using slicing-by-8
		//
		// slicing-by-8：每轮取 8 个字节，按小端序拼成一个 uint64 后与当前 crc
		// 异或，然后分别用 crc 的每个字节查对应的表，再把 8 个结果异或起来。
		// 这等价于连续执行 8 次逐字节查表，但减少循环与依赖链长度。
		//
		// 这里使用 len(p) > 8（而不是 >=8），把最后最多 8 个字节留给后面的
		// 逐字节循环处理，避免越界并简化收尾逻辑。
		for len(p) > 8 {
			crc ^= uint64(p[0]) | uint64(p[1])<<8 | uint64(p[2])<<16 | uint64(p[3])<<24 |
				uint64(p[4])<<32 | uint64(p[5])<<40 | uint64(p[6])<<48 | uint64(p[7])<<56
			crc = helperTable[7][crc&0xff] ^
				helperTable[6][(crc>>8)&0xff] ^
				helperTable[5][(crc>>16)&0xff] ^
				helperTable[4][(crc>>24)&0xff] ^
				helperTable[3][(crc>>32)&0xff] ^
				helperTable[2][(crc>>40)&0xff] ^
				helperTable[1][(crc>>48)&0xff] ^
				helperTable[0][crc>>56]
			p = p[8:]
		}
	}
	// For reminders or small sizes
	//
	// 收尾：对剩余不足 8 字节的数据（或未启用 slicing-by-8 的小数据）逐字节更新。
	// 每一步都是“查表 + 右移 8 位”的标准反射 CRC 计算。
	for _, v := range p {
		crc = tab[byte(crc)^v] ^ (crc >> 8)
	}
	return crc
}

// Update returns the result of adding the bytes in p to the crc.
//
// Update 把 p 追加到已有 crc 之后并返回新值，适合分段计算：
// 可以先算第一段的 crc，再把该值作为下一段的初始 crc 继续计算。
func Update(crc uint64, tab *crc64.Table, p []byte) uint64 {
	return update(crc, tab, p)
}

// Write 将 p 追加到 digest 的内部状态中。它总是返回 len(p), nil，
// 以满足 hash.Hash 接口（写入不会失败）。
func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = update(d.crc, d.tab, p)
	return len(p), nil
}

// Sum64 返回当前累计的 CRC-64 值（uint64 形式）。
func (d *digest) Sum64() uint64 { return d.crc }

// Sum 把当前校验和以小端序追加到 in 后面并返回。
// 小端序意味着最低有效字节在最前面，这是 Redis RDB 文件尾部校验和的存储顺序。
func (d *digest) Sum(in []byte) []byte {
	s := d.Sum64()
	// Compared to the core hash functions we return in little endian byte order.
	return append(in, byte(s), byte(s>>8), byte(s>>16), byte(s>>24), byte(s>>32), byte(s>>40), byte(s>>48), byte(s>>56))
}

// Checksum returns the CRC-64 checksum of data
// using the polynomial represented by the [Table].
//
// Checksum 是一次性计算整段数据 CRC 的便捷函数，内部等价于
// update(0, tab, data)。注意初始值为 0 而不是 Xor_In，因为本包的
// update 实现采用的反射算法已经隐含了相应的处理方式。
func Checksum(data []byte, tab *crc64.Table) uint64 { return update(0, tab, data) }
