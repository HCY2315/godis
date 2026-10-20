// Package lzf 实现 LZF 压缩/解压算法。
//
// LZF 是一种非常轻量、解压速度极快的无损压缩算法，Redis 在 RDB 中保存字符串
// 时可选使用它（RDB 编码类型中的 LZF 压缩字符串）。它的格式没有固定文件头，
// 而是一串“控制字节 + 数据”的指令流：
//
//   - 控制字节高 3 位表示回溯引用的长度（0 表示纯字面量 run）；
//   - 控制字节低 5 位表示回溯引用偏移的高 5 位；
//   - 字面量 run：控制字节值 ctrl < 32，表示接下来有 ctrl+1 个原样字节；
//   - 回溯引用：从当前输出位置往前回退一段距离，复制若干字节。
//
// 本文件是对 https://github.com/zhuyie/golzf 的移植（MIT 协议），
// 保持了与原实现一致的行为。
package lzf

import "errors"

const (
	// htabLog 是压缩时哈希表的位数：14 位，即 2^14 = 16384 个槽位。
	htabLog uint32 = 14
	// htabSize 是哈希表大小，等于 1<<htabLog。
	htabSize uint32 = 1 << htabLog
	// maxLit 是单段字面量 run 的最大长度。控制字节用 5 位表示长度减一，
	// 因此最多一次连续输出 32 个未压缩字节。
	maxLit = 1 << 5
	// maxOff 是回溯引用偏移的上限（13 位）。偏移量 off 小于该值才可编码；
	// off 表示“距离当前输入位置往前多少个字节”，实际编码时存的是 off。
	maxOff = 1 << 13
	// maxRef 是单次回溯引用可复制的最大字节数。
	// 长度字段基础 3 位最多表示 7，超过 7 时再跟一个扩展字节（最大 255），
	// 解压时复制 length+2 个字节，所以最大匹配长度为 (7+255)+2 = 264。
	maxRef = (1 << 8) + (1 << 3)
)

var (
	// errInsufficientBuffer 表示输出缓冲区不够大（压缩结果放不下或解压越界）。
	errInsufficientBuffer = errors.New("insufficient buffer")
	// errDataCorruption 表示输入数据不符合 LZF 格式（控制字节或回溯引用越界）。
	errDataCorruption = errors.New("data corruption")
)

// using https://github.com/zhuyie/golzf according to MIT license
// Decompress decompress lzf compressed data
//
// Decompress 将 input 的前 inLen 个字节按 LZF 格式解压，期望解压后长度为 outLen。
// 返回值是实际解压出的数据切片（长度为实际写出的字节数）。若压缩流为空，
// 返回 (nil, nil)。
//
// 参数说明：
//   - input：压缩数据所在的缓冲区，函数只会读取前 inLen 个字节；
//   - inLen：input 中有效压缩数据的长度；
//   - outLen：预先知道的解压后长度（LZF 本身不携带原始长度，调用方需要提供）。
func Decompress(input []byte, inLen int, outLen int) ([]byte, error) {
	input = input[:inLen]
	output := make([]byte, outLen)
	var inputIndex, outputIndex int

	inputLength := len(input)
	outputLength := len(output)
	if inputLength == 0 {
		return nil, nil
	}

	// 主循环：每次读取一个控制字节，决定是“字面量 run”还是“回溯引用”。
	for inputIndex < inputLength {
		ctrl := int(input[inputIndex])
		inputIndex++

		if ctrl < (1 << 5) { /* literal run */
			// 控制字节高 3 位为 0，说明后面是原样字节。
			// ctrl 的 5 位存的是“长度 - 1”，所以实际长度为 ctrl+1。
			ctrl++

			// 先检查输出缓冲区是否足够，避免写出越界。
			if outputIndex+ctrl > outputLength {
				return nil, errInsufficientBuffer
			}

			// 再检查输入是否还有足够的字面量字节。
			if inputIndex+ctrl > inputLength {
				return nil, errDataCorruption
			}

			// 直接整段复制字面量。
			copy(output[outputIndex:outputIndex+ctrl], input[inputIndex:inputIndex+ctrl])
			inputIndex += ctrl
			outputIndex += ctrl

		} else { /* back reference */
			// 回溯引用：高 3 位是长度，低 5 位是偏移的高位。
			length := ctrl >> 5
			// 先还原偏移的高位部分：((ctrl & 0x1f) << 8)，
			// 再减 1 是因为编码时存的是“距离 - 1”，最后还要减去低 8 位。
			ref := outputIndex - ((ctrl & 0x1f) << 8) - 1

			// 长度字段为 7 时，需要再读一个扩展字节来延长匹配长度。
			if inputIndex >= inputLength {
				return nil, errDataCorruption
			}

			if length == 7 {
				length += int(input[inputIndex])
				inputIndex++

				if inputIndex >= inputLength {
					return nil, errDataCorruption
				}
			}

			// 再减去偏移的低 8 位，得到最终回溯起点。
			ref -= int(input[inputIndex])
			inputIndex++

			// 解压时实际复制 length+2 个字节（长度字段存的是“长度 - 2”）。
			if outputIndex+length+2 > outputLength {
				return nil, errDataCorruption
			}

			// 回溯起点不能为负，否则说明数据损坏。
			if ref < 0 {
				return nil, errDataCorruption
			}

			// Can't use copy(...) here, because it has special handling when source and destination overlap.
			//
			// 这里不能用内置 copy：当源区间与目标区间重叠时，copy 会先快照源数据，
			// 而 LZF 的回溯复制需要“边复制边扩展”的 run-length 语义。
			// 例如 ref 指向刚输出的重复模式时，逐字节复制才能正确生成重复内容。
			for i := 0; i < length+2; i++ {
				output[outputIndex+i] = output[ref+i]
			}
			outputIndex += length + 2
		}
	}

	// 压缩流可能比调用方给的 outLen 短，这里按实际写出的长度截断。
	return output[:outputIndex], nil
}

// Compress compress data using lzf algorithm
//
// Compress 使用 LZF 算法压缩 input，返回压缩后的字节切片。
// 压缩结果没有头部信息，解压时必须由调用方提供原始长度（见 Decompress）。
//
// 输出缓冲区按输入长度分配。如果数据本身不可压缩，压缩结果可能比输入还大，
// 此时函数会返回 errInsufficientBuffer，而不是写出越界。
func Compress(input []byte) ([]byte, error) {
	var hval, ref, hslot, off uint32
	var inputIndex, outputIndex, lit int
	output := make([]byte, len(input))

	// htab 保存每个哈希槽最近一次出现的输入位置，用于查找可回溯的重复串。
	htab := make([]uint32, htabSize)
	inputLength := len(input)
	if inputLength == 0 {
		return nil, nil
	}
	outputLength := len(output)
	if outputLength == 0 {
		return nil, errInsufficientBuffer
	}

	lit = 0 /* start run */
	// 预留第一个字节作为字面量 run 的控制字节；真正的控制值在 run 结束时回填。
	outputIndex++

	// 用前两个字节初始化滑动窗口，之后每轮把窗口右移一个字节（见循环内 hval 更新）。
	hval = uint32(input[inputIndex])<<8 | uint32(input[inputIndex+1])
	for inputIndex < inputLength-2 {
		// 取最近 3 个字节构成的 24 位窗口值。
		hval = (hval << 8) | uint32(input[inputIndex+2])
		// LZF 的快速哈希：保留窗口高 14 位，再减去 hval*5 做混合，
		// 最后用 & (htabSize-1) 截断到 14 位，得到哈希槽。
		hslot = ((hval >> (3*8 - htabLog)) - hval*5) & (htabSize - 1)
		// ref 是同一哈希槽上一次出现的位置；0 被当作“空槽”的哨兵值。
		ref = htab[hslot]
		htab[hslot] = uint32(inputIndex)
		// off 是当前输入位置与上次位置的距离减一。
		off = uint32(inputIndex) - ref - 1

		// 只有同时满足以下条件才认为找到可用的匹配：
		//   1. 偏移在 13 位可编码范围内；
		//   2. ref 非 0（0 表示该槽还没有有效记录）；
		//   3. 当前位置与 ref 位置的前 3 个字节完全相同。
		if off < maxOff &&
			(ref > 0) &&
			(input[ref] == input[inputIndex]) &&
			(input[ref+1] == input[inputIndex+1]) &&
			(input[ref+2] == input[inputIndex+2]) {

			/* match found at *ref++ */
			// len 从 2 开始，因为前面已经确认了 3 个字节中的后两个（索引 1、2），
			// 循环会继续向后比较，最终 len 等于实际匹配长度。
			len := 2
			maxLen := inputLength - inputIndex - len
			if maxLen > maxRef {
				maxLen = maxRef
			}

			// 保守地检查输出空间：匹配最多编码成 3 字节（控制 + 扩展长度 + 偏移低字节），
			// 外加可能需要收尾的字面量控制字节。
			if outputIndex+3+1 >= outputLength { /* first a faster conservative test */
				nlit := 0
				if lit == 0 {
					nlit = 1
				}
				if outputIndex-nlit+3+1 >= outputLength { /* second the exact but rare test */
					return nil, errInsufficientBuffer
				}
			}

			// 结束当前字面量 run：在 run 起始位置回填“长度 - 1”。
			output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
			if lit == 0 {
				outputIndex-- /* undo run if length is zero */
			}

			// 继续向后比较，直到达到上限或遇到不同字节。
			for {
				len++
				if (len >= maxLen) || (input[int(ref)+len] != input[inputIndex+len]) {
					break
				}
			}

			len -= 2 /* len is now #octets - 1 */
			inputIndex++

			// 写控制字节：高 3 位是长度，低 5 位是偏移高位。
			// len < 7 时长度直接放在控制字节里；否则控制字节写 7，
			// 再跟一个扩展字节保存 len-7。
			if len < 7 {
				output[outputIndex] = byte((off >> 8) + uint32(len<<5))
				outputIndex++
			} else {
				output[outputIndex] = byte((off >> 8) + (7 << 5))
				output[outputIndex+1] = byte(len - 7)
				outputIndex += 2
			}

			// 写偏移低 8 位。
			// 这里 outputIndex 额外前进 2：低字节之后空出一个位置，作为下一段
			// run 的控制字节槽位；若下一段仍是回溯引用，后续写控制字节时会回填它，
			// 若下一段是字面量，字面量也会从该槽位之后开始写。
			output[outputIndex] = byte(off)
			outputIndex += 2
			lit = 0 /* start run */

			// 跳过已匹配的字节。
			inputIndex += len + 1

			if inputIndex >= inputLength-2 {
				break
			}

			inputIndex -= 2

			// 匹配结束后，把紧邻的两个位置重新写入哈希表，
			// 避免因为跳过匹配区域而漏掉后续可用的重复串。
			hval = uint32(input[inputIndex])<<8 | uint32(input[inputIndex+1])
			hval = (hval << 8) | uint32(input[inputIndex+2])
			hslot = ((hval >> (3*8 - htabLog)) - (hval * 5)) & (htabSize - 1)
			htab[hslot] = uint32(inputIndex)
			inputIndex++

			hval = (hval << 8) | uint32(input[inputIndex+2])
			hslot = ((hval >> (3*8 - htabLog)) - (hval * 5)) & (htabSize - 1)
			htab[hslot] = uint32(inputIndex)
			inputIndex++

		} else {
			/* one more literal byte we must copy */
			// 没有找到匹配：把当前字节作为字面量输出。
			if outputIndex >= outputLength {
				return nil, errInsufficientBuffer
			}

			lit++
			output[outputIndex] = input[inputIndex]
			outputIndex++
			inputIndex++

			// 字面量 run 达到 32 个时，回填控制字节并开始新的一段 run。
			if lit == maxLit {
				output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
				lit = 0                                   /* start run */
				outputIndex++
			}
		}
	}

	// 主循环结束后，末尾可能还剩不到 3 个字节，无法参与匹配，只能作为字面量输出。
	if outputIndex+3 >= outputLength { /* at most 3 bytes can be missing here */
		return nil, errInsufficientBuffer
	}

	for inputIndex < inputLength {
		lit++
		output[outputIndex] = input[inputIndex]
		outputIndex++
		inputIndex++

		if lit == maxLit {
			output[outputIndex-lit-1] = byte(lit - 1) /* stop run */
			lit = 0                                   /* start run */
			outputIndex++
		}
	}

	// 回填最后一段字面量 run 的控制字节。
	output[outputIndex-lit-1] = byte(lit - 1) /* end run */
	if lit == 0 {                             /* undo run if length is zero */
		outputIndex--
	}

	return output[:outputIndex], nil
}
