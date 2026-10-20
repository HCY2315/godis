// Package main 是 rdb 库的命令行入口程序（编译后即 `rdb` 命令）。
//
// 它把具体工作全部委托给 helper 包：解析 RDB 文件并输出 JSON、内存报告、
// AOF、大 key 排行、前缀分析报告，或启动一个 Web 服务绘制火焰图。
// 本文件只负责：解析命令行参数、把参数翻译成 helper 的 options、
// 把结果写到文件或标准输出。
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/hdt3213/rdb/helper"
)

// help 是 `rdb` 不带 -c 参数（或需要查看帮助）时打印的英文帮助文本。
// 内容包含：参数列表、各参数含义、以及 6 个子命令的用法示例。
// 注意：这是程序输出的一部分，修改时需与 README 中的用法说明保持一致。
const help = `
This is a tool to parse Redis' RDB files
Options:
  -c command, including: json/memory/aof/bigkey/prefix/flamegraph
  -o output file path
  -n number of result, using in command: bigkey/prefix
  -port listen port for flame graph web service
  -sep separator for flamegraph, rdb will separate key by it, default value is ":". 
		supporting multi separators: -sep sep1 -sep sep2 
  -regex using regex expression filter keys
  -no-expired filter expired keys

Examples:
parameters between '[' and ']' is optional
1. convert rdb to json
  rdb -c json -o dump.json dump.rdb
2. generate memory report
  rdb -c memory -o memory.csv dump.rdb
3. convert to aof file
  rdb -c aof -o dump.aof dump.rdb
4. get largest keys
  rdb -c bigkey [-o dump.aof] [-n 10] dump.rdb
5. get number and memory size by prefix
  rdb -c prefix [-n 10] [-max-depth 3] [-o prefix-report.csv] dump.rdb
6. draw flamegraph
  rdb -c flamegraph [-port 16379] [-sep :] dump.rdb
`

// separators 是 flag.Value 的实现，用来支持多次传入 -sep。
//
// Go 标准库的 flag 对同一参数重复出现时只会保留最后一个值，而火焰图需要
// 同时指定多个分隔符（例如 `-sep : -sep .`）。把该类型注册给 flagSet.Var 后，
// 每次遇到 -sep 都会调用 Set，从而把多个分隔符依次追加到切片中。
type separators []string

// String 返回用空格连接后的分隔符列表，仅用于打印 flag 的默认值/当前值，
// 不参与火焰图的实际分割逻辑。
func (s *separators) String() string {
	return strings.Join(*s, " ")
}

// Set 实现 flag.Value 接口：每出现一次 -sep 就把 value 追加到切片末尾。
// 该方法返回 nil 表示参数总能被接受；分隔符是否有效由后续 helper 处理。
func (s *separators) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// main 是 CLI 的入口，整体流程为：
//  1. 定义并解析命令行参数（-c/-o/-n/-port/-sep/-regex/-no-expired/-max-depth）；
//  2. 校验必填项：-c 与源 RDB 文件路径；
//  3. 把 -regex、-no-expired 翻译成 helper 的 options；
//  4. 打开输出目标（bigkey/prefix 用 *os.File，未指定 -o 时写标准输出）；
//  5. 按 -c 的值分派到对应 helper，并统一打印错误。
//
// 完整用法示例（方括号内为可选参数）：
//
//	# 1. 把 RDB 转成 JSON
//	rdb -c json -o dump.json dump.rdb
//	# 2. 生成内存占用报告（CSV）
//	rdb -c memory -o memory.csv dump.rdb
//	# 3. 把 RDB 转成 AOF
//	rdb -c aof -o dump.aof dump.rdb
//	# 4. 找出最大的 N 个 key
//	rdb -c bigkey [-o bigkey.csv] [-n 10] dump.rdb
//	# 5. 按 key 前缀统计数量与内存
//	rdb -c prefix [-n 10] [-max-depth 3] [-o prefix-report.csv] dump.rdb
//	# 6. 绘制火焰图（启动 Web 服务后阻塞等待，Ctrl+C 退出）
//	rdb -c flamegraph [-port 16379] [-sep :] dump.rdb
//	# 7. 组合过滤：只处理 key 匹配正则且未过期的对象
//	rdb -c json -o filtered.json -regex '^user:.*' -no-expired dump.rdb
//
// 使用技巧：
//   - json/memory/aof 必须提供 -o，因为对应 helper 会自行创建输出文件；
//   - bigkey/prefix 的 -n 必须大于 0，未指定 -o 时 CSV 直接打印到标准输出；
//   - -regex 使用 Go regexp 语义（默认是“包含匹配”，需要整串匹配时用 ^...$）；
//   - -no-expired 表示过滤掉已过期的 key，只保留未过期或没有 TTL 的 key；
//   - -sep 可重复出现，用于火焰图按多个分隔符拆分 key；
//   - 源文件路径必须是最后一个非 flag 参数（flagSet.Arg(0)）。
func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// cmd 对应 -c，子命令名：json/memory/aof/bigkey/prefix/flamegraph。
	var cmd string
	// output 对应 -o，输出文件路径；bigkey/prefix 为空时输出到标准输出。
	var output string
	// n 对应 -n，bigkey/prefix 取前 N 条结果，必须大于 0。
	var n int
	// port 对应 -port，火焰图 Web 服务监听端口；0 表示使用 helper 的默认端口 16379。
	var port int
	// seps 对应可重复的 -sep，火焰图按这些分隔符拆分 key。
	var seps separators
	// regexExpr 对应 -regex，用于按正则过滤 key。
	var regexExpr string
	// noExpired 对应 -no-expired，为 true 时跳过已过期的 key。
	var noExpired bool
	// maxDepth 对应 -max-depth，限制前缀分析树的最大深度。
	var maxDepth int
	var err error

	// 注册全部命令行参数。这里通过 StringVar/IntVar 等把参数直接绑定到上面的变量，
	// 因此后续无需再手动读取 flagSet.Lookup 的值。
	flagSet.StringVar(&cmd, "c", "", "command for rdb: json")
	flagSet.StringVar(&output, "o", "", "output file path")
	flagSet.IntVar(&n, "n", 0, "")
	flagSet.IntVar(&maxDepth, "max-depth", 0, "max depth of prefix tree")
	flagSet.IntVar(&port, "port", 0, "listen port for web")
	flagSet.Var(&seps, "sep", "separator for flame graph")
	flagSet.StringVar(&regexExpr, "regex", "", "regex expression")
	flagSet.BoolVar(&noExpired, "no-expired", false, "filter expired keys")
	// flag.ExitOnError 表示参数非法时直接以非零码退出，因此这里可以忽略 Parse 的返回值。
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError
	// 第一个非 flag 位置参数是 RDB 源文件路径。
	src := flagSet.Arg(0)

	// -c 是必填项：缺省时打印帮助并正常退出（退出码 0）。
	if cmd == "" {
		println(help)
		return
	}
	// 源 RDB 文件路径同样是必填项。
	if src == "" {
		println("src file is required")
		return
	}

	// helper 的各个入口都通过 options ...interface{} 接收可选过滤器，
	// 这里把命令行开关翻译成对应的 option 值。
	var options []interface{}
	if regexExpr != "" {
		// 仅在显式提供正则时追加，避免空表达式带来的无意义包装。
		options = append(options, helper.WithRegexOption(regexExpr))
	}
	if noExpired {
		options = append(options, helper.WithNoExpiredOption())
	}

	// outputFile 供 bigkey/prefix 使用：未指定 -o 时写标准输出，否则创建文件。
	// 注意 json/memory/aof 三个子命令调用 helper 时传的是路径字符串 output，
	// 由 helper 自己创建文件，因此这几种模式下必须提供 -o。
	var outputFile *os.File
	if output == "" {
		outputFile = os.Stdout
	} else {
		outputFile, err = os.Create(output)
		if err != nil {
			fmt.Printf("open output faild: %v", err)
		}
		defer func() {
			_ = outputFile.Close()
		}()
	}

	// 按 -c 分派子命令。每个分支只负责调用 helper 并把错误汇总到 err，
	// 具体解析/输出逻辑都在 helper 包中。
	switch cmd {
	case "json":
		// 将 RDB 转为 JSON 文件；options 用于正则/过期过滤。
		err = helper.ToJsons(src, output, options...)
	case "memory":
		// 生成内存占用报告 CSV；options 用于正则/过期过滤。
		err = helper.MemoryProfile(src, output, options...)
	case "aof":
		// 将 RDB 转为 AOF（Redis 序列化协议）；options 同样可过滤 key。
		// 这里把 options 切片作为整体传入（而非展开），是本命令的既有调用方式。
		err = helper.ToAOF(src, output, options)
	case "bigkey":
		// 找出最大的 N 个 key，CSV 写入 outputFile（未指定 -o 时为标准输出）。
		err = helper.FindBiggestKeys(src, n, outputFile, options...)
	case "prefix":
		// 按分隔符前缀聚合统计 key 数量与内存，maxDepth 限制前缀树深度。
		err = helper.PrefixAnalyse(src, n, maxDepth, outputFile, options...)
	case "flamegraph":
		// 启动 Web 服务展示火焰图。helper 返回后服务已在后台运行，
		// 主 goroutine 通过阻塞在一个永不关闭的 channel 上保持进程存活。
		_, err = helper.FlameGraph(src, port, seps, options...)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		<-make(chan struct{})
	default:
		// 未知子命令：提示后直接返回，不尝试解析源文件。
		println("unknown command")
		return
	}
	// 统一处理 helper 返回的错误。
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
}
