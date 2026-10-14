package config

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/lib/utils"

	"github.com/hdt3213/godis/lib/logger"
)

var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

/*
 * ServerProperties —— 命名含义：服务端配置项集合。
 * 用途：把配置文件里的 key=value 通过结构体 tag 反射填充进来（见下方 parse）。
 * 配置来源优先级：环境变量 CONFIG 指定的文件 > 工作目录 redis.conf > 内置默认值。
 * 字段分四大类：公共/网络、持久化(AOF/RDB)、慢日志、集群与复制。
 */
type ServerProperties struct {
	// —— 公共 / 网络 ——
	RunID        string `cfg:"runid"`         // 每次启动都会变的实例 ID
	Bind         string `cfg:"bind"`          // 监听地址
	Port         int    `cfg:"port"`          // 监听端口
	Dir          string `cfg:"dir"`           // 工作目录（持久化文件、tmp 都放这）
	AnnounceHost string `cfg:"announce-host"` // 对外宣告地址（NAT 场景用）
	MaxClients   int    `cfg:"maxclients"`    // 最大客户端数（注意：当前实现未真正校验）
	RequirePass  string `cfg:"requirepass"`   // 访问密码
	Databases    int    `cfg:"databases"`     // 库数量，默认 16

	// —— 持久化：AOF / RDB ——
	AppendOnly        bool   `cfg:"appendonly"`           // 是否开启 AOF
	AppendFilename    string `cfg:"appendfilename"`       // AOF 文件名
	AppendFsync       string `cfg:"appendfsync"`          // 落盘策略：always(每条命令fsync,最安全最慢)/everysec(每秒fsync,推荐)/no(交给OS)
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"` // 重写 AOF 时是否用 RDB 做前缀
	RDBFilename       string `cfg:"dbfilename"`           // RDB 文件名

	// —— 主从复制 ——
	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`

	// —— 网络层选择 ——
	UseGnet bool `cfg:"use-gnet"` // true=用 gnet 事件驱动模型；false=用 std 协程模型

	// —— 慢日志 ——
	SlowLogSlowerThan int64 `cfg:"slowlog-log-slower-than"` // 超过多少微秒记为慢命令
	SlowLogMaxLen     int   `cfg:"slowlog-max-len"`         // 慢日志最多保留多少条

	// —— 集群 ——
	ClusterEnable     bool   `cfg:"cluster-enable"`      // 是否以集群模式启动
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`     // 是否作为种子节点加入集群
	ClusterSeed       string `cfg:"cluster-seed"`        // 种子节点地址
	RaftListenAddr    string `cfg:"raft-listen-address"` // raft 一致性协议监听地址
	RaftAdvertiseAddr string `cfg:"raft-advertise-address"`
	// 以副本身份加入集群时，填主节点的 RedisAdvertiseAddr
	MasterInCluster string `cfg:"master-in-cluster"`
}

var configFilePath string

func GetConfigFilePath() string {
	return configFilePath
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	if p.AnnounceHost != "" {
		return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
	}
	return p.Bind + ":" + strconv.Itoa(p.Port)
}

func (p *ServerProperties) RaftAnnounceAddress() string {
	if p.RaftAdvertiseAddr != "" {
		return p.RaftAdvertiseAddr
	}
	return p.RaftListenAddr
}

// Properties —— 命名含义：全局配置单例。
// 用途：整个程序通过 config.Properties.XXX 读取配置，避免层层传参。
var Properties *ServerProperties

// EachTimeServerInfo —— 命名含义：每次运行的服务器信息（不随 flush 重置）。
// 用途：目前记录启动时间，供 INFO 命令展示 uptime。
var EachTimeServerInfo *ServerInfo

func init() {
	// 启动时间等统计信息不应该被 flush/reset 清掉
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// 【默认配置】没有配置文件时使用（默认面向本机 6379）
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		RunID:      utils.RandString(40),
	}
}

// parse —— 用途：把配置文件的文本解析成 ServerProperties。
// 实现分两步：1) 逐行按空格切成 key/value 存进 rawMap；2) 用反射遍历结构体字段，
// 依据 `cfg:"..."` tag 把字符串值按字段类型（string/int/bool/slice）set 进去。
// 这也是为什么新增配置项“只需给结构体加字段和 tag”。
func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// 第一步：读取文件，跳过注释行，按第一个空格切成 key / value
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// 第二步：反射按 tag 填充字段（支持 string/int/int64/bool/[]string）
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Int64:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

// SetupConfig —— 用途：读取指定配置文件并填充全局 Properties，同时生成新的 RunID。
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
	Properties.RunID = utils.RandString(40)
	configFilePath, err = filepath.Abs(configFilename)
	if err != nil {
		return
	}
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
}

// GetTmpDir —— 用途：返回临时目录（AOF 重写、RDB 生成等临时文件都放这）。
func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
