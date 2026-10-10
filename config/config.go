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
	RunID             string `cfg:"runid"` // 每次启动都会变的实例 ID
	Bind              string `cfg:"bind"`  // 监听地址
	Port              int    `cfg:"port"`  // 监听端口
	Dir               string `cfg:"dir"`   // 工作目录（持久化文件、tmp 都放这）
	AnnounceHost      string `cfg:"announce-host"` // 对外宣告地址（NAT 场景用）
	MaxClients        int    `cfg:"maxclients"`    // 最大客户端数（注意：当前实现未真正校验）
	RequirePass       string `cfg:"requirepass"`   // 访问密码
	Databases         int    `cfg:"databases"`     // 库数量，默认 16

	// —— 持久化：AOF / RDB ——
	AppendOnly        bool   `cfg:"appendonly"`          // 是否开启 AOF
	AppendFilename    string `cfg:"appendfilename"`      // AOF 文件名
	AppendFsync       string `cfg:"appendfsync"`         // 落盘策略：always/everysec
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"` // 重写 AOF 时是否用 RDB 做前缀
	RDBFilename       string `cfg:"dbfilename"`          // RDB 文件名

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

// Properties holds global config properties
var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	// A few stats we don't want to reset: server startup time, and peak mem.
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		RunID:      utils.RandString(40),
	}
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
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

	// parse format
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

// SetupConfig read config file and store properties into Properties
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

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
