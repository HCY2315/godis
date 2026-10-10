/*
 * 程序入口：启动流程总览
 *   1) 初始化日志
 *   2) 读取配置（环境变量 CONFIG > 工作目录下的 redis.conf > 内置默认值）
 *   3) 构造存储引擎 db（集群 cluster 或 单机 database.Server）
 *   4) 选择网络层（gnet 事件驱动模型 或 std 协程模型）并开始监听
 * 阶段定位：本文件只负责“启动阶段”，请求的真正处理在 redis/server 与 database 中。
 */
package main

import (
	"fmt"
	"os"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/server/gnet"
	stdserver "github.com/hdt3213/godis/redis/server/std"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

// defaultProperties —— 命名含义：内置默认配置。
// 用途：当没有配置文件时使用（注意端口是 6399，不是 redis 的 6379）。
var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	// 【阶段1】初始化日志：输出到 logs/godis-YYYY-MM-DD.log
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// 【阶段2】读取配置：优先级 CONFIG 环境变量 > ./redis.conf > 内置默认值
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	listenAddr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)

	// 【阶段3+4】构造存储引擎并选择网络层
	var err error
	if config.Properties.UseGnet {
		var db idatabase.DB
		if config.Properties.ClusterEnable {
			db = cluster.MakeCluster()
		} else {
			db = database.NewStandaloneServer()
		}
		server := gnet.NewGnetServer(db)
		err = server.Run(listenAddr)
	} else {
		handler := stdserver.MakeHandler()
		err = stdserver.Serve(listenAddr, handler)
	}
	if err != nil {
		logger.Errorf("start server failed: %v", err)
	}
}
