/*
 * 通用 TCP 服务器（std 网络层的底座）。
 * 阶段定位：连接接入阶段的“前半段”——只负责 accept 连接、处理退出信号，
 * 拿到连接后交给 Handler 去读命令（后半段在 redis/server/std）。
 * 这是 Go 最经典的“一连接一 goroutine + runtime netpoller 隐式 IO 多路复用”模型。
 */
package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
)

// Config —— 命名含义：tcp 服务器的配置。
// 用途：监听地址/最大连接/超时。当前只有 Address 被真正使用。
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter —— 命名含义：当前在线客户端计数（原子操作）。
var ClientCounter int32

// ListenAndServeWithSignal —— 用途：绑定端口并在收到退出信号（SIGHUP/INT/TERM...）时优雅关闭。
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe —— 用途：核心 accept 循环。阻塞直到监听出错或被要求关闭。
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 起一个 goroutine 专职等待“退出信号”，收到后关闭 listener 和所有连接
	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	ctx := context.Background()
	// waitDone 用于优雅关闭：等所有连接的 Handle 返回后才退出
	var waitDone sync.WaitGroup
	for {
		// 【关键】阻塞等待一个新连接（Go runtime 底层用 epoll 唤醒）
		conn, err := listener.Accept()
		if err != nil {
			// 借鉴 net/http/serve.go#Serve()：临时错误（如超时）重试，其余报错退出
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		// 【关键】每来一个连接就开一个 goroutine 处理
		// 这正是 std 版“一连接一协程”的核心；百万连接=百万 goroutine，开销大
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn) // 阻塞式地读命令并执行，直到连接断开
		}()
	}
	waitDone.Wait()
}
