package connection

import (
	"fmt"
	"github.com/hdt3213/godis/lib/logger"
	"io"
	"sync"
)

// FakeConn —— 命名含义：假的连接（测试用）。
// 用途：不依赖真实网络，用内存 buffer 模拟收发，方便对命令和服务做单元测试。
// 工作方式：Write 把数据追加到 buf 并唤醒等待者；Read 从 buf 读取，读空了就阻塞等待。
type FakeConn struct {
	Connection
	buf    []byte        // 模拟的接收/发送缓冲区
	offset int           // 已读位置
	waitOn chan struct{} // 读空时用于阻塞等待新数据
	closed bool
	mu     sync.Mutex
}

// NewFakeConn —— 用途：创建测试用连接。
func NewFakeConn() *FakeConn {
	c := &FakeConn{}
	return c
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	c.mu.Lock()
	c.buf = append(c.buf, b...)
	c.mu.Unlock()
	c.notify()
	return len(b), nil
}

func (c *FakeConn) notify() {
	if c.waitOn != nil {
		c.mu.Lock()
		if c.waitOn != nil {
			logger.Debug(fmt.Sprintf("notify %p", c.waitOn))
			close(c.waitOn)
			c.waitOn = nil
		}
		c.mu.Unlock()
	}
}

func (c *FakeConn) wait(offset int) {
	c.mu.Lock()
	if c.offset != offset { // new data during waiting lock
		return
	}
	if c.waitOn == nil {
		c.waitOn = make(chan struct{})
	}
	waitOn := c.waitOn
	logger.Debug(fmt.Sprintf("wait on %p", waitOn))
	c.mu.Unlock()
	<-waitOn
	logger.Debug(fmt.Sprintf("wait on %p finish", waitOn))
}

// Read reads data from buffer
func (c *FakeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	n := copy(p, c.buf[c.offset:])
	c.offset += n
	offset := c.offset
	c.mu.Unlock()
	if n == 0 {
		if c.closed {
			return n, io.EOF
		}
		c.wait(offset)
		// after notify
		if c.closed {
			return n, io.EOF
		}
		n = copy(p, c.buf[c.offset:])
		c.offset += n
		return n, nil
	}
	if c.closed {
		return n, io.EOF
	}
	return n, nil
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.waitOn = make(chan struct{})
	c.buf = nil
	c.offset = 0
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf
}

func (c *FakeConn) Close() error {
	c.closed = true
	c.notify()
	return nil
}

func (c *FakeConn) RemoteAddr() string {
	return ""
}
