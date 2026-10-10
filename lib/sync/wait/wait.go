// wait.go：带超时的 WaitGroup。
// 用途：连接关闭时等待“正在发送的数据”发完（见 connection.Connection.sendingData），
// 避免客户端收到半截响应。相比标准 WaitGroup 多了 WaitWithTimeout。
package wait

import (
	"sync"
	"time"
)

// Wait —— 命名含义：可超时等待的计数器（包装 sync.WaitGroup）。
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		w.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
