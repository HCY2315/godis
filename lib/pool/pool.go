// pool.go：通用对象池（带最大空闲/最大活跃限制）。
// 用途：集群节点间、主从之间的连接复用，避免频繁建立 TCP 连接。
// 命名说明：Pool 池，factory 创建对象的函数，finalizer 销毁对象的函数。
package pool

import (
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("pool closed")
	ErrMax    = errors.New("reach max connection limit")
)

// request —— 命名含义：等待从池中取对象的请求。
type request chan interface{}

// Config —— 命名含义：池的容量配置。
type Config struct {
	MaxIdle   uint // 最大空闲对象数
	MaxActive uint // 最大活跃对象数
}

// Pool —— 命名含义：对象池，复用如 redis 连接等资源。
type Pool struct {
	Config
	factory     func() (interface{}, error)
	finalizer   func(x interface{})
	idles       chan interface{}
	waitingReqs []request
	activeCount uint // increases during creating connection, decrease during destroying connection
	mu          sync.Mutex
	closed      bool
}

func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
		Config:      cfg,
	}
}

// getOnNoIdle try to create a new connection or waiting for connection being returned
// invoker should have pool.mu
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	pool.mu.Lock()
	if pool.activeCount >= pool.MaxActive {
		// waiting for connection being returned
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		x, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}

	// create a new connection
	pool.activeCount++ // hold a place for new connection
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		// create failed return token
		pool.mu.Lock()
		pool.activeCount-- // release the holding place
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}
	pool.mu.Unlock()

	select {
	case item := <-pool.idles:
		return item, nil
	default:
		// no pooled item, create one
		return pool.getOnNoIdle()
	}
}

func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}

	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1]
		req <- x
		pool.mu.Unlock()
		return
	}

	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		// reach max idle, destroy redundant item
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
	}
}

func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()

	for x := range pool.idles {
		pool.finalizer(x)
	}
}
