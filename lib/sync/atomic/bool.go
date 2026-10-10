// bool.go：原子布尔值。用于无锁的开关标志（如 Handler.closing、复制 rewriting）。
package atomic

import "sync/atomic"

// Boolean —— 命名含义：原子布尔，读写均线程安全。
type Boolean uint32

// Get reads the value atomically
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// Set writes the value atomically
func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}
