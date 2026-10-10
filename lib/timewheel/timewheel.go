/*
 * 时间轮：高效管理大量“延迟任务/定时任务”的经典结构。
 * 用途：godis 用它实现 key 过期——每个键到期时在时间轮里注册一个回调。
 * 原理：把时间划分成若干槽(slot)，指针随时间推进，到期就触发该槽里的任务，
 * 比“为每个键起一个 timer”更省资源。
 * 命名说明：TimeWheel 时间轮，task 一个延迟任务，location 任务在轮中的位置。
 */
package timewheel

import (
	"container/list"
	"github.com/hdt3213/godis/lib/logger"
	"sync"
	"time"
)

// location —— 命名含义：任务在时间轮中的位置（哪个槽、对应哪个链表元素）。
type location struct {
	slot  int
	etask *list.Element
}

// TimeWheel —— 命名含义：时间轮。
type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	timer             map[string]*location
	currentPos        int
	slotNum           int
	addTaskChannel    chan task
	removeTaskChannel chan string
	stopChannel       chan bool

	mu sync.RWMutex
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()
	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts the time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob adds a new job to the pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	tw.mu.Lock()
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	tw.mu.Unlock()

	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	var tasksToRemove []string
	tw.mu.RLock() // Read lock for accessing the list
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go func(job func()) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job()
		}(task.job)

		if task.key != "" {
			tasksToRemove = append(tasksToRemove, task.key)
		}
		next := e.Next()
		l.Remove(e) // Safe as this is a local operation
		e = next
	}
	tw.mu.RUnlock()

	// Remove tasks from the timer after the scan
	tw.mu.Lock()
	for _, key := range tasksToRemove {
		delete(tw.timer, key)
	}
	tw.mu.Unlock()
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	tw.mu.Lock()
	defer tw.mu.Unlock()

	if task.key != "" {
		if _, ok := tw.timer[task.key]; ok {
			tw.removeTaskInternal(task.key) // Internal version avoids double lock
		}
	}

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum
	return
}

func (tw *TimeWheel) removeTask(key string) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.removeTaskInternal(key)
}

func (tw *TimeWheel) removeTaskInternal(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
