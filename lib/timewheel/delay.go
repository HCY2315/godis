// delay.go：时间轮的全局单例与便捷入口。
// 全局 tw：每槽 1 秒、共 3600 槽；key 用于取消任务（如 key 过期时用 key 作为任务名）。
package timewheel

import "time"

// tw —— 全局时间轮实例。
var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

// Delay —— 用途：延迟 duration 后执行 job（key 用于后续取消）。
func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

// At executes job at given time
func At(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel stops a pending job
func Cancel(key string) {
	tw.RemoveJob(key)
}
