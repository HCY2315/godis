package helper

import (
	"fmt"
	"github.com/hdt3213/rdb/model"
	"regexp"
	"time"
)

// decoder 是解码器的「最小接口」：只要求有 Parse 方法。
// 这样 core.Decoder 以及下面这些「装饰器」都能满足它，
// 从而可以一层层包装（装饰器模式）：比如「先过滤过期 key，再按正则过滤」。
type decoder interface {
	Parse(cb func(object model.RedisObject) bool) error
}

// regexDecoder 是「按 key 正则过滤」的装饰器：
// 它包住内部真正的 decoder，只有当 key 匹配正则时才把对象交给上层回调。
type regexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

// Parse 先调用内部 decoder 解析，再把匹配正则的对象转发给 cb。
// 不匹配的对象返回 true，表示「跳过但继续解析」。
func (d *regexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

// regexWrapper 把 decoder 包装成「带正则过滤」的版本；表达式非法时返回错误。
func regexWrapper(d decoder, expr string) (*regexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal regex expression: %v", expr)
	}
	return &regexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

// RegexOption 是「启用正则过滤」的选项类型（用指针，nil 表示未启用）。
type RegexOption *string

// WithRegexOption 由正则表达式构造一个 RegexOption 选项。
// 用法：helper.MemoryProfile("a.rdb", "a.csv", helper.WithRegexOption("^User:"))
func WithRegexOption(expr string) RegexOption {
	return &expr
}

// noExpiredDecoder 是「过滤已过期 key」的装饰器。
// 注意：RDB 中过期的 key 并不会被删除，解析时仍会读到，所以需要主动过滤。
type noExpiredDecoder struct {
	dec decoder
}

// Parse 只在 key 没有过期时间、或过期时间晚于当前时刻时才转发给 cb。
// now 在开始时取一次，避免每条对象都调用 time.Now()。
func (d *noExpiredDecoder) Parse(cb func(object model.RedisObject) bool) error {
	now := time.Now()
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration == nil || expiration.After(now) {
			return cb(object)
		}
		return true
	})
}

// NoExpiredOption 是「过滤过期 key」的选项类型（true 表示启用）。
type NoExpiredOption bool

// WithNoExpiredOption 构造一个「过滤过期 key」的选项。
func WithNoExpiredOption() NoExpiredOption {
	return NoExpiredOption(true)
}

// wrapDecoder 根据 options 里的选项，把 decoder 逐层包装：
// 先包正则过滤，再包过期过滤。返回最终可用的 decoder。
func wrapDecoder(dec decoder, options ...interface{}) (decoder, error) {
	var regexOpt RegexOption
	var noExpiredOpt NoExpiredOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		case NoExpiredOption:
			noExpiredOpt = o
		}
	}
	if regexOpt != nil {
		var err error
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return nil, err
		}
	}
	if noExpiredOpt {
		dec = &noExpiredDecoder{
			dec: dec,
		}
	}
	return dec, nil
}
