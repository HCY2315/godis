// Package encoder 是编码侧的「门面包」（facade）：把 core 包中用于生成
// RDB 的类型与构造函数以别名/变量转发的方式重新导出，为调用方提供稳定入口。
//
// 使用方只需要 import 本包即可获得：
//   - Encoder：RDB 编码器（实际类型是 core.Encoder）；
//   - NewEncoder：创建编码器；
//   - WithTTL：为下一个对象指定过期时间。
//
// 写法与 core.NewEncoder 完全一致，但依赖的是对外稳定的 encoder 路径，
// 底层实现调整时调用方代码无需跟着改动。
package encoder

import "github.com/hdt3213/rdb/core"

// Encoder is used to generate RDB file
// Encoder 是 core.Encoder 的类型别名（不是新类型），因此 core.Encoder 上的
// 所有方法（WriteHeader/WriteAux/WriteDBHeader/WriteStringObject/.../WriteEnd）
// 都可直接在本类型上调用，二者可互换使用。
//
// 使用顺序由内部状态机约束：WriteHeader -> WriteAux* -> WriteDBHeader ->
// 对象写入* -> WriteEnd。违反顺序会返回错误。
type Encoder = core.Encoder

// NewEncoder creates an encoder instance
// NewEncoder 是 core.NewEncoder 的函数变量转发：接收任意 io.Writer，
// 返回 *core.Encoder（即本包的 *Encoder）。
//
// 使用技巧：
//   - 返回值是指针，写方法都返回 error，应逐个检查；
//   - 如需调整 ziplist/listpack 编码阈值，可在创建后调用
//     SetListZipListOpt/SetHashZipListOpt/SetZSetZipListOpt；
//   - 这里以 var 形式导出而非重新包装函数，保证与 core.NewEncoder
//     的签名和行为完全一致。
var NewEncoder = core.NewEncoder

// WithTTL specific expiration timestamp for object
// WithTTL 是 core.WithTTL 的函数变量转发：参数是毫秒级 Unix 时间戳，
// 返回 core.TTLOption。把它作为 WriteXxxObject 的可变参数传入时，
// 编码器会先写入过期时间操作码，再写对象，因此只对紧随其后的对象生效。
//
// 示例：
//
//	expirationMs := uint64(time.Now().Add(time.Hour).Unix() * 1000)
//	err := enc.WriteStringObject("k", []byte("v"), encoder.WithTTL(expirationMs))
var WithTTL = core.WithTTL
