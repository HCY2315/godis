package core

import (
	"errors"
	"fmt"
)

// Opcode 是 Redis Module 数据流中的操作码，用于告诉解析器下一个值是什么类型。
//
// Module 类型的 value 不是普通的 Redis 对象编码，而是由 module 自己定义的
// 一串「opcode + payload」；解析时必须按 opcode 逐个读取。
type Opcode uint8

// Module 序列化操作码。取值必须与 Redis 的 moduleTypeRead/Write 约定一致：
// 0 表示数据结束，1~5 分别表示有符号整数、无符号整数、float、double、string。
// 注意这里的顺序是 RDB 模块格式的一部分，不能随意调整。
const (
	ModuleOpcodeEOF Opcode = iota
	ModuleOpcodeSInt
	ModuleOpcodeUInt
	ModuleOpcodeFloat
	ModuleOpcodeDouble
	ModuleOpcodeString
)

// ModuleTypeHandler 提供给自定义 Module 类型处理器的读取接口。
//
// 处理器不需要关心底层缓冲，只需按 Module 的序列化格式依次调用对应方法：
// 先 ReadOpcode 判断类型，再调用 ReadSInt/ReadUInt/ReadFloat32/ReadDouble/
// ReadString 读取 payload；遇到 ModuleOpcodeEOF 表示该对象结束。
// 处理器必须完整消费当前对象的全部字节，否则后续 RDB 解析会错位。
type ModuleTypeHandler interface {
	// ReadByte 读取一个原始字节。
	ReadByte() (byte, error)
	// ReadFull 读取恰好 len(buf) 个字节并填满 buf。
	ReadFull(buf []byte) error
	// ReadOpcode 读取一个操作码，并校验其范围（0~5）。
	ReadOpcode() (Opcode, error)
	// ReadUInt 读取一个无符号整数（模块格式的长度/整数编码）。
	ReadUInt() (uint64, error)
	// ReadSInt 读取一个有符号整数（按 int64 解释读到的编码值）。
	ReadSInt() (int64, error)
	// ReadFloat32 读取 4 字节 float。
	ReadFloat32() (float32, error)
	// ReadDouble 读取 8 字节 double。
	ReadDouble() (float64, error)
	// ReadString 读取一个 Redis 字符串（含长度前缀/特殊编码处理）。
	ReadString() ([]byte, error)
	// ReadLength 读取长度编码，返回 (值, 是否为特殊编码, 错误)。
	// special 为 true 时，第一个返回值表示特殊编码类型而非长度。
	ReadLength() (uint64, bool, error)
}

// moduleTypeHandlerImpl 是 ModuleTypeHandler 的实现，内部直接复用 Decoder
// 的底层读取方法，从而与普通对象解析共享同一输入流和缓冲区。
type moduleTypeHandlerImpl struct {
	dec *Decoder
}

// ReadByte 读取一个字节。
func (m moduleTypeHandlerImpl) ReadByte() (byte, error) {
	return m.dec.readByte()
}

// ReadFull 读取 len(buf) 个字节。
func (m moduleTypeHandlerImpl) ReadFull(buf []byte) error {
	return m.dec.readFull(buf)
}

// ReadOpcode 从流中读取一个长度编码的整数作为 opcode，并限制在 0~5。
// 超过 5 说明不是本库支持的 Module 操作码，返回 "unknown opcode"。
func (m moduleTypeHandlerImpl) ReadOpcode() (Opcode, error) {
	code, _, err := m.dec.readLength()
	if err != nil {
		return 0, err
	}
	if code > 5 {
		return 0, errors.New("unknown opcode")
	}
	return Opcode(code), nil
}

// ReadUInt 读取一个无符号整数。Module 格式用 Redis 长度编码表示整数。
func (m moduleTypeHandlerImpl) ReadUInt() (uint64, error) {
	val, _, err := m.dec.readLength()
	return val, err
}

// ReadSInt 读取一个有符号整数。底层仍按长度编码读取，
// 再按二进制补码语义转换为 int64（负数在编码中表现为很大的 uint64）。
func (m moduleTypeHandlerImpl) ReadSInt() (int64, error) {
	val, _, err := m.dec.readLength()
	return int64(val), err
}

// ReadFloat32 读取 4 字节 float（小端，见 readFloat32 的实现）。
func (m moduleTypeHandlerImpl) ReadFloat32() (float32, error) {
	return m.dec.readFloat32()
}

// ReadDouble 读取 8 字节 double（小端，见 readFloat 的实现）。
func (m moduleTypeHandlerImpl) ReadDouble() (float64, error) {
	return m.dec.readFloat()
}

// ReadString 读取一个 Redis 字符串，自动处理长度前缀与特殊编码。
func (m moduleTypeHandlerImpl) ReadString() ([]byte, error) {
	return m.dec.readString()
}

// ReadLength 直接读取长度编码，保留 special 标志供调用方判断特殊编码。
func (m moduleTypeHandlerImpl) ReadLength() (uint64, bool, error) {
	return m.dec.readLength()
}

// ModuleTypeHandleFunc 是自定义 Redis Module 类型的解析回调。
//
// 参数：
//   - handler：读取当前 module 对象 payload 的接口；
//   - encVersion：module 编码版本，取自 module id 的低 10 位；
//
// 返回值：
//   - interface{}：自定义解析结果，会存入 model.ModuleTypeObject.Value，
//     调用方可在 Parse 回调中通过类型断言取回；
//   - error：解析失败时返回。
//
// 使用方式（在创建 Decoder 后、Parse 之前注册）：
//
//	dec := core.NewDecoder(reader).WithSpecialType("MyModule", myHandler)
//	err := dec.Parse(func(o model.RedisObject) bool {
//		if m, ok := o.(*model.ModuleTypeObject); ok {
//			_ = m.Value // 这里就是 myHandler 的返回值
//		}
//		return true
//	})
//
// 处理器必须按 Module 序列化格式一直读到 ModuleOpcodeEOF，否则输入流会错位。
type ModuleTypeHandleFunc func(handler ModuleTypeHandler, encVersion int) (interface{}, error)

// readModuleType 读取一个 Module 对象的头部：先读 module id，
// 再根据 id 解析出 module 类型名与编码版本并调用相应处理器。
func (dec *Decoder) readModuleType() (string, interface{}, error) {
	// module id 本身使用 Redis 长度编码。
	moduleId, _, err := dec.readLength()
	if err != nil {
		return "", nil, err
	}
	return dec.handleModuleType(moduleId)
}

// handleModuleType 根据 module id 查找并执行注册的处理器。
//
// 若该 module 类型没有通过 WithSpecialType 注册，会打印提示并回退到
// skipModuleAuxData，只把 payload 跳过（此时返回的 value 为 nil），
// 从而保证整个 RDB 仍能继续解析。
func (dec *Decoder) handleModuleType(moduleId uint64) (string, interface{}, error) {
	moduleType := moduleTypeNameByID(moduleId)
	handler, found := dec.withSpecialTypes[moduleType]
	if !found {
		fmt.Printf("unknown module type: %s,will skip\n", moduleType)
		handler = skipModuleAuxData
	}
	encVersion := moduleTypeEncVersionByID(moduleId)
	val, err := handler(moduleTypeHandlerImpl{dec: dec}, int(encVersion))
	return moduleType, val, err
}

// moduleTypeNameByID 从 module id 中还原 9 字符的 module 类型名。
//
// module id 的布局：高 54 位是类型名（9 个字符 × 6 位），低 10 位是编码版本。
// 因此先右移 10 位去掉版本，再从最低 6 位开始、从后往前填充 name，
// 保证最终字符串的字符顺序与编码时一致。每 6 位映射到
// ModuleTypeNameCharSet 中的一个字符（64 个字符，正好 6 位可索引）。
func moduleTypeNameByID(moduleId uint64) string {
	cset := ModuleTypeNameCharSet
	name := make([]byte, 9)
	moduleId >>= 10
	for j := 0; j < 9; j++ {
		name[8-j] = cset[moduleId&63]
		moduleId >>= 6
	}
	return string(name)
}

// moduleTypeEncVersionByID 取 module id 的低 10 位作为编码版本。
// 低 10 位掩码为 1023（二进制 1111111111）。
func moduleTypeEncVersionByID(moduleId uint64) uint64 {
	return moduleId & 1023
}

// skipModuleAuxData skips module aux data
// skipModuleAuxData 是未注册类型时的默认处理器：不解析具体含义，
// 只按 opcode 逐个读取并丢弃 payload，直到遇到 ModuleOpcodeEOF。
// 这样即使不认识某个 module，也能安全地跳过该对象继续解析后续数据。
func skipModuleAuxData(h ModuleTypeHandler, _ int) (interface{}, error) {
	// 先读第一个 opcode；若一开始就是 EOF，说明 payload 为空。
	opCode, err := h.ReadOpcode()
	if err != nil {
		return nil, err
	}
	for opCode != ModuleOpcodeEOF {
		// 根据 opcode 读取对应类型的 payload，读到的值直接丢弃。
		switch opCode {
		case ModuleOpcodeSInt:
			_, err = h.ReadSInt()
		case ModuleOpcodeUInt:
			_, err = h.ReadUInt()
		case ModuleOpcodeFloat:
			_, err = h.ReadFloat32()
		case ModuleOpcodeDouble:
			_, err = h.ReadDouble()
		case ModuleOpcodeString:
			_, err = h.ReadString()
		default:
			// ReadOpcode 已限制 0~5，正常不会走到这里；保留兜底以防未来扩展。
			err = fmt.Errorf("unknown module opcode %d", opCode)
		}
		if err != nil {
			return nil, err
		}
		// 继续读取下一个 opcode，直到 EOF。
		opCode, err = h.ReadOpcode()
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ModuleTypeNameCharSet 是 module 类型名的字符表，共 64 个字符，
// 与 Redis 的 moduleTypeEncodeId 使用同一套映射：
// A-Z、a-z、0-9、'-'、'_'。每个字符占 6 位。
const ModuleTypeNameCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
