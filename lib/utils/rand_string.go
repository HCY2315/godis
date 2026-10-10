// rand_string.go：随机字符串生成（用于 RunID、集群复制 ID 等）。
package utils

import (
	"math/rand"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString —— 用途：生成长度为 n 的随机字母数字字符串。
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

var hexLetters = []rune("0123456789abcdef")

func RandHexString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexLetters[r.Intn(len(hexLetters))]
	}
	return string(b)
}

// RandIndex returns random indexes to random pick elements from slice
func RandIndex(size int) []int {
	result := make([]int, size)
	for i := range result {
		result[i] = i
	}
	rand.Shuffle(size, func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}
