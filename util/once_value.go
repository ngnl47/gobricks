package util

import "sync"

// OnceValue 延迟初始化+单例特性
type OnceValue[T any] func() T

func NewOnceValue[T any](factory func() T) OnceValue[T] {
	var once sync.Once
	var value T
	return func() T {
		once.Do(func() {
			value = factory()
		})
		return value
	}
}
