package util

import "sync"

type RWMutexValue[T any] struct {
	sync.RWMutex
	Value T
}

func (v *RWMutexValue[T]) Get() T {
	v.RLock()
	r := v.Value
	v.RUnlock()
	return r
}

func (v *RWMutexValue[T]) Set(value T) {
	v.Lock()
	v.Value = value
	v.Unlock()
}

func (v *RWMutexValue[T]) RLockDo(action func()) {
	v.RLock()
	defer v.RUnlock()
	action()
}

func (v *RWMutexValue[T]) LockDo(action func()) {
	v.Lock()
	defer v.Unlock()
	action()
}

type MutexValue[T any] struct {
	sync.Mutex
	Value T
}

func (v *MutexValue[T]) Get() T {
	v.Lock()
	r := v.Value
	v.Unlock()
	return r
}

func (v *MutexValue[T]) Set(value T) {
	v.Lock()
	v.Value = value
	v.Unlock()
}

func (v *MutexValue[T]) LockDo(action func()) {
	v.Lock()
	defer v.Unlock()
	action()
}
