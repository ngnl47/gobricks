package util

import (
	"container/heap"
	"strconv"
	"sync"
	"time"
)

// TaskID 任务ID
type TaskID int64

// String 返回任务ID的字符串表示
func (id TaskID) String() string {
	return strconv.FormatInt(int64(id), 10)
}

// DelayQueue 延迟队列。可以保证相同延迟的任务执行顺序与添加顺序一致。
type DelayQueue struct {
	mu           sync.Mutex
	tasks        *taskHeap // 自定义排序的小顶堆
	ticker       *time.Ticker
	tickInterval time.Duration // tick 间隔
	done         chan struct{}
	taskQueue    chan func()
	wg           sync.WaitGroup
	seq          int64
	running      bool
	taskIndex    map[TaskID]int // 记录任务在堆中的索引，用于快速删除
}

// delayedTask 延迟任务
type delayedTask struct {
	executeAt time.Time
	task      func()
	seq       int64       // 序列号，用于相同执行时间的任务排序，同时作为任务ID
	dq        *DelayQueue // 指向所属的 DelayQueue，用于更新索引映射
}

// taskHeap 任务堆，实现 heap.Interface
type taskHeap []*delayedTask

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// 首先按执行时间排序
	if !h[i].executeAt.Equal(h[j].executeAt) {
		return h[i].executeAt.Before(h[j].executeAt)
	}
	// 执行时间相同，按序列号排序（保证添加顺序）
	return h[i].seq < h[j].seq
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	// 更新索引映射
	if h[i].dq != nil {
		h[i].dq.taskIndex[TaskID(h[i].seq)] = i
		h[j].dq.taskIndex[TaskID(h[j].seq)] = j
	}
}

func (h *taskHeap) Push(x interface{}) {
	task := x.(*delayedTask)
	*h = append(*h, task)
	// 更新索引映射
	if task.dq != nil {
		task.dq.taskIndex[TaskID(task.seq)] = len(*h) - 1
	}
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	// 从索引映射中删除
	if x.dq != nil {
		delete(x.dq.taskIndex, TaskID(x.seq))
	}
	return x
}

// NewDelayQueue 创建一个新的延迟队列
func NewDelayQueue(tickInterval time.Duration) *DelayQueue {
	tasks := &taskHeap{}
	heap.Init(tasks) // 初始化堆
	return &DelayQueue{
		tasks:        tasks,
		tickInterval: tickInterval,
		done:         make(chan struct{}),
		taskQueue:    make(chan func(), 10000),
		taskIndex:    make(map[TaskID]int),
	}
}

// Start 启动延迟队列
func (dq *DelayQueue) Start() {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.running {
		return
	}

	dq.ticker = time.NewTicker(dq.tickInterval)
	dq.wg.Add(2)
	go dq.scheduler()
	go dq.worker()
	dq.running = true
}

// Stop 停止延迟队列
func (dq *DelayQueue) Stop() {
	dq.mu.Lock()
	if !dq.running {
		dq.mu.Unlock()
		return
	}
	close(dq.done)
	dq.ticker.Stop()
	dq.running = false
	dq.mu.Unlock()

	close(dq.taskQueue)
	// 然后等待所有协程完成
	dq.wg.Wait()
}

// AddScheduled 添加周期执行任务
func (dq *DelayQueue) AddScheduled(interval time.Duration, f func()) (*TaskID, error) {
	if f == nil {
		return nil, nil
	}
	if interval <= 0 {
		return nil, nil
	}

	dq.mu.Lock()
	defer dq.mu.Unlock()

	// 立即执行一次
	dq.seq++
	taskID := TaskID(dq.seq)
	task := &delayedTask{
		executeAt: time.Now(),
		task: func() {
			f()
			dq.AddScheduled(interval, f) // 添加下一次
		},
		seq: dq.seq,
		dq:  dq,
	}
	heap.Push(dq.tasks, task)

	return &taskID, nil
}

// AfterFunc 类似 time.AfterFunc，添加一个延时任务
func (dq *DelayQueue) AfterFunc(d time.Duration, f func()) *TaskID {
	taskID, _ := dq.Add(d, f)
	return taskID
}

// Add 添加仅执行一次的延时任务
func (dq *DelayQueue) Add(delay time.Duration, f func()) (*TaskID, error) {
	if f == nil {
		return nil, nil
	}
	if delay < 0 {
		return nil, nil
	}

	dq.mu.Lock()
	defer dq.mu.Unlock()

	dq.seq++
	taskID := TaskID(dq.seq)
	task := &delayedTask{
		executeAt: time.Now().Add(delay),
		task:      f,
		seq:       dq.seq,
		dq:        dq,
	}
	heap.Push(dq.tasks, task)

	return &taskID, nil
}

// Remove 根据任务ID删除任务
func (dq *DelayQueue) Remove(taskID *TaskID) error {
	if taskID == nil {
		return nil
	}

	dq.mu.Lock()
	defer dq.mu.Unlock()

	// 查找任务索引
	index, ok := dq.taskIndex[*taskID]
	if !ok {
		return nil
	}

	// 从堆中删除任务
	heap.Remove(dq.tasks, index)

	return nil
}

// scheduler 调度器，负责检查任务是否到执行时间
func (dq *DelayQueue) scheduler() {
	defer dq.wg.Done()
	for {
		select {
		case <-dq.ticker.C:
			dq.mu.Lock()
			now := time.Now()
			for dq.tasks.Len() > 0 && !(*dq.tasks)[0].executeAt.After(now) {
				task := heap.Pop(dq.tasks).(*delayedTask)
				dq.taskQueue <- task.task
			}
			dq.mu.Unlock()
		case <-dq.done:
			return
		}
	}
}

// worker 工作协程，按顺序执行任务
func (dq *DelayQueue) worker() {
	defer dq.wg.Done()
	for task := range dq.taskQueue {
		task()
	}
}
