package util

import (
	"fmt"
	"reflect"
	"sync"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(topic string, fn interface{}) error
	SubscribeAsync(topic string, fn interface{}, transactional bool) error
	SubscribeOnce(topic string, fn interface{}) error
	SubscribeOnceAsync(topic string, fn interface{}) error
	Unsubscribe(topic string, handler interface{}) error
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(topic string, args ...interface{})
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers map[string][]*eventHandler
	mu       sync.RWMutex
	wg       sync.WaitGroup
}

type eventHandler struct {
	callBack      reflect.Value
	flagOnce      bool
	async         bool
	transactional bool
	once          sync.Once // SubscribeOnce(Async): user callback runs at most once
	sync.Mutex              // transactional async: serialize callbacks for this subscription
}

// NewEventBus returns new EventBus with empty handlers.
func NewEventBus() Bus {
	b := &EventBus{
		handlers: make(map[string][]*eventHandler),
	}
	return Bus(b)
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *EventBus) doSubscribe(topic string, fn interface{}, handler *eventHandler) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if reflect.TypeOf(fn).Kind() != reflect.Func {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}
	bus.handlers[topic] = append(bus.handlers[topic], handler)
	return nil
}

// Subscribe subscribes to a topic.
// Returns error if `fn` is not a function.
func (bus *EventBus) Subscribe(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{callBack: reflect.ValueOf(fn)})
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
// Transactional determines whether subsequent callbacks for a topic are
// run serially (true) or concurrently (false)
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeAsync(topic string, fn interface{}, transactional bool) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack:      reflect.ValueOf(fn),
		async:         true,
		transactional: transactional,
	})
}

// SubscribeOnce subscribes to a topic once. Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnce(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
	})
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
// Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnceAsync(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
		async:    true,
	})
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus) HasCallback(topic string) bool {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	h, ok := bus.handlers[topic]
	return ok && len(h) > 0
}

// Unsubscribe removes callback defined for a topic.
// Returns error if there are no callbacks subscribed to the topic.
func (bus *EventBus) Unsubscribe(topic string, handler interface{}) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if _, ok := bus.handlers[topic]; ok && len(bus.handlers[topic]) > 0 {
		bus.removeHandler(topic, bus.findHandlerIdx(topic, reflect.ValueOf(handler)))
		return nil
	}
	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
//
// Handlers are resolved under a short read lock (snapshot); callbacks run without holding the bus lock,
// so concurrent Publish and slow handlers do not block other topics. Unsubscribe may race with Publish:
// a handler removed after the snapshot was taken can still be invoked once.
func (bus *EventBus) Publish(topic string, args ...interface{}) {
	bus.mu.RLock()
	handlers, ok := bus.handlers[topic]
	if !ok || len(handlers) == 0 {
		bus.mu.RUnlock()
		return
	}
	copyHandlers := append([]*eventHandler(nil), handlers...)
	bus.mu.RUnlock()

	for _, handler := range copyHandlers {
		if handler.async {
			bus.wg.Add(1)
			if handler.transactional {
				// Serialize sequential Publish to the same transactional subscriber:
				// lock on the publisher goroutine before starting the worker (matches legacy EventBus).
				handler.Lock()
				go bus.runTransactionalAsync(handler, topic, args...)
				continue
			}
			go bus.doPublishAsync(handler, topic, args...)
			continue
		}
		if handler.flagOnce {
			h := handler
			h.once.Do(func() {
				bus.mu.Lock()
				bus.removeHandlerIfPresent(topic, h)
				bus.mu.Unlock()
				bus.doPublish(h, topic, args...)
			})
			continue
		}
		bus.doPublish(handler, topic, args...)
	}
}

func (bus *EventBus) doPublish(handler *eventHandler, topic string, args ...interface{}) {
	passedArguments := bus.setUpPublish(handler, args...)
	handler.callBack.Call(passedArguments)
}

func (bus *EventBus) doPublishAsync(handler *eventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	if handler.flagOnce {
		handler.once.Do(func() {
			bus.mu.Lock()
			bus.removeHandlerIfPresent(topic, handler)
			bus.mu.Unlock()
			bus.doPublish(handler, topic, args...)
		})
		return
	}
	bus.doPublish(handler, topic, args...)
}

// runTransactionalAsync runs after publisher acquired handler.Lock().
func (bus *EventBus) runTransactionalAsync(handler *eventHandler, topic string, args ...interface{}) {
	defer bus.wg.Done()
	defer handler.Unlock()
	if handler.flagOnce {
		handler.once.Do(func() {
			bus.mu.Lock()
			bus.removeHandlerIfPresent(topic, handler)
			bus.mu.Unlock()
			bus.doPublish(handler, topic, args...)
		})
		return
	}
	bus.doPublish(handler, topic, args...)
}

// removeHandlerIfPresent removes h from bus.handlers[topic]. Caller must hold bus.mu (write lock).
func (bus *EventBus) removeHandlerIfPresent(topic string, h *eventHandler) {
	slice, ok := bus.handlers[topic]
	if !ok {
		return
	}
	for i, eh := range slice {
		if eh == h {
			bus.removeHandler(topic, i)
			return
		}
	}
}

func (bus *EventBus) removeHandler(topic string, idx int) {
	if _, ok := bus.handlers[topic]; !ok {
		return
	}
	l := len(bus.handlers[topic])

	if idx < 0 || idx >= l {
		return
	}

	copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
	bus.handlers[topic][l-1] = nil
	bus.handlers[topic] = bus.handlers[topic][:l-1]
}

func (bus *EventBus) findHandlerIdx(topic string, callback reflect.Value) int {
	if _, ok := bus.handlers[topic]; ok {
		for idx, handler := range bus.handlers[topic] {
			if handler.callBack.Type() == callback.Type() &&
				handler.callBack.Pointer() == callback.Pointer() {
				return idx
			}
		}
	}
	return -1
}

func (bus *EventBus) setUpPublish(callback *eventHandler, args ...interface{}) []reflect.Value {
	funcType := callback.callBack.Type()
	passedArguments := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		} else {
			passedArguments[i] = reflect.ValueOf(v)
		}
	}

	return passedArguments
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}
