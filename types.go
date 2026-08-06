package redisemaphore

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidConfig    = errors.New("redisemaphore: invalid configuration")
	ErrAcquireTimeout   = errors.New("redisemaphore: acquisition timed out")
	ErrDuplicateRequest = errors.New("redisemaphore: duplicate active request")
	ErrLeaseLost        = errors.New("redisemaphore: lease ownership lost")
)

// EventType identifies an observable state transition.
type EventType string

const (
	EventAcquireStart    EventType = "acquire_start"
	EventAcquireSuccess  EventType = "acquire_success"
	EventAcquireFailure  EventType = "acquire_failure"
	EventLeaseRenewed    EventType = "lease_renewed"
	EventLeaseRenewError EventType = "lease_renew_error"
	EventLeaseLost       EventType = "lease_lost"
	EventRelease         EventType = "release"
	EventCleanup         EventType = "cleanup"
	EventPruned          EventType = "pruned"
	EventRedisError      EventType = "redis_error"
)

// Event is deliberately low-cardinality. RequestID is provided for logs and
// traces, but callers should not use it as a metric label.
type Event struct {
	Type          EventType
	Resource      string
	Namespace     string
	Queue         string
	RequestID     string
	WaitDuration  time.Duration
	Holders       int64
	Waiters       int64
	PrunedHolders int64
	PrunedWaiters int64
	Err           error
}

type Observer interface {
	Observe(context.Context, Event)
}

type ObserverFunc func(context.Context, Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	f(ctx, event)
}

const observerQueueCapacity = 64

type observerDelivery struct {
	ctx   context.Context
	event Event
}

// boundedObserver removes user-provided observability code from ownership and
// renewal paths. It keeps at most one delivery goroutine and a fixed-size
// queue. When the queue is full, the oldest event is discarded so recent
// lease-loss and Redis-error signals are retained preferentially.
type boundedObserver struct {
	delegate Observer

	mu      sync.Mutex
	queue   [observerQueueCapacity]observerDelivery
	head    int
	size    int
	running bool
}

func newNonBlockingObserver(observer Observer) Observer {
	if observer == nil {
		return nil
	}
	if _, ok := observer.(*boundedObserver); ok {
		return observer
	}
	return &boundedObserver{delegate: observer}
}

func (o *boundedObserver) Observe(ctx context.Context, event Event) {
	o.mu.Lock()
	if o.size == len(o.queue) {
		o.queue[o.head] = observerDelivery{}
		o.head = (o.head + 1) % len(o.queue)
		o.size--
	}
	tail := (o.head + o.size) % len(o.queue)
	o.queue[tail] = observerDelivery{ctx: ctx, event: event}
	o.size++
	if o.running {
		o.mu.Unlock()
		return
	}
	o.running = true
	o.mu.Unlock()

	go o.drain()
}

func (o *boundedObserver) drain() {
	for {
		o.mu.Lock()
		if o.size == 0 {
			o.running = false
			o.mu.Unlock()
			return
		}
		delivery := o.queue[o.head]
		o.queue[o.head] = observerDelivery{}
		o.head = (o.head + 1) % len(o.queue)
		o.size--
		o.mu.Unlock()

		func() {
			defer func() {
				_ = recover()
			}()
			o.delegate.Observe(delivery.ctx, delivery.event)
		}()
	}
}

func emitEvent(ctx context.Context, observer Observer, event Event) {
	if observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.Observe(ctx, event)
}

// Snapshot is an approximate diagnostic view assembled from multiple
// primary-routed reads. Counts exclude entries that were expired at the
// sampled Redis server time, but fields can reflect slightly different
// moments when state changes concurrently.
type Snapshot struct {
	Holders          int64
	Waiters          int64
	WaitersByQueue   map[string]int64
	OldestWait       time.Duration
	CapacityExceeded bool
}
