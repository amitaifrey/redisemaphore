package redisemaphore

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrInvalidConfig    = errors.New("redisemaphore: invalid configuration")
	ErrAcquireTimeout   = errors.New("redisemaphore: acquisition timed out")
	ErrDuplicateRequest = errors.New("redisemaphore: duplicate active request")
	ErrLeaseLost        = errors.New("redisemaphore: lease ownership lost")
)

const (
	logEventAcquireStart    = "acquire_start"
	logEventAcquireSuccess  = "acquire_success"
	logEventAcquireFailure  = "acquire_failure"
	logEventLeaseRenewed    = "lease_renewed"
	logEventLeaseRenewError = "lease_renew_error"
	logEventLeaseLost       = "lease_lost"
	logEventRelease         = "release"
	logEventCleanup         = "cleanup"
	logEventPruned          = "pruned"
	logEventRedisError      = "redis_error"
)

const (
	logAttrEvent         = "event"
	logAttrEventTime     = "event_time"
	logAttrResource      = "resource"
	logAttrNamespace     = "namespace"
	logAttrQueue         = "queue"
	logAttrRequestID     = "request_id"
	logAttrWaitDuration  = "wait_duration"
	logAttrHolders       = "holders"
	logAttrWaiters       = "waiters"
	logAttrPrunedHolders = "pruned_holders"
	logAttrPrunedWaiters = "pruned_waiters"
	logAttrError         = "error"
)

// Logger receives structured diagnostic events. *slog.Logger implements Logger
// directly.
type Logger interface {
	LogAttrs(context.Context, slog.Level, string, ...slog.Attr)
}

var _ Logger = (*slog.Logger)(nil)

const loggerQueueCapacity = 64

type logDelivery struct {
	ctx     context.Context
	level   slog.Level
	message string
	attrs   []slog.Attr
}

// boundedLogger removes user-provided logging code from ownership and renewal
// paths. It keeps at most one delivery goroutine and a fixed-size
// queue. When the queue is full, the oldest event is discarded so recent
// lease-loss and Redis-error signals are retained preferentially.
type boundedLogger struct {
	delegate Logger

	mu      sync.Mutex
	queue   [loggerQueueCapacity]logDelivery
	head    int
	size    int
	running bool
}

func newNonBlockingLogger(logger Logger) Logger {
	if logger == nil {
		return nil
	}
	if _, ok := logger.(*boundedLogger); ok {
		return logger
	}
	return &boundedLogger{delegate: logger}
}

func (l *boundedLogger) LogAttrs(ctx context.Context, level slog.Level, message string, attrs ...slog.Attr) {
	delivery := logDelivery{
		ctx:     ctx,
		level:   level,
		message: message,
		attrs:   append([]slog.Attr(nil), attrs...),
	}

	l.mu.Lock()
	if l.size == len(l.queue) {
		l.queue[l.head] = logDelivery{}
		l.head = (l.head + 1) % len(l.queue)
		l.size--
	}
	tail := (l.head + l.size) % len(l.queue)
	l.queue[tail] = delivery
	l.size++
	if l.running {
		l.mu.Unlock()
		return
	}
	l.running = true
	l.mu.Unlock()

	go l.drain()
}

func (l *boundedLogger) drain() {
	for {
		l.mu.Lock()
		if l.size == 0 {
			l.running = false
			l.mu.Unlock()
			return
		}
		delivery := l.queue[l.head]
		l.queue[l.head] = logDelivery{}
		l.head = (l.head + 1) % len(l.queue)
		l.size--
		l.mu.Unlock()

		func() {
			defer func() {
				_ = recover()
			}()
			l.delegate.LogAttrs(delivery.ctx, delivery.level, delivery.message, delivery.attrs...)
		}()
	}
}

func logEvent(ctx context.Context, logger Logger, level slog.Level, event string, attrs ...slog.Attr) {
	if logger == nil {
		return
	}
	eventAttrs := make([]slog.Attr, 0, len(attrs)+2)
	eventAttrs = append(eventAttrs,
		slog.String(logAttrEvent, event),
		slog.Time(logAttrEventTime, time.Now()),
	)
	eventAttrs = append(eventAttrs, attrs...)

	defer func() {
		_ = recover()
	}()
	logger.LogAttrs(ctx, level, event, eventAttrs...)
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
