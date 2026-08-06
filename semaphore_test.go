package redisemaphore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/amitaifrey/redisemaphore"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func startMiniRedis(t *testing.T) string {
	t.Helper()
	return startMiniRedisServer(t).Addr()
}

func startMiniRedisServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr
}

func newRedisClient(t *testing.T, addr string) redis.UniversalClient {
	t.Helper()

	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Redis client: %v", err)
		}
	})
	return client
}

func testNamespace(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%s", t.Name(), uuid.NewString())
}

func testSemaphoreConfig(t *testing.T, capacity int, queues ...string) redisemaphore.SemaphoreConfig {
	t.Helper()
	return redisemaphore.SemaphoreConfig{
		Namespace:        testNamespace(t),
		Capacity:         capacity,
		QueuesByPriority: queues,
		AcquireTimeout:   3 * time.Second,
		PermitTTL:        600 * time.Millisecond,
		WaiterTTL:        300 * time.Millisecond,
		PollInitial:      5 * time.Millisecond,
		PollMax:          20 * time.Millisecond,
		CleanupTimeout:   100 * time.Millisecond,
	}
}

type snapshotter interface {
	Snapshot(context.Context) (redisemaphore.Snapshot, error)
}

func waitForSnapshot(
	t *testing.T,
	sem snapshotter,
	timeout time.Duration,
	predicate func(redisemaphore.Snapshot) bool,
) redisemaphore.Snapshot {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last redisemaphore.Snapshot
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		last, lastErr = sem.Snapshot(ctx)
		cancel()
		if lastErr == nil && predicate(last) {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("snapshot condition not met before %s; last snapshot=%+v, last error=%v", timeout, last, lastErr)
	return redisemaphore.Snapshot{}
}

func receiveError(t *testing.T, ch <-chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for result", timeout)
		return nil
	}
}

func TestNewSemaphoreRejectsInvalidConfiguration(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))

	tests := []struct {
		name   string
		config redisemaphore.SemaphoreConfig
	}{
		{
			name: "blank namespace",
			config: redisemaphore.SemaphoreConfig{
				Capacity: 1, QueuesByPriority: []string{"default"},
			},
		},
		{
			name: "zero capacity",
			config: redisemaphore.SemaphoreConfig{
				Namespace: "invalid-capacity", QueuesByPriority: []string{"default"},
			},
		},
		{
			name: "no queues",
			config: redisemaphore.SemaphoreConfig{
				Namespace: "invalid-queues", Capacity: 1,
			},
		},
		{
			name: "duplicate queues",
			config: redisemaphore.SemaphoreConfig{
				Namespace: "duplicate-queues", Capacity: 1, QueuesByPriority: []string{"high", "high"},
			},
		},
		{
			name: "poll bounds reversed",
			config: redisemaphore.SemaphoreConfig{
				Namespace: "invalid-poll", Capacity: 1, QueuesByPriority: []string{"default"},
				PollInitial: 2 * time.Second, PollMax: time.Second,
			},
		},
		{
			name: "duration not millisecond aligned",
			config: redisemaphore.SemaphoreConfig{
				Namespace: "fractional-millisecond", Capacity: 1, QueuesByPriority: []string{"default"},
				PollInitial: 1500 * time.Microsecond,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sem, err := redisemaphore.NewSemaphore(client, tt.config)
			if sem != nil {
				t.Errorf("NewSemaphore() returned non-nil semaphore for invalid configuration")
			}
			if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
				t.Fatalf("NewSemaphore() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestSemaphoreRunOwnsAndReleasesPermit(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	called := false
	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "request-1",
	}, func(context.Context) error {
		called = true
		snapshot, snapshotErr := sem.Snapshot(context.Background())
		if snapshotErr != nil {
			return snapshotErr
		}
		if snapshot.Holders != 1 {
			return fmt.Errorf("holders during callback = %d, want 1", snapshot.Holders)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if !called {
		t.Fatal("Run() did not call callback")
	}

	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.Holders != 0 || snapshot.Waiters != 0 {
		t.Fatalf("snapshot after Run() = %+v, want no holders or waiters", snapshot)
	}
}

func TestSemaphoreRunRejectsInvalidRequest(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	sem, err := redisemaphore.NewSemaphore(client, testSemaphoreConfig(t, 1, "default"))
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	tests := []struct {
		name     string
		request  redisemaphore.AcquireRequest
		callback func(context.Context) error
	}{
		{
			name: "blank request ID", request: redisemaphore.AcquireRequest{Queue: "default"},
			callback: func(context.Context) error { return nil },
		},
		{
			name: "unknown queue", request: redisemaphore.AcquireRequest{Queue: "unknown", RequestID: "request"},
			callback: func(context.Context) error { return nil },
		},
		{
			name: "nil callback", request: redisemaphore.AcquireRequest{Queue: "default", RequestID: "request"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sem.Run(context.Background(), tt.request, tt.callback)
			if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
				t.Fatalf("Run() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestSemaphoreCapacityAcrossIndependentClients(t *testing.T) {
	addr := startMiniRedis(t)
	config := testSemaphoreConfig(t, 3, "default")
	config.AcquireTimeout = 10 * time.Second

	const clientCount = 8
	const requestCount = 48
	semaphores := make([]*redisemaphore.Semaphore, 0, clientCount)
	for i := 0; i < clientCount; i++ {
		client := newRedisClient(t, addr)
		sem, err := redisemaphore.NewSemaphore(client, config)
		if err != nil {
			t.Fatalf("NewSemaphore(client %d): %v", i, err)
		}
		semaphores = append(semaphores, sem)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, requestCount)
	var wg sync.WaitGroup
	var active atomic.Int64
	var maximum atomic.Int64
	var exceeded atomic.Bool
	capacityReached := make(chan struct{})
	var capacityReachedOnce sync.Once

	for i := 0; i < requestCount; i++ {
		sem := semaphores[i%len(semaphores)]
		requestID := fmt.Sprintf("request-%03d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := sem.Run(ctx, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: requestID,
			}, func(workCtx context.Context) error {
				current := active.Add(1)
				defer active.Add(-1)
				if current > int64(config.Capacity) {
					exceeded.Store(true)
				}
				if current >= int64(config.Capacity) {
					capacityReachedOnce.Do(func() { close(capacityReached) })
				}
				for {
					old := maximum.Load()
					if current <= old || maximum.CompareAndSwap(old, current) {
						break
					}
				}
				select {
				case <-workCtx.Done():
					return workCtx.Err()
				case <-capacityReached:
				}
				select {
				case <-workCtx.Done():
					return workCtx.Err()
				case <-time.After(10 * time.Millisecond):
					return nil
				}
			})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Run() failed: %v", err)
		}
	}
	if exceeded.Load() {
		t.Fatalf("callbacks exceeded configured capacity %d; observed maximum %d", config.Capacity, maximum.Load())
	}
	if got := maximum.Load(); got != int64(config.Capacity) {
		t.Fatalf("maximum concurrent callbacks = %d, want %d", got, config.Capacity)
	}
}

func TestSemaphoreStrictPriorityAndFIFO(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "high", "low")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "high", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	order := make(chan string, 3)
	results := make(chan error, 3)
	launch := func(queue, requestID string) {
		go func() {
			results <- sem.Run(ctx, redisemaphore.AcquireRequest{
				Queue: queue, RequestID: requestID,
			}, func(context.Context) error {
				order <- requestID
				return nil
			})
		}()
	}

	launch("low", "low-first")
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.WaitersByQueue["low"] == 1
	})
	launch("low", "low-second")
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.WaitersByQueue["low"] == 2
	})
	launch("high", "high")
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Waiters == 3 && snapshot.WaitersByQueue["high"] == 1
	})

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}

	wantOrder := []string{"high", "low-first", "low-second"}
	for i, want := range wantOrder {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("callback %d = %q, want %q; complete expected order %v", i, got, want, wantOrder)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for callback %d (%q)", i, want)
		}
	}
	for i := 0; i < len(wantOrder); i++ {
		if err := receiveError(t, results, time.Second); err != nil {
			t.Errorf("queued Run() failed: %v", err)
		}
	}
}

func TestSemaphoreRejectsDuplicateActiveRequestAndAllowsReuse(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "reusable",
		}, func(workCtx context.Context) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}

	var duplicateCalled atomic.Bool
	err = sem.Run(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "reusable",
	}, func(context.Context) error {
		duplicateCalled.Store(true)
		return nil
	})
	if !errors.Is(err, redisemaphore.ErrDuplicateRequest) {
		t.Fatalf("duplicate Run() error = %v, want ErrDuplicateRequest", err)
	}
	if duplicateCalled.Load() {
		t.Fatal("duplicate Run() called its callback")
	}

	close(release)
	if err := receiveError(t, firstResult, time.Second); err != nil {
		t.Fatalf("first Run(): %v", err)
	}

	reusedCalled := false
	err = sem.Run(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "reusable",
	}, func(context.Context) error {
		reusedCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("Run() after request ID reuse: %v", err)
	}
	if !reusedCalled {
		t.Fatal("Run() after request ID reuse did not call callback")
	}
}

func TestSemaphoreCanceledWaiterIsCleanedUp(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	testCtx, cancelTest := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelTest()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(testCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	var waiterCalled atomic.Bool
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- sem.Run(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "canceled-waiter",
		}, func(context.Context) error {
			waiterCalled.Store(true)
			return nil
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.WaitersByQueue["default"] == 1
	})

	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	if waiterCalled.Load() {
		t.Fatal("canceled waiter callback was called")
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 0
	})

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestSemaphoreParentCancellationCancelsCallbackAndCleansHolder(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	sem, err := redisemaphore.NewSemaphore(client, testSemaphoreConfig(t, 1, "default"))
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	callbackStarted := make(chan struct{})
	callbackCanceled := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- sem.Run(runCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "canceled-holder",
		}, func(workCtx context.Context) error {
			close(callbackStarted)
			<-workCtx.Done()
			close(callbackCanceled)
			return workCtx.Err()
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}

	cancelRun()
	select {
	case <-callbackCanceled:
	case <-time.After(time.Second):
		t.Fatal("derived callback context was not canceled")
	}
	if err := receiveError(t, result, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 0 && snapshot.Waiters == 0
	})
}

func TestSemaphoreLiveOldWaiterIsNotPrunedByEnqueueAge(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.WaiterTTL = 90 * time.Millisecond
	config.CleanupTimeout = 20 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	waiterStarted := make(chan struct{})
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "long-waiter",
		}, func(context.Context) error {
			close(waiterStarted)
			return nil
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Waiters == 1
	})

	// This exceeds three waiter lease periods. The polling caller keeps its
	// liveness lease fresh without changing its original FIFO sequence.
	time.Sleep(300 * time.Millisecond)
	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.Waiters != 1 {
		t.Fatalf("waiters after long live wait = %d, want 1", snapshot.Waiters)
	}
	select {
	case <-waiterStarted:
		t.Fatal("waiter entered while capacity was held")
	default:
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
	select {
	case <-waiterStarted:
	case <-time.After(time.Second):
		t.Fatal("live old waiter was not admitted after release")
	}
	if err := receiveError(t, waiterResult, time.Second); err != nil {
		t.Fatalf("waiter Run(): %v", err)
	}
}

func TestSemaphoreWaiterHeartbeatAccountsForReplyLatency(t *testing.T) {
	addr := startMiniRedis(t)
	config := testSemaphoreConfig(t, 1, "default")
	config.WaiterTTL = 300 * time.Millisecond
	config.PollInitial = 290 * time.Millisecond
	config.PollMax = 290 * time.Millisecond

	holderClient := newRedisClient(t, addr)
	holder, err := redisemaphore.NewSemaphore(holderClient, config)
	if err != nil {
		t.Fatalf("holder NewSemaphore(): %v", err)
	}
	waiterClient := newRedisClient(t, addr)
	waiter, err := redisemaphore.NewSemaphore(waiterClient, config)
	if err != nil {
		t.Fatalf("waiter NewSemaphore(): %v", err)
	}

	var delayCommands atomic.Bool
	delayCommands.Store(true)
	var heartbeatCommands atomic.Int64
	waiterClient.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil && delayCommands.Load() && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			heartbeatCommands.Add(1)
			time.Sleep(140 * time.Millisecond)
		}
		return err
	}})

	testCtx, cancelTest := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelTest()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- holder.Run(testCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "latency-holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- waiter.Run(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "latency-waiter",
		}, func(context.Context) error {
			return errors.New("latency waiter callback must not run while capacity is held")
		})
	}()
	waitForSnapshot(t, holder, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})

	configKeys, err := holderClient.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil || len(configKeys) != 1 {
		t.Fatalf("find semaphore config key: keys=%v error=%v", configKeys, err)
	}
	base := strings.TrimSuffix(configKeys[0], ":config")
	token, err := holderClient.HGet(context.Background(), base+":active-request", "latency-waiter").Result()
	if err != nil {
		t.Fatalf("read waiter token: %v", err)
	}
	initialSequence, err := holderClient.ZScore(context.Background(), base+":queue:default", token).Result()
	if err != nil {
		t.Fatalf("read initial waiter sequence: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for heartbeatCommands.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if heartbeatCommands.Load() < 5 {
		t.Fatalf("observed %d delayed waiter commands, want at least 5", heartbeatCommands.Load())
	}
	currentSequence, err := holderClient.ZScore(context.Background(), base+":queue:default", token).Result()
	if err != nil {
		t.Fatalf("read current waiter sequence: %v", err)
	}
	if currentSequence != initialSequence {
		t.Fatalf("waiter FIFO sequence changed from %v to %v under reply latency", initialSequence, currentSequence)
	}

	delayCommands.Store(false)
	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter Run() error = %v, want context.Canceled", err)
	}
	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestSemaphoreWaiterRedisOutageDoesNotHotSpinAfterHeartbeatDeadline(t *testing.T) {
	addr := startMiniRedis(t)
	config := testSemaphoreConfig(t, 1, "default")
	config.WaiterTTL = 120 * time.Millisecond
	config.PollInitial = 5 * time.Millisecond
	config.PollMax = 20 * time.Millisecond

	holder, err := redisemaphore.NewSemaphore(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("holder NewSemaphore(): %v", err)
	}
	waiterClient := newRedisClient(t, addr)
	waiter, err := redisemaphore.NewSemaphore(waiterClient, config)
	if err != nil {
		t.Fatalf("waiter NewSemaphore(): %v", err)
	}

	var failWaiterCommands atomic.Bool
	var failedCommands atomic.Int64
	waiterClient.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if failWaiterCommands.Load() && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			failedCommands.Add(1)
			return io.ErrUnexpectedEOF
		}
		return next(ctx, cmd)
	}})

	testCtx, cancelTest := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelTest()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- holder.Run(testCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "outage-holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return context.Cause(workCtx)
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- waiter.Run(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "outage-waiter",
		}, func(context.Context) error {
			return errors.New("outage waiter callback must not run while capacity is held")
		})
	}()
	waitForSnapshot(t, holder, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})

	failedCommands.Store(0)
	failWaiterCommands.Store(true)
	// This spans several waiter heartbeat deadlines. The historical bug left
	// an overdue deadline in place and retried with zero-duration timers.
	time.Sleep(250 * time.Millisecond)
	count := failedCommands.Load()
	if count < 3 {
		t.Fatalf("observed %d failed waiter commands, want the outage hook exercised", count)
	}
	if count > 80 {
		t.Fatalf("observed %d failed waiter commands in 250ms; acquire loop is hot-spinning", count)
	}

	failWaiterCommands.Store(false)
	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter Run() error = %v, want context.Canceled", err)
	}
	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestSemaphoreRemovesStaleQueueHeadWithoutMetadata(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.AcquireTimeout = 500 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	configKeys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil {
		t.Fatalf("find semaphore configuration key: %v", err)
	}
	if len(configKeys) != 1 {
		t.Fatalf("found configuration keys %v, want exactly one", configKeys)
	}
	base := strings.TrimSuffix(configKeys[0], ":config")
	queueKey := base + ":queue:default"
	const staleToken = "stale-head-without-metadata"
	if err := client.ZAdd(context.Background(), queueKey, redis.Z{
		Score: -1, Member: staleToken,
	}).Err(); err != nil {
		t.Fatalf("inject stale queue head: %v", err)
	}

	called := false
	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "live-request",
	}, func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Run() behind stale head: %v", err)
	}
	if !called {
		t.Fatal("live callback was not admitted after stale-head cleanup")
	}
	if _, err := client.ZScore(context.Background(), queueKey, staleToken).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("stale queue head still present; ZSCORE error = %v", err)
	}
}

func TestSemaphoreRecoversCommittedAcquireWithLostResponse(t *testing.T) {
	mr := startMiniRedisServer(t)
	client := newRedisClient(t, mr.Addr())
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	// Load the admission and release scripts before installing the fault hook,
	// so the one armed command is the acquisition EVALSHA itself.
	if err := sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime",
	}, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("prime Run(): %v", err)
	}

	configKeys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil {
		t.Fatalf("find semaphore configuration key: %v", err)
	}
	if len(configKeys) != 1 {
		t.Fatalf("found configuration keys %v, want exactly one", configKeys)
	}
	activeRequestKey := strings.TrimSuffix(configKeys[0], ":config") + ":active-request"

	var armed atomic.Bool
	armed.Store(true)
	var injected atomic.Bool
	var committedToken string
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if armed.CompareAndSwap(true, false) && injected.CompareAndSwap(false, true) {
			if err := next(ctx, cmd); err != nil {
				return err
			}
			committedToken = mr.HGet(activeRequestKey, "ambiguous-acquire")
			if committedToken == "" {
				return errors.New("fault hook did not observe the committed acquisition token")
			}
			// Put the first lease close to expiry. The retry must recognize the
			// same token and refresh it before invoking the callback.
			mr.FastForward(150 * time.Millisecond)
			return io.ErrUnexpectedEOF
		}
		return next(ctx, cmd)
	}})

	var callbackCalls atomic.Int64
	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "ambiguous-acquire",
	}, func(context.Context) error {
		callbackCalls.Add(1)
		if activeToken := mr.HGet(activeRequestKey, "ambiguous-acquire"); activeToken != committedToken {
			return fmt.Errorf("active token at callback = %q, want committed token %q", activeToken, committedToken)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run() after committed acquire with lost response: %v", err)
	}
	if !injected.Load() {
		t.Fatal("acquisition fault hook did not trigger")
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("callback calls = %d, want exactly 1", got)
	}
	if token := mr.HGet(activeRequestKey, "ambiguous-acquire"); token != "" {
		t.Fatalf("active request token after release = %q, want no mapping", token)
	}
}

func TestSemaphoreRetriesAmbiguousReleaseWithSameToken(t *testing.T) {
	tests := []struct {
		name            string
		commitFirstCall bool
	}{
		{name: "committed response lost", commitFirstCall: true},
		{name: "uncommitted response lost", commitFirstCall: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newRedisClient(t, startMiniRedis(t))
			config := testSemaphoreConfig(t, 1, "default")
			sem, err := redisemaphore.NewSemaphore(client, config)
			if err != nil {
				t.Fatalf("NewSemaphore(): %v", err)
			}

			// Prime the release script so arming inside the callback targets its
			// EVALSHA rather than a SCRIPT LOAD fallback.
			if err := sem.Run(context.Background(), redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "prime",
			}, func(context.Context) error { return nil }); err != nil {
				t.Fatalf("prime Run(): %v", err)
			}

			var armed atomic.Bool
			var injected atomic.Bool
			client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
				if armed.CompareAndSwap(true, false) && injected.CompareAndSwap(false, true) {
					if tt.commitFirstCall {
						if err := next(ctx, cmd); err != nil {
							return err
						}
					}
					return io.ErrUnexpectedEOF
				}
				return next(ctx, cmd)
			}})

			if err := sem.Run(context.Background(), redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "faulted-release",
			}, func(context.Context) error {
				// Renewal is still sleeping when this immediate callback returns,
				// so the next Redis command is the token-checked release.
				armed.Store(true)
				return nil
			}); err != nil {
				t.Fatalf("Run() with ambiguous release: %v", err)
			}
			if !injected.Load() {
				t.Fatal("release fault hook did not trigger")
			}

			snapshot, err := sem.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot() after ambiguous release: %v", err)
			}
			if snapshot.Holders != 0 || snapshot.Waiters != 0 {
				t.Fatalf("snapshot after ambiguous release = %+v, want no holders or waiters", snapshot)
			}
			if err := sem.Run(context.Background(), redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "after-fault",
			}, func(context.Context) error { return nil }); err != nil {
				t.Fatalf("Run() after ambiguous release: %v", err)
			}
		})
	}
}

func TestSemaphoreCleanupBacklogLargerThanBatchEventuallyAdmits(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	const capacity = 3
	config := testSemaphoreConfig(t, capacity, "default")
	config.AcquireTimeout = 2 * time.Second
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	configKeys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil {
		t.Fatalf("find semaphore configuration key: %v", err)
	}
	if len(configKeys) != 1 {
		t.Fatalf("found configuration keys %v, want exactly one", configKeys)
	}
	holdersKey := strings.TrimSuffix(configKeys[0], ":config") + ":holders"
	const staleHolders = 128*3 + 1
	stale := make([]redis.Z, staleHolders)
	for index := range stale {
		stale[index] = redis.Z{Score: 0, Member: fmt.Sprintf("expired-holder-%03d", index)}
	}
	if err := client.ZAdd(context.Background(), holdersKey, stale...).Err(); err != nil {
		t.Fatalf("inject %d expired holders: %v", staleHolders, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const requests = 12
	start := make(chan struct{})
	capacityReached := make(chan struct{})
	var capacityReachedOnce sync.Once
	var active atomic.Int64
	var maximum atomic.Int64
	var exceeded atomic.Bool
	results := make(chan error, requests)
	var wg sync.WaitGroup
	for index := 0; index < requests; index++ {
		requestID := fmt.Sprintf("backlog-request-%02d", index)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- sem.Run(ctx, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: requestID,
			}, func(workCtx context.Context) error {
				current := active.Add(1)
				defer active.Add(-1)
				if current > capacity {
					exceeded.Store(true)
				}
				for {
					old := maximum.Load()
					if current <= old || maximum.CompareAndSwap(old, current) {
						break
					}
				}
				if current == capacity {
					capacityReachedOnce.Do(func() { close(capacityReached) })
				}
				select {
				case <-capacityReached:
				case <-workCtx.Done():
					return workCtx.Err()
				}
				time.Sleep(5 * time.Millisecond)
				return nil
			})
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("Run() through cleanup backlog: %v", err)
		}
	}
	if exceeded.Load() {
		t.Fatalf("callbacks exceeded capacity %d; observed maximum %d", capacity, maximum.Load())
	}
	if got := maximum.Load(); got != capacity {
		t.Fatalf("maximum concurrent callbacks = %d, want %d", got, capacity)
	}
	remaining, err := client.ZCard(context.Background(), holdersKey).Result()
	if err != nil {
		t.Fatalf("count holders after backlog cleanup: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("holder records after backlog cleanup = %d, want 0", remaining)
	}
}

func TestSemaphoreRenewsPermitUntilCallbackReturns(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var renewals atomic.Int64
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	config.Observer = redisemaphore.ObserverFunc(func(_ context.Context, event redisemaphore.Event) {
		if event.Type == redisemaphore.EventLeaseRenewed {
			renewals.Add(1)
		}
	})
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "renewed-holder",
		}, func(workCtx context.Context) error {
			close(firstStarted)
			select {
			case <-releaseFirst:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}

	deadline := time.Now().Add(time.Second)
	for renewals.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if renewals.Load() < 2 {
		t.Fatalf("observed %d successful renewals, want at least 2", renewals.Load())
	}

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "contender",
		}, func(context.Context) error {
			close(secondStarted)
			return nil
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})
	time.Sleep(config.PermitTTL + 30*time.Millisecond)
	select {
	case <-secondStarted:
		t.Fatal("contender entered before renewed holder returned")
	default:
	}

	close(releaseFirst)
	if err := receiveError(t, firstResult, time.Second); err != nil {
		t.Fatalf("first Run(): %v", err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not enter after holder returned")
	}
	if err := receiveError(t, secondResult, time.Second); err != nil {
		t.Fatalf("second Run(): %v", err)
	}
}

func TestSemaphoreBlockingObserverCannotConsumeLease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	observerEntered := make(chan struct{})
	unblockObserver := make(chan struct{})
	defer close(unblockObserver)
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	config.Observer = redisemaphore.ObserverFunc(func(_ context.Context, event redisemaphore.Event) {
		if event.Type == redisemaphore.EventAcquireSuccess && event.RequestID == "holder" {
			close(observerEntered)
			<-unblockObserver
		}
	})
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-observerEntered:
	case <-time.After(time.Second):
		t.Fatal("observer did not receive acquisition event")
	}
	select {
	case <-holderStarted:
	case <-time.After(config.PermitTTL / 2):
		t.Fatal("blocking observer delayed the callback and lease-renewal lifecycle")
	}

	contenderStarted := make(chan struct{})
	contenderResult := make(chan error, 1)
	go func() {
		contenderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "contender",
		}, func(context.Context) error {
			close(contenderStarted)
			return nil
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})
	time.Sleep(2*config.PermitTTL + 30*time.Millisecond)
	select {
	case <-contenderStarted:
		t.Fatal("contender entered while holder ran with a blocked observer")
	default:
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
	select {
	case <-contenderStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not enter after holder returned")
	}
	if err := receiveError(t, contenderResult, time.Second); err != nil {
		t.Fatalf("contender Run(): %v", err)
	}
}

func TestSemaphoreSnapshotReportsLiveWaitersByQueue(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "high", "low")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "high", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	waitCtx, cancelWait := context.WithCancel(ctx)
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- sem.Run(waitCtx, redisemaphore.AcquireRequest{
			Queue: "low", RequestID: "waiter",
		}, func(context.Context) error {
			return errors.New("canceled waiter callback must not run")
		})
	}()
	snapshot := waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})
	time.Sleep(20 * time.Millisecond)
	snapshot, err = sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() after wait: %v", err)
	}
	if snapshot.WaitersByQueue["high"] != 0 || snapshot.WaitersByQueue["low"] != 1 {
		t.Fatalf("WaitersByQueue = %#v, want high=0 and low=1", snapshot.WaitersByQueue)
	}
	if snapshot.OldestWait <= 0 {
		t.Fatalf("OldestWait = %s, want a positive duration", snapshot.OldestWait)
	}
	if snapshot.CapacityExceeded {
		t.Fatal("Snapshot() reported capacity exceeded")
	}

	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}
	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestSemaphoreSnapshotScansWaiterMetadataInBatches(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 2, "high", "low")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	configKeys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil || len(configKeys) != 1 {
		t.Fatalf("find semaphore config key: keys=%v error=%v", configKeys, err)
	}
	base := strings.TrimSuffix(configKeys[0], ":config")
	now, err := client.Time(context.Background()).Result()
	if err != nil {
		t.Fatalf("Redis TIME: %v", err)
	}

	const waiterCount = 1200
	waiterLeases := make([]redis.Z, 0, waiterCount)
	queueMetadata := make([]interface{}, 0, 2*waiterCount)
	startedMetadata := make([]interface{}, 0, 2*waiterCount)
	for index := 0; index < waiterCount; index++ {
		token := fmt.Sprintf("snapshot-token-%04d", index)
		waiterLeases = append(waiterLeases, redis.Z{
			Score:  float64(now.Add(time.Minute).UnixMilli()),
			Member: token,
		})
		queueMetadata = append(queueMetadata, token, index%2+1)
		startedMetadata = append(startedMetadata, token, now.Add(-time.Duration(index+1)*time.Millisecond).UnixMilli())
	}
	if err := client.ZAdd(context.Background(), base+":waiters", waiterLeases...).Err(); err != nil {
		t.Fatalf("inject waiter leases: %v", err)
	}
	if err := client.HSet(context.Background(), base+":token-queue", queueMetadata...).Err(); err != nil {
		t.Fatalf("inject waiter queues: %v", err)
	}
	if err := client.HSet(context.Background(), base+":waiter-started", startedMetadata...).Err(); err != nil {
		t.Fatalf("inject waiter start times: %v", err)
	}

	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.Waiters != waiterCount {
		t.Fatalf("Waiters = %d, want %d", snapshot.Waiters, waiterCount)
	}
	if snapshot.WaitersByQueue["high"] != waiterCount/2 || snapshot.WaitersByQueue["low"] != waiterCount/2 {
		t.Fatalf("WaitersByQueue = %#v, want %d in each queue", snapshot.WaitersByQueue, waiterCount/2)
	}
	if snapshot.OldestWait < time.Second {
		t.Fatalf("OldestWait = %s, want at least one second", snapshot.OldestWait)
	}
}

func TestSemaphoreAcquireTimeoutDoesNotCallCallback(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.AcquireTimeout = 100 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- sem.Run(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "holder",
		}, func(workCtx context.Context) error {
			close(holderStarted)
			select {
			case <-releaseHolder:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-holderStarted:
	case <-time.After(time.Second):
		t.Fatal("holder callback did not start")
	}

	var called atomic.Bool
	err = sem.Run(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "timeout",
	}, func(context.Context) error {
		called.Store(true)
		return nil
	})
	if !errors.Is(err, redisemaphore.ErrAcquireTimeout) {
		t.Fatalf("Run() error = %v, want ErrAcquireTimeout", err)
	}
	if called.Load() {
		t.Fatal("timed-out acquisition called callback")
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Waiters == 0
	})

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestSemaphoreCallbackPanicStillCleansUp(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	panicValue := "callback exploded"
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		_ = sem.Run(context.Background(), redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "panic",
		}, func(context.Context) error {
			panic(panicValue)
		})
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want %#v", recovered, panicValue)
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 0 && snapshot.Waiters == 0
	})

	called := false
	err = sem.Run(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "after-panic",
	}, func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Run() after panic: %v", err)
	}
	if !called {
		t.Fatal("callback after panic was not called")
	}
}

func TestSemaphoreRejectsMismatchedNamespaceConfiguration(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "high", "low")
	first, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("first NewSemaphore(): %v", err)
	}
	if first == nil {
		t.Fatal("first NewSemaphore() returned nil")
	}

	config.Capacity = 2
	second, err := redisemaphore.NewSemaphore(client, config)
	if second != nil {
		t.Error("mismatched NewSemaphore() returned a non-nil semaphore")
	}
	if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
		t.Fatalf("mismatched NewSemaphore() error = %v, want ErrInvalidConfig", err)
	}
}

func TestSemaphoreRejectsTamperedConfigurationRecord(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	first, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("first NewSemaphore(): %v", err)
	}
	if first == nil {
		t.Fatal("first NewSemaphore() returned nil")
	}

	keys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil {
		t.Fatalf("find semaphore configuration key: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("found configuration keys %v, want exactly one", keys)
	}
	if err := client.Set(context.Background(), keys[0], `{"tampered":true}`, 0).Err(); err != nil {
		t.Fatalf("tamper with configuration key: %v", err)
	}

	second, err := redisemaphore.NewSemaphore(client, config)
	if second != nil {
		t.Error("NewSemaphore() returned non-nil semaphore for tampered namespace")
	}
	if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
		t.Fatalf("NewSemaphore() error after tampering = %v, want ErrInvalidConfig", err)
	}
}
