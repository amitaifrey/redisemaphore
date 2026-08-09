package redisemaphore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

type loggerFunc func(context.Context, slog.Level, string, ...slog.Attr)

type testRedisServerError string

func (err testRedisServerError) Error() string { return string(err) }
func (testRedisServerError) RedisError()       {}

func (f loggerFunc) LogAttrs(ctx context.Context, level slog.Level, message string, attrs ...slog.Attr) {
	f(ctx, level, message, attrs...)
}

func logAttrString(attrs []slog.Attr, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.String()
		}
	}
	return ""
}

func acquireAndRelease(ctx context.Context, sem *redisemaphore.Semaphore, request redisemaphore.AcquireRequest) error {
	permit, err := sem.Acquire(ctx, request)
	if err != nil {
		return err
	}
	return permit.Release()
}

func semaphoreBase(t *testing.T, client redis.UniversalClient) string {
	t.Helper()
	keys, err := client.Keys(context.Background(), "redisemaphore:*:v1:semaphore:config").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("find semaphore config key: keys=%v error=%v", keys, err)
	}
	return strings.TrimSuffix(keys[0], ":config")
}

func installReplacementPermit(
	t *testing.T,
	client redis.UniversalClient,
	base string,
	requestID string,
	replacementToken string,
	ttl time.Duration,
) {
	t.Helper()
	now, err := client.Time(context.Background()).Result()
	if err != nil {
		t.Fatalf("Redis TIME before installing replacement: %v", err)
	}
	if err := client.ZAdd(context.Background(), base+":holders", redis.Z{
		Score: float64(now.Add(ttl).UnixMilli()), Member: replacementToken,
	}).Err(); err != nil {
		t.Fatalf("install replacement holder: %v", err)
	}
	if err := client.HSet(context.Background(), base+":token-request", replacementToken, requestID).Err(); err != nil {
		t.Fatalf("install replacement token metadata: %v", err)
	}
	if err := client.HSet(context.Background(), base+":active-request", requestID, replacementToken).Err(); err != nil {
		t.Fatalf("install replacement active request: %v", err)
	}
}

func assertReplacementPermit(
	t *testing.T,
	client redis.UniversalClient,
	base string,
	requestID string,
	replacementToken string,
) {
	t.Helper()
	active, err := client.HGet(context.Background(), base+":active-request", requestID).Result()
	if err != nil {
		t.Fatalf("read replacement active request: %v", err)
	}
	if active != replacementToken {
		t.Fatalf("active token after stale cleanup = %q, want %q", active, replacementToken)
	}
	if _, err := client.ZScore(context.Background(), base+":holders", replacementToken).Result(); err != nil {
		t.Fatalf("replacement holder after stale cleanup: %v", err)
	}
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

func TestSemaphoreAcquireOwnsAndReleaseRelinquishesPermit(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "request-1",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	if permit == nil {
		t.Fatal("Acquire() returned a nil permit")
	}
	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() while held: %v", err)
	}
	if snapshot.Holders != 1 {
		t.Fatalf("holders while permit is held = %d, want 1", snapshot.Holders)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("Release(): %v", err)
	}

	snapshot, err = sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() after release: %v", err)
	}
	if snapshot.Holders != 0 || snapshot.Waiters != 0 {
		t.Fatalf("snapshot after Release() = %+v, want no holders or waiters", snapshot)
	}
}

func TestPermitReleaseIsConcurrentIdempotentAndCachesResultAcrossCopies(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	// Prime the release script so command counting below sees one EVALSHA and
	// does not depend on go-redis's transparent SCRIPT LOAD fallback.
	if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime",
	}); err != nil {
		t.Fatalf("prime permit lifecycle: %v", err)
	}

	var armed atomic.Bool
	var releaseCommands atomic.Int64
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if armed.Load() && (cmd.Name() == "eval" || cmd.Name() == "evalsha") {
			releaseCommands.Add(1)
		}
		return next(ctx, cmd)
	}})

	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "concurrent-release",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	permitCopy := *permit

	const callers = 32
	start := make(chan struct{})
	results := make(chan error, callers)
	var wg sync.WaitGroup
	armed.Store(true)
	for index := range callers {
		releaser := permit
		if index%2 != 0 {
			releaser = &permitCopy
		}
		wg.Add(1)
		go func(releaser *redisemaphore.Permit) {
			defer wg.Done()
			<-start
			results <- releaser.Release()
		}(releaser)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent Release(): %v", err)
		}
	}
	if got := releaseCommands.Load(); got != 1 {
		t.Fatalf("Redis release commands = %d, want exactly 1", got)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("repeated Release(): %v", err)
	}
	if err := permitCopy.Release(); err != nil {
		t.Fatalf("Release() through copied Permit: %v", err)
	}
	if got := releaseCommands.Load(); got != 1 {
		t.Fatalf("Redis release commands after repeated Release() = %d, want cached result with 1 command", got)
	}
	if cause := context.Cause(permit.Context()); !errors.Is(cause, context.Canceled) {
		t.Fatalf("permit context cause after Release() = %v, want context.Canceled", cause)
	}

	armed.Store(false)
	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.Holders != 0 || snapshot.Waiters != 0 {
		t.Fatalf("snapshot after concurrent release = %+v, want no holders or waiters", snapshot)
	}
}

func TestSemaphoreDelayedAcquireReplyIsReconfirmedBeforePermitReturns(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime",
	}); err != nil {
		t.Fatalf("prime permit lifecycle: %v", err)
	}

	var armed atomic.Bool
	var successfulCommands atomic.Int64
	delayStarted := make(chan struct{})
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		successfulCommands.Add(1)
		if armed.CompareAndSwap(true, false) {
			close(delayStarted)
			time.Sleep(140 * time.Millisecond)
		}
		return nil
	}})

	armed.Store(true)
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "delayed-acquire",
	})
	if err != nil {
		t.Fatalf("Acquire() with delayed response: %v", err)
	}
	select {
	case <-delayStarted:
	default:
		t.Fatal("acquire delay hook did not trigger")
	}
	if got := successfulCommands.Load(); got < 2 {
		t.Fatalf("successful Redis commands before Permit return = %d, want at least 2 to reconfirm the delayed acquisition", got)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("Release(): %v", err)
	}
}

func TestPermitDelayedCommittedRenewalPastSafetyCutoffLosesLease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 300 * time.Millisecond
	config.CleanupTimeout = 50 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	var armed atomic.Bool
	delayStarted := make(chan struct{})
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		if armed.CompareAndSwap(true, false) {
			close(delayStarted)
			// The renewal commits, but its acknowledgement arrives after the
			// original confirmation's safety cutoff.
			time.Sleep(230 * time.Millisecond)
		}
		return nil
	}})
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "delayed-renewal",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	armed.Store(true)

	select {
	case <-delayStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("renewal delay hook did not trigger")
	}
	select {
	case <-permit.Context().Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Permit context remained live past the renewal safety cutoff")
	}
	if cause := context.Cause(permit.Context()); !errors.Is(cause, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Permit context cause = %v, want ErrLeaseLost", cause)
	}
	if err := permit.Release(); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Release() after delayed committed renewal = %v, want ErrLeaseLost", err)
	}
}

func TestPermitBlockedPreSendRenewalTriggersLeaseWatchdog(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 600 * time.Millisecond
	config.CleanupTimeout = 40 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	var blockRenewals atomic.Bool
	var observedBlock atomic.Bool
	renewalBlocked := make(chan struct{})
	unblockRenewals := make(chan struct{})
	t.Cleanup(func() { close(unblockRenewals) })
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if blockRenewals.Load() {
			if observedBlock.CompareAndSwap(false, true) {
				close(renewalBlocked)
			}
			// Deliberately ignore ctx to model a client stuck before sending.
			<-unblockRenewals
		}
		return next(ctx, cmd)
	}})
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "blocked-renewal",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	armedAt := time.Now()
	blockRenewals.Store(true)
	select {
	case <-renewalBlocked:
	case <-time.After(config.PermitTTL):
		t.Fatal("renewal did not reach the pre-send blocking hook")
	}
	safetyCutoff := config.PermitTTL - config.PermitTTL/3
	select {
	case <-permit.Context().Done():
		if cause := context.Cause(permit.Context()); !errors.Is(cause, redisemaphore.ErrLeaseLost) {
			t.Fatalf("Permit context cause = %v, want ErrLeaseLost", cause)
		}
		if elapsed := time.Since(armedAt); elapsed > safetyCutoff+75*time.Millisecond {
			t.Fatalf("lease watchdog canceled Permit context after %s, want by safety cutoff %s (plus scheduler allowance)", elapsed, safetyCutoff)
		}
	case <-time.After(safetyCutoff + 150*time.Millisecond):
		t.Fatal("Permit context remained live past the lease safety cutoff")
	}

	// Let token-safe cleanup use an unblocked command while the abandoned
	// renewal remains parked until test cleanup.
	blockRenewals.Store(false)
	if err := permit.Release(); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Release() after blocked renewal = %v, want ErrLeaseLost", err)
	}
}

func TestPermitBlockedPreSendReleaseHonorsCleanupTimeout(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.CleanupTimeout = 80 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	var blockCleanup atomic.Bool
	var observedBlock atomic.Bool
	cleanupBlocked := make(chan struct{})
	unblockCleanup := make(chan struct{})
	t.Cleanup(func() {
		blockCleanup.Store(false)
		close(unblockCleanup)
	})
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if blockCleanup.Load() {
			if observedBlock.CompareAndSwap(false, true) {
				close(cleanupBlocked)
			}
			// Ignore the per-attempt context to prove the outer wall-clock
			// cleanup bound does not rely on the client honoring cancellation.
			<-unblockCleanup
		}
		return next(ctx, cmd)
	}})
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "blocked-release",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	blockCleanup.Store(true)
	startedAt := time.Now()
	result := make(chan error, 1)
	go func() { result <- permit.Release() }()
	select {
	case <-cleanupBlocked:
	case <-time.After(config.CleanupTimeout):
		t.Fatal("release did not reach the pre-send blocking hook")
	}
	select {
	case releaseErr := <-result:
		if !errors.Is(releaseErr, context.DeadlineExceeded) {
			t.Fatalf("Release() with blocked cleanup = %v, want context deadline", releaseErr)
		}
		if elapsed := time.Since(startedAt); elapsed > config.CleanupTimeout+100*time.Millisecond {
			t.Fatalf("blocked Release() returned after %s, want within cleanup timeout %s (plus scheduler allowance)", elapsed, config.CleanupTimeout)
		}
	case <-time.After(config.CleanupTimeout + 250*time.Millisecond):
		t.Fatal("Release() remained blocked past the cleanup wall-clock bound")
	}
}

func TestPermitDelayedCleanupReportsMissingAndPreservesReplacement(t *testing.T) {
	addr := startMiniRedis(t)
	client := newRedisClient(t, addr)
	replacementClient := newRedisClient(t, addr)
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 900 * time.Millisecond
	config.CleanupTimeout = 180 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime-cleanup-script",
	}); err != nil {
		t.Fatalf("prime permit lifecycle: %v", err)
	}
	base := semaphoreBase(t, replacementClient)
	var armReleaseBlock atomic.Bool
	var releaseBlocked atomic.Bool
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	unblockRelease := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(unblockRelease)
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if armReleaseBlock.Load() && releaseBlocked.CompareAndSwap(false, true) {
			close(blocked)
			<-unblock
			// Execute after the local per-attempt wait expires. The definitive
			// missing-token response must still win over that local timeout.
			return next(context.Background(), cmd)
		}
		return next(ctx, cmd)
	}})
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "late-definitive-missing",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	armReleaseBlock.Store(true)
	result := make(chan error, 1)
	go func() { result <- permit.Release() }()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("release did not reach blocking hook")
	}
	time.Sleep(config.CleanupTimeout/2 + 20*time.Millisecond)
	const replacementToken = "replacement-after-local-cleanup-timeout"
	installReplacementPermit(t, replacementClient, base, "late-definitive-missing", replacementToken, 2*time.Second)
	unblockRelease()

	if releaseErr := receiveError(t, result, time.Second); !errors.Is(releaseErr, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Release() after definitive missing-token cleanup = %v, want ErrLeaseLost", releaseErr)
	}
	assertReplacementPermit(t, replacementClient, base, "late-definitive-missing", replacementToken)
}

func TestPermitDefinitiveRedisErrorDoesNotMakeMissingReleaseAmbiguous(t *testing.T) {
	addr := startMiniRedis(t)
	client := newRedisClient(t, addr)
	replacementClient := newRedisClient(t, addr)
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 900 * time.Millisecond
	config.CleanupTimeout = 90 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}
	if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime-server-error-release",
	}); err != nil {
		t.Fatalf("prime permit lifecycle: %v", err)
	}
	base := semaphoreBase(t, replacementClient)
	var inject atomic.Bool
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if inject.CompareAndSwap(true, false) {
			// Redis error replies are definitive non-execution. A later missing
			// token must not be accepted as proof this command committed.
			return testRedisServerError("READONLY You can't write against a read only replica")
		}
		return next(ctx, cmd)
	}})
	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "definitive-server-error",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	const replacementToken = "replacement-after-readonly"
	installReplacementPermit(t, replacementClient, base, "definitive-server-error", replacementToken, 2*time.Second)
	inject.Store(true)

	if releaseErr := permit.Release(); !errors.Is(releaseErr, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Release() after READONLY then missing token = %v, want ErrLeaseLost", releaseErr)
	}
	assertReplacementPermit(t, replacementClient, base, "definitive-server-error", replacementToken)
}

func TestSemaphoreAcquireRejectsInvalidRequest(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	sem, err := redisemaphore.NewSemaphore(client, testSemaphoreConfig(t, 1, "default"))
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	tests := []struct {
		name    string
		ctx     context.Context
		request redisemaphore.AcquireRequest
	}{
		{
			name: "blank request ID", ctx: context.Background(),
			request: redisemaphore.AcquireRequest{Queue: "default"},
		},
		{
			name: "unknown queue", ctx: context.Background(),
			request: redisemaphore.AcquireRequest{Queue: "unknown", RequestID: "request"},
		},
		{
			name: "nil context", request: redisemaphore.AcquireRequest{Queue: "default", RequestID: "request"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permit, err := sem.Acquire(tt.ctx, tt.request)
			if permit != nil {
				t.Fatal("Acquire() returned a permit for an invalid request")
			}
			if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
				t.Fatalf("Acquire() error = %v, want ErrInvalidConfig", err)
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
			permit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: requestID,
			})
			if err != nil {
				errs <- err
				return
			}
			current := active.Add(1)
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
			case <-permit.Context().Done():
				err = context.Cause(permit.Context())
			case <-capacityReached:
				select {
				case <-permit.Context().Done():
					err = context.Cause(permit.Context())
				case <-time.After(10 * time.Millisecond):
				}
			}
			active.Add(-1)
			errs <- errors.Join(err, permit.Release())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("permit lifecycle failed: %v", err)
		}
	}
	if exceeded.Load() {
		t.Fatalf("permits exceeded configured capacity %d; observed maximum %d", config.Capacity, maximum.Load())
	}
	if got := maximum.Load(); got != int64(config.Capacity) {
		t.Fatalf("maximum concurrent permits = %d, want %d", got, config.Capacity)
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
	holderPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "high", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	order := make(chan string, 3)
	results := make(chan error, 3)
	launch := func(queue, requestID string) {
		go func() {
			permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
				Queue: queue, RequestID: requestID,
			})
			if acquireErr != nil {
				results <- acquireErr
				return
			}
			order <- requestID
			results <- permit.Release()
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

	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}

	wantOrder := []string{"high", "low-first", "low-second"}
	for i, want := range wantOrder {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("admission %d = %q, want %q; complete expected order %v", i, got, want, wantOrder)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for admission %d (%q)", i, want)
		}
	}
	for i := 0; i < len(wantOrder); i++ {
		if err := receiveError(t, results, time.Second); err != nil {
			t.Errorf("queued permit lifecycle failed: %v", err)
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
	firstPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "reusable",
	})
	if err != nil {
		t.Fatalf("first Acquire(): %v", err)
	}

	duplicatePermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "reusable",
	})
	if duplicatePermit != nil {
		t.Fatal("duplicate Acquire() returned a permit")
	}
	if !errors.Is(err, redisemaphore.ErrDuplicateRequest) {
		t.Fatalf("duplicate Acquire() error = %v, want ErrDuplicateRequest", err)
	}

	if err := firstPermit.Release(); err != nil {
		t.Fatalf("first Release(): %v", err)
	}

	reusedPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "reusable",
	})
	if err != nil {
		t.Fatalf("Acquire() after request ID reuse: %v", err)
	}
	if err := reusedPermit.Release(); err != nil {
		t.Fatalf("Release() reused permit: %v", err)
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
	holderPermit, err := sem.Acquire(testCtx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	waiterResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "canceled-waiter",
		})
		if permit != nil {
			waiterResult <- errors.Join(errors.New("canceled waiter acquired a permit"), permit.Release())
			return
		}
		waiterResult <- acquireErr
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.WaitersByQueue["default"] == 1
	})

	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 0
	})

	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}
}

func TestSemaphoreParentCancellationCancelsPermitContextButRenewsUntilRelease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	parentCtx, cancelParent := context.WithCancelCause(context.Background())
	permit, err := sem.Acquire(parentCtx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "canceled-holder",
	})
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	parentCause := errors.New("parent stopped protected work")
	cancelParent(parentCause)
	select {
	case <-permit.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("permit context was not canceled with its parent")
	}
	if cause := context.Cause(permit.Context()); !errors.Is(cause, parentCause) {
		t.Fatalf("permit context cause = %v, want parent cause %v", cause, parentCause)
	}

	// The work context and ownership lifecycle are intentionally separate. A
	// canceled parent stops protected work, but renewal keeps the slot owned
	// until that work has stopped and the caller explicitly releases it.
	time.Sleep(2*config.PermitTTL + 30*time.Millisecond)
	snapshot, err := sem.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() after parent cancellation: %v", err)
	}
	if snapshot.Holders != 1 {
		t.Fatalf("holders after parent cancellation and multiple TTLs = %d, want 1", snapshot.Holders)
	}

	contenderResult := make(chan error, 1)
	go func() {
		contenderResult <- acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "contender",
		})
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})

	if err := permit.Release(); err != nil {
		t.Fatalf("Release() after parent cancellation: %v", err)
	}
	if err := receiveError(t, contenderResult, time.Second); err != nil {
		t.Fatalf("contender lifecycle after explicit release: %v", err)
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
	holderPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waiterStarted := make(chan struct{})
	waiterResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "long-waiter",
		})
		if acquireErr != nil {
			waiterResult <- acquireErr
			return
		}
		close(waiterStarted)
		waiterResult <- permit.Release()
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
		t.Fatal("waiter acquired while capacity was held")
	default:
	}

	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	select {
	case <-waiterStarted:
	case <-time.After(time.Second):
		t.Fatal("live old waiter was not admitted after release")
	}
	if err := receiveError(t, waiterResult, time.Second); err != nil {
		t.Fatalf("waiter permit lifecycle: %v", err)
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
	holderPermit, err := holder.Acquire(testCtx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "latency-holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	waiterResult := make(chan error, 1)
	go func() {
		permit, acquireErr := waiter.Acquire(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "latency-waiter",
		})
		if permit != nil {
			waiterResult <- errors.Join(errors.New("latency waiter acquired while capacity was held"), permit.Release())
			return
		}
		waiterResult <- acquireErr
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
		t.Fatalf("waiter Acquire() error = %v, want context.Canceled", err)
	}
	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
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
	holderPermit, err := holder.Acquire(testCtx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "outage-holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(testCtx)
	waiterResult := make(chan error, 1)
	go func() {
		permit, acquireErr := waiter.Acquire(waitCtx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "outage-waiter",
		})
		if permit != nil {
			waiterResult <- errors.Join(errors.New("outage waiter acquired while capacity was held"), permit.Release())
			return
		}
		waiterResult <- acquireErr
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
		t.Fatalf("waiter Acquire() error = %v, want context.Canceled", err)
	}
	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
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

	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "live-request",
	})
	if err != nil {
		t.Fatalf("Acquire() behind stale head: %v", err)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("Release(): %v", err)
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
	if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "prime",
	}); err != nil {
		t.Fatalf("prime permit lifecycle: %v", err)
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
			// same token and refresh it before returning the Permit.
			mr.FastForward(150 * time.Millisecond)
			return io.ErrUnexpectedEOF
		}
		return next(ctx, cmd)
	}})

	permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "ambiguous-acquire",
	})
	if err != nil {
		t.Fatalf("Acquire() after committed acquire with lost response: %v", err)
	}
	if !injected.Load() {
		t.Fatal("acquisition fault hook did not trigger")
	}
	if activeToken := mr.HGet(activeRequestKey, "ambiguous-acquire"); activeToken != committedToken {
		t.Fatalf("active token after Acquire() = %q, want committed token %q", activeToken, committedToken)
	}
	if err := permit.Release(); err != nil {
		t.Fatalf("Release() after recovered acquisition: %v", err)
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

			// Prime the release script so arming immediately before Release targets its
			// EVALSHA rather than a SCRIPT LOAD fallback.
			if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "prime",
			}); err != nil {
				t.Fatalf("prime permit lifecycle: %v", err)
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

			permit, err := sem.Acquire(context.Background(), redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "faulted-release",
			})
			if err != nil {
				t.Fatalf("Acquire() before ambiguous release: %v", err)
			}
			// Renewal is still sleeping, so the next Redis command is the
			// token-checked release.
			armed.Store(true)
			if err := permit.Release(); err != nil {
				t.Fatalf("Release() with ambiguous response: %v", err)
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
			if err := acquireAndRelease(context.Background(), sem, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: "after-fault",
			}); err != nil {
				t.Fatalf("permit lifecycle after ambiguous release: %v", err)
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
			permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
				Queue: "default", RequestID: requestID,
			})
			if acquireErr != nil {
				results <- acquireErr
				return
			}
			current := active.Add(1)
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
			var workErr error
			select {
			case <-capacityReached:
				time.Sleep(5 * time.Millisecond)
			case <-permit.Context().Done():
				workErr = context.Cause(permit.Context())
			}
			active.Add(-1)
			results <- errors.Join(workErr, permit.Release())
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("permit lifecycle through cleanup backlog: %v", err)
		}
	}
	if exceeded.Load() {
		t.Fatalf("permits exceeded capacity %d; observed maximum %d", capacity, maximum.Load())
	}
	if got := maximum.Load(); got != capacity {
		t.Fatalf("maximum concurrent permits = %d, want %d", got, capacity)
	}
	remaining, err := client.ZCard(context.Background(), holdersKey).Result()
	if err != nil {
		t.Fatalf("count holders after backlog cleanup: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("holder records after backlog cleanup = %d, want 0", remaining)
	}
}

func TestSemaphoreRenewsPermitUntilRelease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var renewals atomic.Int64
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	config.Logger = loggerFunc(func(_ context.Context, _ slog.Level, message string, _ ...slog.Attr) {
		if message == "lease_renewed" {
			renewals.Add(1)
		}
	})
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	firstPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "renewed-holder",
	})
	if err != nil {
		t.Fatalf("acquire first permit: %v", err)
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
		permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "contender",
		})
		if acquireErr != nil {
			secondResult <- acquireErr
			return
		}
		close(secondStarted)
		secondResult <- permit.Release()
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})
	time.Sleep(config.PermitTTL + 30*time.Millisecond)
	select {
	case <-secondStarted:
		t.Fatal("contender acquired before renewed holder released")
	default:
	}

	if err := firstPermit.Release(); err != nil {
		t.Fatalf("release first permit: %v", err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not acquire after holder released")
	}
	if err := receiveError(t, secondResult, time.Second); err != nil {
		t.Fatalf("contender permit lifecycle: %v", err)
	}
}

func TestSemaphoreBlockingLoggerCannotConsumeLease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	loggerEntered := make(chan struct{})
	unblockLogger := make(chan struct{})
	defer close(unblockLogger)
	config := testSemaphoreConfig(t, 1, "default")
	config.PermitTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	config.Logger = loggerFunc(func(_ context.Context, _ slog.Level, message string, attrs ...slog.Attr) {
		if message == "acquire_success" && logAttrString(attrs, "request_id") == "holder" {
			close(loggerEntered)
			<-unblockLogger
		}
	})
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	holderPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	select {
	case <-loggerEntered:
	case <-time.After(time.Second):
		t.Fatal("logger did not receive acquisition event")
	}

	contenderStarted := make(chan struct{})
	contenderResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(ctx, redisemaphore.AcquireRequest{
			Queue: "default", RequestID: "contender",
		})
		if acquireErr != nil {
			contenderResult <- acquireErr
			return
		}
		close(contenderStarted)
		contenderResult <- permit.Release()
	}()
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Holders == 1 && snapshot.Waiters == 1
	})
	time.Sleep(2*config.PermitTTL + 30*time.Millisecond)
	select {
	case <-contenderStarted:
		t.Fatal("contender acquired while holder renewed with a blocked logger")
	default:
	}

	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	select {
	case <-contenderStarted:
	case <-time.After(time.Second):
		t.Fatal("contender did not acquire after holder released")
	}
	if err := receiveError(t, contenderResult, time.Second); err != nil {
		t.Fatalf("contender permit lifecycle: %v", err)
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
	holderPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "high", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(ctx)
	waiterResult := make(chan error, 1)
	go func() {
		permit, acquireErr := sem.Acquire(waitCtx, redisemaphore.AcquireRequest{
			Queue: "low", RequestID: "waiter",
		})
		if permit != nil {
			waiterResult <- errors.Join(errors.New("canceled waiter acquired a permit"), permit.Release())
			return
		}
		waiterResult <- acquireErr
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
	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
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

func TestSemaphoreAcquireTimeoutDoesNotReturnPermit(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testSemaphoreConfig(t, 1, "default")
	config.AcquireTimeout = 100 * time.Millisecond
	sem, err := redisemaphore.NewSemaphore(client, config)
	if err != nil {
		t.Fatalf("NewSemaphore(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	holderPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "holder",
	})
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	timedOutPermit, err := sem.Acquire(ctx, redisemaphore.AcquireRequest{
		Queue: "default", RequestID: "timeout",
	})
	if !errors.Is(err, redisemaphore.ErrAcquireTimeout) {
		t.Fatalf("Acquire() error = %v, want ErrAcquireTimeout", err)
	}
	if timedOutPermit != nil {
		t.Fatal("timed-out acquisition returned a permit")
	}
	waitForSnapshot(t, sem, time.Second, func(snapshot redisemaphore.Snapshot) bool {
		return snapshot.Waiters == 0
	})

	if err := holderPermit.Release(); err != nil {
		t.Fatalf("release holder: %v", err)
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
