package redisemaphore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitaifrey/redisemaphore"
	"github.com/redis/go-redis/v9"
)

type redisCommandHook struct {
	process func(context.Context, redis.Cmder, redis.ProcessHook) error
}

type testRedisServerError string

func (err testRedisServerError) Error() string { return string(err) }
func (testRedisServerError) RedisError()       {}

func (redisCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (hook redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return hook.process(ctx, cmd, next)
	}
}

func (redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func testMutexConfig(t *testing.T) redisemaphore.MutexConfig {
	t.Helper()
	return redisemaphore.MutexConfig{
		Namespace:      testNamespace(t),
		AcquireTimeout: 2 * time.Second,
		LeaseTTL:       600 * time.Millisecond,
		PollInitial:    5 * time.Millisecond,
		PollMax:        20 * time.Millisecond,
		CleanupTimeout: 40 * time.Millisecond,
	}
}

func TestNewMutexRejectsInvalidConfiguration(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))

	tests := []struct {
		name   string
		config redisemaphore.MutexConfig
	}{
		{name: "blank namespace", config: redisemaphore.MutexConfig{}},
		{
			name: "poll bounds reversed",
			config: redisemaphore.MutexConfig{
				Namespace: "invalid-poll", PollInitial: 2 * time.Second, PollMax: time.Second,
			},
		},
		{
			name: "lease too short for cleanup",
			config: redisemaphore.MutexConfig{
				Namespace: "invalid-lease", LeaseTTL: 30 * time.Millisecond, CleanupTimeout: 20 * time.Millisecond,
			},
		},
		{
			name: "duration not millisecond aligned",
			config: redisemaphore.MutexConfig{
				Namespace: "fractional-millisecond", PollInitial: 1500 * time.Microsecond,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutex, err := redisemaphore.NewMutex(client, tt.config)
			if mutex != nil {
				t.Errorf("NewMutex() returned non-nil mutex for invalid configuration")
			}
			if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
				t.Fatalf("NewMutex() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestMutexRunSerializesIndependentClients(t *testing.T) {
	addr := startMiniRedis(t)
	config := testMutexConfig(t)
	first, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("first NewMutex(): %v", err)
	}
	second, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("second NewMutex(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- first.Run(ctx, "first", func(workCtx context.Context) error {
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
		t.Fatal("first mutex callback did not start")
	}

	secondStarted := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- second.Run(ctx, "second", func(context.Context) error {
			close(secondStarted)
			return nil
		})
	}()
	select {
	case <-secondStarted:
		t.Fatal("second mutex callback overlapped first")
	case <-time.After(80 * time.Millisecond):
	}

	close(releaseFirst)
	if err := receiveError(t, firstResult, time.Second); err != nil {
		t.Fatalf("first Run(): %v", err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second mutex callback did not start after release")
	}
	if err := receiveError(t, secondResult, time.Second); err != nil {
		t.Fatalf("second Run(): %v", err)
	}
}

func TestMutexRunRejectsInvalidRequest(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	mutex, err := redisemaphore.NewMutex(client, testMutexConfig(t))
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	err = mutex.Run(context.Background(), "", func(context.Context) error { return nil })
	if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
		t.Fatalf("Run() with blank request ID error = %v, want ErrInvalidConfig", err)
	}
	err = mutex.Run(context.Background(), "request", nil)
	if !errors.Is(err, redisemaphore.ErrInvalidConfig) {
		t.Fatalf("Run() with nil callback error = %v, want ErrInvalidConfig", err)
	}
}

func TestMutexParentCancellationKeepsRenewingDuringCallbackWindDown(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var renewals atomic.Int64
	config := testMutexConfig(t)
	config.LeaseTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	config.Observer = redisemaphore.ObserverFunc(func(_ context.Context, event redisemaphore.Event) {
		if event.Type == redisemaphore.EventLeaseRenewed {
			renewals.Add(1)
		}
	})
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	callbackStarted := make(chan struct{})
	callbackCanceled := make(chan struct{})
	finishWindDown := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(runCtx, "wind-down", func(workCtx context.Context) error {
			close(callbackStarted)
			<-workCtx.Done()
			close(callbackCanceled)
			<-finishWindDown
			return workCtx.Err()
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("mutex callback did not start")
	}

	deadline := time.Now().Add(time.Second)
	for renewals.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if renewals.Load() == 0 {
		t.Fatal("mutex lease was not renewed before cancellation")
	}

	cancelRun()
	select {
	case <-callbackCanceled:
	case <-time.After(time.Second):
		t.Fatal("derived callback context was not canceled")
	}
	renewalsAtCancel := renewals.Load()
	deadline = time.Now().Add(time.Second)
	for renewals.Load() <= renewalsAtCancel && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if renewals.Load() <= renewalsAtCancel {
		t.Fatal("lease renewal stopped before callback wind-down completed")
	}

	close(finishWindDown)
	if err := receiveError(t, result, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestMutexCanceledWaiterDoesNotRunCallback(t *testing.T) {
	addr := startMiniRedis(t)
	config := testMutexConfig(t)
	holder, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("holder NewMutex(): %v", err)
	}
	waiter, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("waiter NewMutex(): %v", err)
	}

	testCtx, cancelTest := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelTest()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- holder.Run(testCtx, "holder", func(workCtx context.Context) error {
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
		waiterResult <- waiter.Run(waitCtx, "waiter", func(context.Context) error {
			waiterCalled.Store(true)
			return nil
		})
	}()
	time.Sleep(30 * time.Millisecond)
	cancelWait()
	if err := receiveError(t, waiterResult, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter Run() error = %v, want context.Canceled", err)
	}
	if waiterCalled.Load() {
		t.Fatal("canceled mutex waiter called its callback")
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestMutexAcquireTimeoutDoesNotRunCallback(t *testing.T) {
	addr := startMiniRedis(t)
	config := testMutexConfig(t)
	config.AcquireTimeout = 100 * time.Millisecond
	holder, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("holder NewMutex(): %v", err)
	}
	waiter, err := redisemaphore.NewMutex(newRedisClient(t, addr), config)
	if err != nil {
		t.Fatalf("waiter NewMutex(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderResult := make(chan error, 1)
	go func() {
		holderResult <- holder.Run(ctx, "holder", func(workCtx context.Context) error {
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
	err = waiter.Run(ctx, "timeout", func(context.Context) error {
		called.Store(true)
		return nil
	})
	if !errors.Is(err, redisemaphore.ErrAcquireTimeout) {
		t.Fatalf("waiter Run() error = %v, want ErrAcquireTimeout", err)
	}
	if called.Load() {
		t.Fatal("timed-out mutex acquisition called callback")
	}

	close(releaseHolder)
	if err := receiveError(t, holderResult, time.Second); err != nil {
		t.Fatalf("holder Run(): %v", err)
	}
}

func TestMutexStaleOwnerCannotDeleteReplacement(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testMutexConfig(t)
	config.LeaseTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	callbackStarted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(ctx, "stale-owner", func(workCtx context.Context) error {
			close(callbackStarted)
			<-workCtx.Done()
			return workCtx.Err()
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("mutex callback did not start")
	}

	keys, err := client.Keys(context.Background(), "redisemaphore:*:v1:mutex").Result()
	if err != nil {
		t.Fatalf("find mutex key: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("found mutex keys %v, want exactly one", keys)
	}
	const replacementToken = "replacement-owner-token"
	if err := client.Set(context.Background(), keys[0], replacementToken, 2*time.Second).Err(); err != nil {
		t.Fatalf("replace mutex owner: %v", err)
	}

	if err := receiveError(t, result, time.Second); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Run() after ownership replacement error = %v, want ErrLeaseLost", err)
	}
	owner, err := client.Get(context.Background(), keys[0]).Result()
	if err != nil {
		t.Fatalf("read replacement owner: %v", err)
	}
	if owner != replacementToken {
		t.Fatalf("owner after stale cleanup = %q, want replacement token", owner)
	}
}

func TestMutexReleaseReportsLeaseLostWithoutDeletingReplacement(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	config := testMutexConfig(t)
	config.LeaseTTL = 900 * time.Millisecond
	config.CleanupTimeout = 50 * time.Millisecond
	leaseLostEvents := make(chan redisemaphore.Event, 1)
	config.Observer = redisemaphore.ObserverFunc(func(_ context.Context, event redisemaphore.Event) {
		if event.Type == redisemaphore.EventLeaseLost {
			select {
			case leaseLostEvents <- event:
			default:
			}
		}
	})
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	callbackStarted := make(chan struct{})
	finishCallback := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(ctx, "replaced-before-release", func(workCtx context.Context) error {
			close(callbackStarted)
			select {
			case <-finishCallback:
				return nil
			case <-workCtx.Done():
				return workCtx.Err()
			}
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("mutex callback did not start")
	}

	keys, err := client.Keys(context.Background(), "redisemaphore:*:v1:mutex").Result()
	if err != nil {
		t.Fatalf("find mutex key: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("found mutex keys %v, want exactly one", keys)
	}
	const replacementToken = "replacement-immediately-before-release"
	if err := client.Set(context.Background(), keys[0], replacementToken, 2*time.Second).Err(); err != nil {
		t.Fatalf("replace mutex owner: %v", err)
	}
	close(finishCallback)

	if err := receiveError(t, result, time.Second); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Run() release mismatch error = %v, want ErrLeaseLost", err)
	}
	owner, err := client.Get(context.Background(), keys[0]).Result()
	if err != nil {
		t.Fatalf("read replacement owner: %v", err)
	}
	if owner != replacementToken {
		t.Fatalf("owner after mismatched release = %q, want replacement token", owner)
	}
	select {
	case event := <-leaseLostEvents:
		if !errors.Is(event.Err, redisemaphore.ErrLeaseLost) {
			t.Fatalf("lease-loss event error = %v, want ErrLeaseLost", event.Err)
		}
		if event.RequestID != "replaced-before-release" {
			t.Fatalf("lease-loss event request ID = %q, want replaced-before-release", event.RequestID)
		}
	case <-time.After(time.Second):
		t.Fatal("release mismatch did not emit EventLeaseLost")
	}
}

func TestMutexCleanupWaitTimeoutDoesNotHideDefinitiveMissingToken(t *testing.T) {
	addr := startMiniRedis(t)
	client := newRedisClient(t, addr)
	replacementClient := newRedisClient(t, addr)
	config := testMutexConfig(t)
	config.LeaseTTL = 900 * time.Millisecond
	config.CleanupTimeout = 180 * time.Millisecond

	var armReleaseBlock atomic.Bool
	var releaseBlocked atomic.Bool
	blocked := make(chan struct{})
	unblock := make(chan struct{}, 1)
	defer close(unblock)
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if armReleaseBlock.Load() && releaseBlocked.CompareAndSwap(false, true) {
			close(blocked)
			<-unblock
			// Execute after the per-attempt watchdog has expired. The command's
			// definitive result, rather than the old waiting context, is what the
			// cleanup state machine must interpret.
			return next(context.Background(), cmd)
		}
		return next(ctx, cmd)
	}})

	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}
	// Load the release script before fault injection so the blocked operation
	// has one definitive EVALSHA result rather than a NOSCRIPT fallback whose
	// second command observes the canceled per-attempt context.
	if err := mutex.Run(context.Background(), "prime-cleanup-script", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("prime Run(): %v", err)
	}
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(context.Background(), "late-definitive-missing", func(context.Context) error {
			armReleaseBlock.Store(true)
			return nil
		})
	}()

	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("release did not reach blocking hook")
	}
	// Let the first local per-attempt wait (CleanupTimeout/2) expire while the
	// one Redis command is still pending.
	time.Sleep(config.CleanupTimeout/2 + 20*time.Millisecond)
	keys, err := replacementClient.Keys(context.Background(), "redisemaphore:*:v1:mutex").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("find mutex key: keys=%v error=%v", keys, err)
	}
	const replacementToken = "replacement-after-local-cleanup-timeout"
	if err := replacementClient.Set(context.Background(), keys[0], replacementToken, 2*time.Second).Err(); err != nil {
		t.Fatalf("install replacement token: %v", err)
	}
	unblock <- struct{}{}

	if err := receiveError(t, result, time.Second); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Run() after definitive missing-token cleanup = %v, want ErrLeaseLost", err)
	}
	owner, err := replacementClient.Get(context.Background(), keys[0]).Result()
	if err != nil {
		t.Fatalf("read replacement token: %v", err)
	}
	if owner != replacementToken {
		t.Fatalf("owner after late cleanup = %q, want %q", owner, replacementToken)
	}
}

func TestMutexDefinitiveServerErrorDoesNotMakeMissingReleaseAmbiguous(t *testing.T) {
	addr := startMiniRedis(t)
	client := newRedisClient(t, addr)
	replacementClient := newRedisClient(t, addr)
	config := testMutexConfig(t)
	config.LeaseTTL = 900 * time.Millisecond
	config.CleanupTimeout = 90 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}
	if err := mutex.Run(context.Background(), "prime-server-error-release", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("prime Run(): %v", err)
	}

	var inject atomic.Bool
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if inject.CompareAndSwap(true, false) {
			keys, keysErr := replacementClient.Keys(context.Background(), "redisemaphore:*:v1:mutex").Result()
			if keysErr != nil || len(keys) != 1 {
				return fmt.Errorf("find mutex key during fault: keys=%v error=%w", keys, keysErr)
			}
			if setErr := replacementClient.Set(context.Background(), keys[0], "replacement-after-readonly", 2*time.Second).Err(); setErr != nil {
				return fmt.Errorf("install replacement during fault: %w", setErr)
			}
			// Redis error replies are definitive non-execution. A later missing
			// token cannot be treated as confirmation that this command committed.
			return testRedisServerError("READONLY You can't write against a read only replica")
		}
		return next(ctx, cmd)
	}})

	err = mutex.Run(context.Background(), "definitive-server-error", func(context.Context) error {
		inject.Store(true)
		return nil
	})
	if !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Run() after READONLY then missing release = %v, want ErrLeaseLost", err)
	}
}

func TestMutexRetriesAmbiguousReleaseWithSameToken(t *testing.T) {
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
			config := testMutexConfig(t)
			mutex, err := redisemaphore.NewMutex(client, config)
			if err != nil {
				t.Fatalf("NewMutex(): %v", err)
			}

			// Prime the release script so the fault hook can target its EVALSHA
			// by shape without relying on an internal script hash.
			if err := mutex.Run(context.Background(), "prime", func(context.Context) error { return nil }); err != nil {
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

			if err := mutex.Run(context.Background(), "faulted-release", func(context.Context) error {
				// Renewal is still asleep when this immediate callback returns, so
				// the next Redis command is the token-checked release.
				armed.Store(true)
				return nil
			}); err != nil {
				t.Fatalf("Run() with ambiguous release: %v", err)
			}
			if !injected.Load() {
				t.Fatal("release fault hook did not trigger")
			}
			if err := mutex.Run(context.Background(), "after-fault", func(context.Context) error { return nil }); err != nil {
				t.Fatalf("Run() after ambiguous release: %v", err)
			}
		})
	}
}

func TestMutexRecoversCommittedAcquireWithLostResponse(t *testing.T) {
	mr := startMiniRedisServer(t)
	client := newRedisClient(t, mr.Addr())
	config := testMutexConfig(t)
	config.LeaseTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}
	if err := mutex.Run(context.Background(), "prime", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("prime Run(): %v", err)
	}

	var armed atomic.Bool
	armed.Store(true)
	var injected atomic.Bool
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if armed.CompareAndSwap(true, false) && injected.CompareAndSwap(false, true) {
			if err := next(ctx, cmd); err != nil {
				return err
			}
			// Move the server close to expiry before losing the acknowledgement.
			// A read-only confirmation would leave too little lease to safely run;
			// the same-token acquisition retry must refresh it atomically.
			mr.FastForward(150 * time.Millisecond)
			return io.ErrUnexpectedEOF
		}
		return next(ctx, cmd)
	}})

	var callbackCalls atomic.Int64
	ttlAtCallback := make(chan time.Duration, 1)
	err = mutex.Run(context.Background(), "ambiguous-acquire", func(ctx context.Context) error {
		callbackCalls.Add(1)
		keys, keysErr := client.Keys(ctx, "redisemaphore:*:v1:mutex").Result()
		if keysErr != nil {
			return keysErr
		}
		if len(keys) != 1 {
			return fmt.Errorf("found mutex keys %v, want exactly one", keys)
		}
		ttl, ttlErr := client.PTTL(ctx, keys[0]).Result()
		if ttlErr != nil {
			return ttlErr
		}
		ttlAtCallback <- ttl
		return nil
	})
	if err != nil {
		t.Fatalf("Run() after committed acquire with lost response: %v", err)
	}
	if !injected.Load() {
		t.Fatal("acquire fault hook did not trigger")
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("callback calls = %d, want exactly 1", got)
	}
	if got := <-ttlAtCallback; got < 120*time.Millisecond {
		t.Fatalf("lease TTL at callback = %s, want a same-token refresh before callback", got)
	}
}

func TestMutexDelayedAcquireReplyIsReconfirmedBeforeCallback(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var armed atomic.Bool
	armed.Store(true)
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

	config := testMutexConfig(t)
	config.LeaseTTL = 180 * time.Millisecond
	config.CleanupTimeout = 30 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	commandsAtCallback := make(chan int64, 1)
	err = mutex.Run(context.Background(), "delayed-acquire", func(context.Context) error {
		commandsAtCallback <- successfulCommands.Load()
		return nil
	})
	if err != nil {
		t.Fatalf("Run() with delayed acquire response: %v", err)
	}
	select {
	case <-delayStarted:
	default:
		t.Fatal("acquire delay hook did not trigger")
	}
	if got := <-commandsAtCallback; got < 2 {
		t.Fatalf("successful Redis commands before callback = %d, want at least 2 to reconfirm the delayed acquisition", got)
	}
}

func TestMutexDelayedCommittedRenewalPastCutoffLosesLease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var armed atomic.Bool
	delayStarted := make(chan struct{})
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		if armed.CompareAndSwap(true, false) {
			close(delayStarted)
			time.Sleep(230 * time.Millisecond)
		}
		return nil
	}})

	config := testMutexConfig(t)
	config.LeaseTTL = 300 * time.Millisecond
	config.CleanupTimeout = 50 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	runCtx, cancelRun := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRun()
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(runCtx, "delayed-renewal", func(workCtx context.Context) error {
			// No Redis command occurs between arming here and the first renewal.
			armed.Store(true)
			<-workCtx.Done()
			return context.Cause(workCtx)
		})
	}()
	select {
	case <-delayStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("renewal delay hook did not trigger")
	}
	// The renewal committed, but its acknowledgement is withheld past the
	// safety cutoff. The single-flight runner must retain that command's old
	// start time and conservatively report lease loss; it cannot launch a
	// parallel command to manufacture a newer confirmation.
	if err := receiveError(t, result, time.Second); !errors.Is(err, redisemaphore.ErrLeaseLost) {
		t.Fatalf("Run() after delayed committed renewal error = %v, want ErrLeaseLost", err)
	}
}

func TestMutexBlockedPreSendRenewalTriggersLeaseWatchdog(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var blockRenewals atomic.Bool
	var observedBlock atomic.Bool
	renewalBlocked := make(chan struct{})
	unblockRenewals := make(chan struct{})
	defer close(unblockRenewals)
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if blockRenewals.Load() {
			if observedBlock.CompareAndSwap(false, true) {
				close(renewalBlocked)
			}
			// Deliberately ignore ctx. This models a client or hook stuck before
			// bytes are sent, where a context deadline cannot unblock the call.
			<-unblockRenewals
		}
		return next(ctx, cmd)
	}})

	config := testMutexConfig(t)
	config.LeaseTTL = 600 * time.Millisecond
	config.CleanupTimeout = 40 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	callbackArmed := make(chan time.Time, 1)
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(context.Background(), "blocked-renewal", func(workCtx context.Context) error {
			blockRenewals.Store(true)
			callbackArmed <- time.Now()
			<-workCtx.Done()
			// Let the token-safe release pass while the single abandoned renewal
			// operation remains blocked until the test's deferred cleanup.
			blockRenewals.Store(false)
			return context.Cause(workCtx)
		})
	}()

	armedAt := <-callbackArmed
	select {
	case <-renewalBlocked:
	case <-time.After(config.LeaseTTL):
		t.Fatal("renewal did not reach the pre-send blocking hook")
	}
	select {
	case runErr := <-result:
		if !errors.Is(runErr, redisemaphore.ErrLeaseLost) {
			t.Fatalf("Run() with blocked renewal error = %v, want ErrLeaseLost", runErr)
		}
		safetyCutoff := config.LeaseTTL - config.LeaseTTL/3
		if elapsed := time.Since(armedAt); elapsed > safetyCutoff+75*time.Millisecond {
			t.Fatalf("lease watchdog canceled callback after %s, want by safety cutoff %s (plus scheduler allowance)", elapsed, safetyCutoff)
		}
	case <-time.After(config.LeaseTTL - config.LeaseTTL/3 + 150*time.Millisecond):
		t.Fatal("Run() did not return after the lease safety cutoff")
	}
}

func TestMutexBlockedPreSendCleanupHonorsWallClockTimeout(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	var blockCleanup atomic.Bool
	var observedBlock atomic.Bool
	cleanupBlocked := make(chan struct{})
	unblockCleanup := make(chan struct{})
	defer close(unblockCleanup)
	client.AddHook(redisCommandHook{process: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if blockCleanup.Load() {
			if observedBlock.CompareAndSwap(false, true) {
				close(cleanupBlocked)
			}
			// Ignore the per-attempt context to prove the outer cleanup bound
			// does not depend on the Redis client honoring cancellation.
			<-unblockCleanup
		}
		return next(ctx, cmd)
	}})

	config := testMutexConfig(t)
	config.CleanupTimeout = 80 * time.Millisecond
	mutex, err := redisemaphore.NewMutex(client, config)
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	callbackReturned := make(chan time.Time, 1)
	result := make(chan error, 1)
	go func() {
		result <- mutex.Run(context.Background(), "blocked-cleanup", func(context.Context) error {
			blockCleanup.Store(true)
			callbackReturned <- time.Now()
			return nil
		})
	}()

	startedAt := <-callbackReturned
	select {
	case <-cleanupBlocked:
	case <-time.After(config.CleanupTimeout):
		t.Fatal("cleanup did not reach the pre-send blocking hook")
	}
	select {
	case runErr := <-result:
		if !errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("Run() with blocked cleanup error = %v, want context deadline", runErr)
		}
		if elapsed := time.Since(startedAt); elapsed > config.CleanupTimeout+100*time.Millisecond {
			t.Fatalf("blocked cleanup returned after %s, want within cleanup timeout %s (plus scheduler allowance)", elapsed, config.CleanupTimeout)
		}
	case <-time.After(config.CleanupTimeout + 250*time.Millisecond):
		t.Fatal("Run() remained blocked past the cleanup wall-clock bound")
	}
	blockCleanup.Store(false)
}

func TestMutexCallbackPanicStillReleasesLease(t *testing.T) {
	client := newRedisClient(t, startMiniRedis(t))
	mutex, err := redisemaphore.NewMutex(client, testMutexConfig(t))
	if err != nil {
		t.Fatalf("NewMutex(): %v", err)
	}

	panicValue := "mutex callback exploded"
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		_ = mutex.Run(context.Background(), "panic", func(context.Context) error {
			panic(panicValue)
		})
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want %#v", recovered, panicValue)
	}

	called := false
	err = mutex.Run(context.Background(), "after-panic", func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Run() after panic: %v", err)
	}
	if !called {
		t.Fatal("mutex callback after panic was not called")
	}
}
