package redisemaphore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultMutexAcquireTimeout = 30 * time.Second
	defaultMutexLeaseTTL       = 60 * time.Second
	defaultMutexPollInitial    = 100 * time.Millisecond
	defaultMutexPollMax        = time.Second
	defaultMutexCleanupTimeout = 2 * time.Second
)

var (
	errInvalidMutexScriptResponse = errors.New("redisemaphore: invalid mutex script response")
	mutexAcquireScript            = redis.NewScript(`
local owner = redis.call("GET", KEYS[1])
if owner == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return 1
end
if not owner then
  if redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX") then
    return 1
  end
end
return 0
`)
	mutexRenewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)
	mutexReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)
)

// MutexConfig configures a renewable, token-owned Redis mutex. Zero duration
// fields use their documented defaults.
type MutexConfig struct {
	Namespace string
	// AcquireTimeout bounds polling, but the Redis client's I/O timeout can
	// extend wall-clock return while one admission command is in flight.
	AcquireTimeout time.Duration
	LeaseTTL       time.Duration
	PollInitial    time.Duration
	PollMax        time.Duration
	CleanupTimeout time.Duration
	Observer       Observer
}

// Mutex serializes callbacks across all clients using the same namespace.
// Its lease is renewed while the callback is running.
type Mutex struct {
	redisClient    redis.UniversalClient
	namespace      string
	key            string
	acquireTimeout time.Duration
	leaseTTL       time.Duration
	pollInitial    time.Duration
	pollMax        time.Duration
	cleanupTimeout time.Duration
	observer       Observer
}

// NewMutex constructs a renewable mutex. The callback-oriented Run API keeps
// ownership and release tied to the code protected by the mutex.
func NewMutex(redisClient redis.UniversalClient, config MutexConfig) (*Mutex, error) {
	config = applyMutexDefaults(config)
	if err := validateMutexConfig(redisClient, config); err != nil {
		return nil, err
	}

	return &Mutex{
		redisClient:    redisClient,
		namespace:      config.Namespace,
		key:            newMutexKey(config.Namespace),
		acquireTimeout: config.AcquireTimeout,
		leaseTTL:       config.LeaseTTL,
		pollInitial:    config.PollInitial,
		pollMax:        config.PollMax,
		cleanupTimeout: config.CleanupTimeout,
		observer:       newNonBlockingObserver(config.Observer),
	}, nil
}

// Run waits for the mutex, invokes fn with a lease-aware context, and releases
// the mutex after fn returns. The callback context is canceled if the lease can
// no longer be confirmed. A callback panic is propagated after cleanup.
func (m *Mutex) Run(ctx context.Context, requestID string, fn func(context.Context) error) error {
	if ctx == nil {
		return invalidConfig("context must not be nil")
	}
	if strings.TrimSpace(requestID) == "" {
		return invalidConfig("request ID must not be empty")
	}
	if fn == nil {
		return invalidConfig("callback must not be nil")
	}

	token, err := newMutexToken()
	if err != nil {
		return fmt.Errorf("generate mutex ownership token: %w", err)
	}

	confirmedAt, err := m.acquire(ctx, requestID, token)
	if err != nil {
		return err
	}

	return runWithLease(ctx, leaseRunOptions{
		resource:       "mutex",
		namespace:      m.namespace,
		requestID:      requestID,
		ttl:            m.leaseTTL,
		pollInitial:    m.pollInitial,
		pollMax:        m.pollMax,
		cleanupTimeout: m.cleanupTimeout,
		observer:       m.observer,
		confirmedAt:    confirmedAt,
		renew: func(renewCtx context.Context) error {
			return m.renew(renewCtx, token)
		},
		release: func(releaseCtx context.Context) (bool, error) {
			return m.release(releaseCtx, token)
		},
	}, fn)
}

func (m *Mutex) acquire(ctx context.Context, requestID, token string) (time.Time, error) {
	startedAt := time.Now()
	emitEvent(ctx, m.observer, Event{
		Type:      EventAcquireStart,
		Resource:  "mutex",
		Namespace: m.namespace,
		RequestID: requestID,
	})

	waitCtx, cancelWait := context.WithTimeout(ctx, m.acquireTimeout)
	defer cancelWait()

	backoff := m.pollInitial
	var lastErr error
	for {
		attemptStarted := time.Now()
		result, err := mutexAcquireScript.Run(
			waitCtx,
			m.redisClient,
			[]string{m.key},
			token,
			m.leaseTTL.Milliseconds(),
		).Int64()
		if err == nil {
			if waitCtx.Err() != nil {
				acquireErr := mutexWaitError(ctx, waitCtx)
				return time.Time{}, m.failAcquire(requestID, token, startedAt, acquireErr)
			}
			switch result {
			case 1:
				if !leaseConfirmationIsSafe(time.Now(), attemptStarted, m.leaseTTL) {
					lastErr = errLeaseConfirmationTooOld
					continue
				}
				m.emitAcquireSuccess(requestID, startedAt)
				return attemptStarted, nil
			case 0:
				// Another token currently owns the mutex.
				lastErr = nil
			default:
				acquireErr := fmt.Errorf("%w: unexpected acquire result %d", errInvalidMutexScriptResponse, result)
				return time.Time{}, m.failAcquire(requestID, token, startedAt, acquireErr)
			}
		} else {
			lastErr = fmt.Errorf("acquire mutex: %w", err)
			m.emitRedisError(requestID, lastErr)
			if !isRetryableRedisOperationError(err) {
				return time.Time{}, m.failAcquire(requestID, token, startedAt, lastErr)
			}
		}

		if waitCtx.Err() != nil {
			acquireErr := mutexWaitError(ctx, waitCtx)
			if lastErr != nil {
				acquireErr = errors.Join(acquireErr, lastErr)
			}
			return time.Time{}, m.failAcquire(requestID, token, startedAt, acquireErr)
		}

		timer := time.NewTimer(jitter(backoff))
		select {
		case <-waitCtx.Done():
			stopTimer(timer)
			acquireErr := mutexWaitError(ctx, waitCtx)
			if lastErr != nil {
				acquireErr = errors.Join(acquireErr, lastErr)
			}
			return time.Time{}, m.failAcquire(requestID, token, startedAt, acquireErr)
		case <-timer.C:
		}
		backoff = growBackoff(backoff, m.pollMax)
	}
}

func (m *Mutex) renew(ctx context.Context, token string) error {
	result, err := mutexRenewScript.Run(
		ctx,
		m.redisClient,
		[]string{m.key},
		token,
		m.leaseTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return fmt.Errorf("renew mutex lease: %w", err)
	}
	if result == 0 {
		return ErrLeaseLost
	}
	if result == 1 {
		return nil
	}
	return fmt.Errorf("%w: unexpected renew result %d", errInvalidMutexScriptResponse, result)
}

// release is token-safe. The bool reports whether this call matched and
// removed the caller's token; retryTokenCleanup gives a later missing result
// idempotent meaning only after an ambiguous earlier attempt.
func (m *Mutex) release(ctx context.Context, token string) (bool, error) {
	result, err := mutexReleaseScript.Run(ctx, m.redisClient, []string{m.key}, token).Int64()
	if err != nil {
		return false, fmt.Errorf("release mutex lease: %w", err)
	}
	switch result {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("%w: unexpected release result %d", errInvalidMutexScriptResponse, result)
	}
}

func (m *Mutex) failAcquire(requestID, token string, startedAt time.Time, acquireErr error) error {
	cleanupErr := retryTokenCleanup(
		context.Background(),
		m.cleanupTimeout,
		m.pollInitial,
		m.pollMax,
		true,
		func(cleanupCtx context.Context) (bool, error) {
			matched, err := m.release(cleanupCtx, token)
			if err != nil {
				m.emitRedisError(requestID, err)
			}
			return matched, err
		},
	)

	if cleanupErr != nil {
		acquireErr = errors.Join(acquireErr, fmt.Errorf("clean up mutex acquisition: %w", cleanupErr))
	}
	emitEvent(context.Background(), m.observer, Event{
		Type:      EventCleanup,
		Resource:  "mutex",
		Namespace: m.namespace,
		RequestID: requestID,
		Err:       cleanupErr,
	})
	emitEvent(context.Background(), m.observer, Event{
		Type:         EventAcquireFailure,
		Resource:     "mutex",
		Namespace:    m.namespace,
		RequestID:    requestID,
		WaitDuration: time.Since(startedAt),
		Err:          acquireErr,
	})
	return acquireErr
}

func (m *Mutex) emitAcquireSuccess(requestID string, startedAt time.Time) {
	emitEvent(context.Background(), m.observer, Event{
		Type:         EventAcquireSuccess,
		Resource:     "mutex",
		Namespace:    m.namespace,
		RequestID:    requestID,
		WaitDuration: time.Since(startedAt),
	})
}

func (m *Mutex) emitRedisError(requestID string, err error) {
	emitEvent(context.Background(), m.observer, Event{
		Type:      EventRedisError,
		Resource:  "mutex",
		Namespace: m.namespace,
		RequestID: requestID,
		Err:       err,
	})
}

func applyMutexDefaults(config MutexConfig) MutexConfig {
	if config.AcquireTimeout == 0 {
		config.AcquireTimeout = defaultMutexAcquireTimeout
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaultMutexLeaseTTL
	}
	if config.PollInitial == 0 {
		config.PollInitial = defaultMutexPollInitial
	}
	if config.PollMax == 0 {
		config.PollMax = defaultMutexPollMax
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultMutexCleanupTimeout
	}
	return config
}

func validateMutexConfig(redisClient redis.UniversalClient, config MutexConfig) error {
	if nilRedisClient(redisClient) {
		return invalidConfig("Redis client must not be nil")
	}
	if strings.TrimSpace(config.Namespace) == "" {
		return invalidConfig("namespace must not be empty")
	}

	durations := []struct {
		name  string
		value time.Duration
	}{
		{"acquire timeout", config.AcquireTimeout},
		{"lease TTL", config.LeaseTTL},
		{"initial poll interval", config.PollInitial},
		{"maximum poll interval", config.PollMax},
		{"cleanup timeout", config.CleanupTimeout},
	}
	for _, duration := range durations {
		if duration.value < time.Millisecond {
			return invalidConfig("%s must be at least 1ms", duration.name)
		}
		if duration.value%time.Millisecond != 0 {
			return invalidConfig("%s must be aligned to whole milliseconds", duration.name)
		}
	}
	if config.PollInitial > config.PollMax {
		return invalidConfig("initial poll interval must not exceed maximum poll interval")
	}
	if config.CleanupTimeout > config.LeaseTTL/3 {
		return invalidConfig("lease TTL must be at least three times the cleanup timeout")
	}
	return nil
}

func nilRedisClient(redisClient redis.UniversalClient) bool {
	if redisClient == nil {
		return true
	}
	value := reflect.ValueOf(redisClient)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func newMutexToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func mutexWaitError(parentCtx, waitCtx context.Context) error {
	if err := parentCtx.Err(); err != nil {
		return err
	}
	if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
		return ErrAcquireTimeout
	}
	return waitCtx.Err()
}

func isRedisServerError(err error) bool {
	var serverErr redis.Error
	return errors.As(err, &serverErr)
}

// go-redis retries most of these internally, but a failover or a long-running
// script can outlast its configured retry budget. Keep polling within the
// mutex's acquisition deadline instead of turning a transient Redis state into
// an immediate acquisition failure. Other Redis replies (for example WRONGTYPE
// and authentication errors) are deterministic and fail fast.
func isRetryableRedisServerError(err error) bool {
	for _, prefix := range []string{
		"ASK",
		"BUSY",
		"CLUSTERDOWN",
		"LOADING",
		"MASTERDOWN",
		"MOVED",
		"NOREPLICAS",
		"NOSCRIPT",
		"READONLY",
		"TRYAGAIN",
	} {
		if redis.HasErrorPrefix(err, prefix) {
			return true
		}
	}
	return strings.Contains(err.Error(), "max number of clients reached")
}

func isRetryableRedisOperationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.Nil) ||
		errors.Is(err, errInvalidMutexScriptResponse) ||
		errors.Is(err, errInvalidSemaphoreScriptResponse) {
		return false
	}
	if isRedisServerError(err) {
		return isRetryableRedisServerError(err)
	}
	// Non-server errors are normally transport, pool, or per-attempt context
	// failures. Their commit outcome is uncertain, so retry with the same token.
	return true
}
