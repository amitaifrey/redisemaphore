package redisemaphore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultAcquireTimeout = 30 * time.Second
	defaultPermitTTL      = 90 * time.Second
	defaultWaiterTTL      = 10 * time.Second
	defaultPollInitial    = 100 * time.Millisecond
	defaultPollMax        = time.Second
	defaultCleanupTimeout = 2 * time.Second
	semaphoreSchema       = 1
	cleanupBatchSize      = 128
)

var errInvalidSemaphoreScriptResponse = errors.New("redisemaphore: invalid semaphore script response")

var (
	tryAcquireRedisScript       = redis.NewScript(tryAcquireScript)
	renewSemaphoreRedisScript   = redis.NewScript(renewSemaphoreScript)
	releaseSemaphoreRedisScript = redis.NewScript(releaseSemaphoreScript)
	cancelSemaphoreRedisScript  = redis.NewScript(cancelSemaphoreScript)
	snapshotLeasesRedisScript   = redis.NewScript(snapshotLeasesScript)
	snapshotWaitersRedisScript  = redis.NewScript(snapshotWaitersScript)
	snapshotMetadataRedisScript = redis.NewScript(snapshotMetadataScript)
)

// SemaphoreConfig configures a renewable, priority-aware distributed
// semaphore. Capacity, queue order, and lease durations are immutable for a
// namespace once the namespace has been initialized in Redis.
type SemaphoreConfig struct {
	Namespace        string
	Capacity         int
	QueuesByPriority []string
	// AcquireTimeout bounds polling, but the Redis client's I/O timeout can
	// extend wall-clock return while one admission command is in flight.
	AcquireTimeout time.Duration
	PermitTTL      time.Duration
	WaiterTTL      time.Duration
	PollInitial    time.Duration
	PollMax        time.Duration
	CleanupTimeout time.Duration
	Logger         Logger
}

// AcquireRequest identifies one attempt to acquire a semaphore permit.
// RequestID must be unique among active and waiting attempts in the namespace.
type AcquireRequest struct {
	Queue     string
	RequestID string
}

// Semaphore is a renewable distributed semaphore.
type Semaphore struct {
	redis       redis.UniversalClient
	config      SemaphoreConfig
	keys        semaphoreKeys
	fingerprint string
	queueIndex  map[string]int
}

type semaphoreFingerprint struct {
	Schema           int      `json:"schema"`
	Capacity         int      `json:"capacity"`
	QueuesByPriority []string `json:"queues_by_priority"`
	PermitTTLMillis  int64    `json:"permit_ttl_ms"`
	WaiterTTLMillis  int64    `json:"waiter_ttl_ms"`
}

// NewSemaphore validates and initializes a semaphore namespace. A namespace
// cannot subsequently be opened with a different capacity, queue order, or
// lease duration.
func NewSemaphore(client redis.UniversalClient, config SemaphoreConfig) (*Semaphore, error) {
	config = withSemaphoreDefaults(config)
	if err := validateSemaphoreConfig(client, config); err != nil {
		return nil, err
	}

	checkCtx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout)
	defer cancel()

	fingerprint, err := json.Marshal(semaphoreFingerprint{
		Schema:           semaphoreSchema,
		Capacity:         config.Capacity,
		QueuesByPriority: config.QueuesByPriority,
		PermitTTLMillis:  config.PermitTTL.Milliseconds(),
		WaiterTTLMillis:  config.WaiterTTL.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal semaphore configuration: %w", err)
	}

	keys := newSemaphoreKeys(config.Namespace, config.QueuesByPriority)
	storedValue, err := client.Eval(checkCtx, initializeSemaphoreScript, []string{keys.config}, string(fingerprint)).Result()
	if err != nil {
		return nil, fmt.Errorf("initialize semaphore configuration: %w", err)
	}
	stored, ok := storedValue.(string)
	if !ok {
		if storedBytes, bytesOK := storedValue.([]byte); bytesOK {
			stored = string(storedBytes)
		} else {
			return nil, fmt.Errorf("unexpected semaphore configuration result %T", storedValue)
		}
	}
	if stored != string(fingerprint) {
		return nil, invalidConfig("namespace %q is already configured differently", config.Namespace)
	}

	queueIndex := make(map[string]int, len(config.QueuesByPriority))
	for index, queue := range config.QueuesByPriority {
		queueIndex[queue] = index + 1 // Lua queue indexes are one-based.
	}
	config.Logger = newNonBlockingLogger(config.Logger)

	return &Semaphore{
		redis:       client,
		config:      config,
		keys:        keys,
		fingerprint: string(fingerprint),
		queueIndex:  queueIndex,
	}, nil
}

// Acquire returns a renewable permit. Callers must pass permit.Context() to
// protected work and call permit.Release() after that work has stopped.
func (s *Semaphore) Acquire(ctx context.Context, request AcquireRequest) (*Permit, error) {
	if ctx == nil {
		return nil, invalidConfig("context must not be nil")
	}
	queueIndex, ok := s.queueIndex[request.Queue]
	if !ok {
		return nil, invalidConfig("queue %q is not configured", request.Queue)
	}
	if strings.TrimSpace(request.RequestID) == "" {
		return nil, invalidConfig("request ID must not be empty")
	}

	token, err := randomSemaphoreToken()
	if err != nil {
		return nil, fmt.Errorf("generate semaphore ownership token: %w", err)
	}

	started := time.Now()
	logEvent(ctx, s.config.Logger, slog.LevelDebug, logEventAcquireStart,
		slog.String(logAttrResource, "semaphore"),
		slog.String(logAttrNamespace, s.config.Namespace),
		slog.String(logAttrQueue, request.Queue),
		slog.String(logAttrRequestID, request.RequestID),
	)

	result, err := s.acquire(ctx, request, queueIndex, token)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cleanupErr := s.cancelWithTimeout(token, request.RequestID)
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("cancel semaphore attempt: %w", cleanupErr))
		}
		cleanupLevel := slog.LevelDebug
		if cleanupErr != nil {
			cleanupLevel = slog.LevelError
		}
		logEvent(context.Background(), s.config.Logger, cleanupLevel, logEventCleanup,
			slog.String(logAttrResource, "semaphore"),
			slog.String(logAttrNamespace, s.config.Namespace),
			slog.String(logAttrQueue, request.Queue),
			slog.String(logAttrRequestID, request.RequestID),
			slog.Duration(logAttrWaitDuration, time.Since(started)),
			slog.Any(logAttrError, cleanupErr),
		)
		logEvent(context.Background(), s.config.Logger, slog.LevelWarn, logEventAcquireFailure,
			slog.String(logAttrResource, "semaphore"),
			slog.String(logAttrNamespace, s.config.Namespace),
			slog.String(logAttrQueue, request.Queue),
			slog.String(logAttrRequestID, request.RequestID),
			slog.Duration(logAttrWaitDuration, time.Since(started)),
			slog.Any(logAttrError, err),
		)
		return nil, err
	}

	permit, err := newPermit(ctx, permitOptions{
		resource:       "semaphore",
		namespace:      s.config.Namespace,
		queue:          request.Queue,
		requestID:      request.RequestID,
		ttl:            s.config.PermitTTL,
		pollInitial:    s.config.PollInitial,
		pollMax:        s.config.PollMax,
		cleanupTimeout: s.config.CleanupTimeout,
		logger:         s.config.Logger,
		confirmedAt:    result.confirmedAt,
		renew: func(renewCtx context.Context) error {
			return s.renew(renewCtx, token, request.RequestID)
		},
		release: func(releaseCtx context.Context) (bool, error) {
			return s.release(releaseCtx, token, request.RequestID)
		},
	})
	if err != nil {
		logEvent(context.Background(), s.config.Logger, slog.LevelWarn, logEventAcquireFailure,
			slog.String(logAttrResource, "semaphore"),
			slog.String(logAttrNamespace, s.config.Namespace),
			slog.String(logAttrQueue, request.Queue),
			slog.String(logAttrRequestID, request.RequestID),
			slog.Duration(logAttrWaitDuration, time.Since(started)),
			slog.Any(logAttrError, err),
		)
		return nil, err
	}

	logEvent(ctx, s.config.Logger, slog.LevelDebug, logEventAcquireSuccess,
		slog.String(logAttrResource, "semaphore"),
		slog.String(logAttrNamespace, s.config.Namespace),
		slog.String(logAttrQueue, request.Queue),
		slog.String(logAttrRequestID, request.RequestID),
		slog.Duration(logAttrWaitDuration, time.Since(started)),
		slog.Int64(logAttrHolders, result.holders),
		slog.Int64(logAttrWaiters, result.waiters),
		slog.Int64(logAttrPrunedHolders, result.prunedHolders),
		slog.Int64(logAttrPrunedWaiters, result.prunedWaiters),
	)

	return permit, nil
}

type acquireScriptResult struct {
	confirmedAt   time.Time
	holders       int64
	waiters       int64
	prunedHolders int64
	prunedWaiters int64
}

func (s *Semaphore) acquire(ctx context.Context, request AcquireRequest, queueIndex int, token string) (acquireScriptResult, error) {
	acquireCtx, cancel := context.WithTimeout(ctx, s.config.AcquireTimeout)
	defer cancel()

	backoff := s.config.PollInitial
	var lastErr error
	var heartbeatDue time.Time
	for {
		result, status, err := s.tryAcquire(acquireCtx, request, queueIndex, token)
		if err == nil {
			lastErr = nil
			if acquireCtx.Err() != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return acquireScriptResult{}, ctxErr
				}
				return acquireScriptResult{}, ErrAcquireTimeout
			}
			if result.prunedHolders != 0 || result.prunedWaiters != 0 {
				logEvent(context.Background(), s.config.Logger, slog.LevelDebug, logEventPruned,
					slog.String(logAttrResource, "semaphore"),
					slog.String(logAttrNamespace, s.config.Namespace),
					slog.String(logAttrQueue, request.Queue),
					slog.String(logAttrRequestID, request.RequestID),
					slog.Int64(logAttrHolders, result.holders),
					slog.Int64(logAttrWaiters, result.waiters),
					slog.Int64(logAttrPrunedHolders, result.prunedHolders),
					slog.Int64(logAttrPrunedWaiters, result.prunedWaiters),
				)
			}
			switch status {
			case tryAcquireGranted:
				return result, nil
			case tryAcquireWaiting:
				// Keep the token's original FIFO position and heartbeat it
				// after the randomized backoff below.
				heartbeatDue = result.confirmedAt.Add(s.config.WaiterTTL / 3)
			case tryAcquireDuplicate:
				return acquireScriptResult{}, ErrDuplicateRequest
			case tryAcquireTokenConflict:
				return acquireScriptResult{}, fmt.Errorf("semaphore ownership token conflict")
			case tryAcquireConfigMismatch:
				return acquireScriptResult{}, invalidConfig("persisted configuration for namespace %q changed", s.config.Namespace)
			default:
				return acquireScriptResult{}, fmt.Errorf("%w: unknown try-acquire status %d", errInvalidSemaphoreScriptResponse, status)
			}
		} else {
			if heartbeatDue.IsZero() && !result.confirmedAt.IsZero() {
				// The script may have registered or refreshed this waiter even
				// though its reply was lost. Retry by the conservative heartbeat
				// deadline until a definitive response establishes a newer one.
				heartbeatDue = result.confirmedAt.Add(s.config.WaiterTTL / 3)
			}
			logEvent(context.Background(), s.config.Logger, slog.LevelWarn, logEventRedisError,
				slog.String(logAttrResource, "semaphore"),
				slog.String(logAttrNamespace, s.config.Namespace),
				slog.String(logAttrQueue, request.Queue),
				slog.String(logAttrRequestID, request.RequestID),
				slog.Any(logAttrError, err),
			)
			if isDeterministicSemaphoreError(err) {
				return acquireScriptResult{}, err
			}
			// The script may have committed before a response was lost. Retrying
			// with the same token is therefore required for correctness.
			lastErr = err
		}

		delay := jitter(backoff)
		if !heartbeatDue.IsZero() {
			// A reply that arrives after the heartbeat deadline needs one
			// immediate retry so the waiter can still refresh before its TTL.
			// Clear the deadline after scheduling it: if that immediate retry
			// also fails, its conservative command-start time establishes a new
			// future deadline instead of pinning every outage retry to zero.
			untilHeartbeat := time.Until(heartbeatDue)
			if untilHeartbeat <= 0 {
				delay = 0
			} else if delay > untilHeartbeat {
				delay = untilHeartbeat
			}
			heartbeatDue = time.Time{}
		}
		timer := time.NewTimer(delay)
		select {
		case <-acquireCtx.Done():
			stopTimer(timer)
			terminalErr := ctx.Err()
			if terminalErr == nil {
				terminalErr = ErrAcquireTimeout
			}
			if lastErr != nil {
				terminalErr = errors.Join(terminalErr, lastErr)
			}
			return acquireScriptResult{}, terminalErr
		case <-timer.C:
		}
		backoff = growBackoff(backoff, s.config.PollMax)
	}
}

const (
	tryAcquireWaiting        int64 = 0
	tryAcquireGranted        int64 = 1
	tryAcquireDuplicate      int64 = -1
	tryAcquireTokenConflict  int64 = -2
	tryAcquireConfigMismatch int64 = -3
)

func (s *Semaphore) tryAcquire(ctx context.Context, request AcquireRequest, queueIndex int, token string) (acquireScriptResult, int64, error) {
	keys := make([]string, 0, 8+len(s.keys.queues))
	keys = append(keys,
		s.keys.config,
		s.keys.holders,
		s.keys.waiters,
		s.keys.waiterStarted,
		s.keys.tokenQueue,
		s.keys.tokenRequest,
		s.keys.activeRequest,
		s.keys.sequence,
	)
	keys = append(keys, s.keys.queues...)

	confirmedAt := time.Now()
	value, err := tryAcquireRedisScript.Run(ctx, s.redis, keys,
		s.fingerprint,
		token,
		request.RequestID,
		queueIndex,
		s.config.Capacity,
		s.config.WaiterTTL.Milliseconds(),
		s.config.PermitTTL.Milliseconds(),
		cleanupBatchSize,
	).Result()
	if err != nil {
		return acquireScriptResult{confirmedAt: confirmedAt}, 0, err
	}
	values, ok := value.([]interface{})
	if !ok || len(values) != 5 {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: unexpected try-acquire result %T: %v", errInvalidSemaphoreScriptResponse, value, value)
	}
	status, err := scriptInt(values[0])
	if err != nil {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: %v", errInvalidSemaphoreScriptResponse, err)
	}
	holders, err := scriptInt(values[1])
	if err != nil {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: %v", errInvalidSemaphoreScriptResponse, err)
	}
	waiters, err := scriptInt(values[2])
	if err != nil {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: %v", errInvalidSemaphoreScriptResponse, err)
	}
	prunedHolders, err := scriptInt(values[3])
	if err != nil {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: %v", errInvalidSemaphoreScriptResponse, err)
	}
	prunedWaiters, err := scriptInt(values[4])
	if err != nil {
		return acquireScriptResult{}, 0, fmt.Errorf("%w: %v", errInvalidSemaphoreScriptResponse, err)
	}
	return acquireScriptResult{
		confirmedAt:   confirmedAt,
		holders:       holders,
		waiters:       waiters,
		prunedHolders: prunedHolders,
		prunedWaiters: prunedWaiters,
	}, status, nil
}

func (s *Semaphore) renew(ctx context.Context, token, requestID string) error {
	value, err := renewSemaphoreRedisScript.Run(ctx, s.redis, []string{
		s.keys.holders,
		s.keys.tokenRequest,
		s.keys.activeRequest,
	}, token, requestID, s.config.PermitTTL.Milliseconds()).Result()
	if err != nil {
		return err
	}
	owned, err := scriptInt(value)
	if err != nil {
		return fmt.Errorf("%w: renew semaphore: %v", errInvalidSemaphoreScriptResponse, err)
	}
	if owned != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Semaphore) release(ctx context.Context, token, requestID string) (bool, error) {
	keys := s.cleanupKeys()
	value, err := releaseSemaphoreRedisScript.Run(ctx, s.redis, keys, token, requestID).Result()
	if err != nil {
		return false, fmt.Errorf("release semaphore token: %w", err)
	}
	matched, err := scriptInt(value)
	if err != nil {
		return false, fmt.Errorf("%w: release semaphore token: %v", errInvalidSemaphoreScriptResponse, err)
	}
	return matched == 1, nil
}

func (s *Semaphore) cancel(ctx context.Context, token, requestID string) (bool, error) {
	keys := s.cleanupKeys()
	value, err := cancelSemaphoreRedisScript.Run(ctx, s.redis, keys, token, requestID).Result()
	if err != nil {
		return false, fmt.Errorf("cancel semaphore token: %w", err)
	}
	removed, err := scriptInt(value)
	if err != nil {
		return false, fmt.Errorf("%w: cancel semaphore token: %v", errInvalidSemaphoreScriptResponse, err)
	}
	return removed > 0, nil
}

func isDeterministicSemaphoreError(err error) bool {
	return errors.Is(err, errInvalidSemaphoreScriptResponse) ||
		errors.Is(err, redis.Nil) ||
		errors.Is(err, redis.ErrClosed) ||
		(isRedisServerError(err) && !isRetryableRedisServerError(err))
}

func (s *Semaphore) cancelWithTimeout(token, requestID string) error {
	return retryTokenCleanup(
		context.Background(),
		s.config.CleanupTimeout,
		s.config.PollInitial,
		s.config.PollMax,
		true,
		func(ctx context.Context) (bool, error) {
			return s.cancel(ctx, token, requestID)
		},
	)
}

func (s *Semaphore) cleanupKeys() []string {
	keys := make([]string, 0, 6+len(s.keys.queues))
	keys = append(keys,
		s.keys.holders,
		s.keys.waiters,
		s.keys.waiterStarted,
		s.keys.tokenQueue,
		s.keys.tokenRequest,
		s.keys.activeRequest,
	)
	return append(keys, s.keys.queues...)
}

// Snapshot returns a read-only diagnostic view using one sampled Redis server
// timestamp. It is intentionally a diagnostic multi-call: state can change
// between calls, and total work is proportional to stored waiter records.
// Waiter metadata is requested in scan batches so one call does not assemble
// the entire waiter set in Lua or retain it in client memory.
func (s *Semaphore) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, invalidConfig("context must not be nil")
	}
	value, err := snapshotLeasesRedisScript.Run(ctx, s.redis, []string{
		s.keys.config,
		s.keys.holders,
		s.keys.waiters,
	}).Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("read live semaphore lease counts: %w", err)
	}
	values, ok := value.([]interface{})
	if !ok || len(values) != 3 {
		return Snapshot{}, fmt.Errorf("%w: unexpected semaphore lease snapshot result %T: %v", errInvalidSemaphoreScriptResponse, value, value)
	}
	nowMillis, err := scriptInt(values[0])
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: parse Redis time for semaphore snapshot: %v", errInvalidSemaphoreScriptResponse, err)
	}
	holders, err := scriptInt(values[1])
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: parse semaphore holder count: %v", errInvalidSemaphoreScriptResponse, err)
	}
	waiters, err := scriptInt(values[2])
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter count: %v", errInvalidSemaphoreScriptResponse, err)
	}

	byQueue := make(map[string]int64, len(s.config.QueuesByPriority))
	for _, queue := range s.config.QueuesByPriority {
		byQueue[queue] = 0
	}
	if waiters == 0 {
		return Snapshot{
			Holders:          holders,
			Waiters:          waiters,
			WaitersByQueue:   byQueue,
			CapacityExceeded: holders > int64(s.config.Capacity),
		}, nil
	}

	var oldestStarted int64
	const snapshotBatchSize = 512
	cursor := "0"
	for {
		scanValue, scanErr := snapshotWaitersRedisScript.Run(ctx, s.redis, []string{
			s.keys.config,
			s.keys.waiters,
		}, cursor, snapshotBatchSize).Result()
		if scanErr != nil {
			return Snapshot{}, fmt.Errorf("scan semaphore waiter leases: %w", scanErr)
		}
		scan, scanOK := scanValue.([]interface{})
		if !scanOK || len(scan) != 2 {
			return Snapshot{}, fmt.Errorf("%w: unexpected semaphore waiter scan result %T: %v", errInvalidSemaphoreScriptResponse, scanValue, scanValue)
		}
		nextCursor, cursorErr := scriptString(scan[0])
		if cursorErr != nil {
			return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter scan cursor: %v", errInvalidSemaphoreScriptResponse, cursorErr)
		}
		entries, entriesErr := scriptValues(scan[1])
		if entriesErr != nil {
			return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter scan entries: %v", errInvalidSemaphoreScriptResponse, entriesErr)
		}
		if len(entries)%2 != 0 {
			return Snapshot{}, fmt.Errorf("%w: odd semaphore waiter scan entry count %d", errInvalidSemaphoreScriptResponse, len(entries))
		}

		liveTokens := make([]string, 0, len(entries)/2)
		for index := 0; index < len(entries); index += 2 {
			token, tokenErr := scriptString(entries[index])
			if tokenErr != nil {
				return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter token: %v", errInvalidSemaphoreScriptResponse, tokenErr)
			}
			expiryText, expiryErr := scriptString(entries[index+1])
			if expiryErr != nil {
				return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter expiry: %v", errInvalidSemaphoreScriptResponse, expiryErr)
			}
			expiry, expiryErr := strconv.ParseFloat(expiryText, 64)
			if expiryErr != nil {
				return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter expiry %q: %v", errInvalidSemaphoreScriptResponse, expiryText, expiryErr)
			}
			if expiry > float64(nowMillis) {
				liveTokens = append(liveTokens, token)
			}
		}

		for offset := 0; offset < len(liveTokens); offset += snapshotBatchSize {
			end := min(offset+snapshotBatchSize, len(liveTokens))
			metadataTokens := liveTokens[offset:end]
			arguments := make([]interface{}, len(metadataTokens))
			for index, token := range metadataTokens {
				arguments[index] = token
			}
			metadataValue, metadataErr := snapshotMetadataRedisScript.Run(ctx, s.redis, []string{
				s.keys.config,
				s.keys.tokenQueue,
				s.keys.waiterStarted,
			}, arguments...).Result()
			if metadataErr != nil {
				return Snapshot{}, fmt.Errorf("read semaphore waiter metadata: %w", metadataErr)
			}
			metadata, metadataOK := metadataValue.([]interface{})
			if !metadataOK || len(metadata) != 2 {
				return Snapshot{}, fmt.Errorf("%w: unexpected semaphore metadata snapshot result %T: %v", errInvalidSemaphoreScriptResponse, metadataValue, metadataValue)
			}
			queueValues, queueErr := scriptValues(metadata[0])
			if queueErr != nil {
				return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter queues: %v", errInvalidSemaphoreScriptResponse, queueErr)
			}
			startedValues, startedErr := scriptValues(metadata[1])
			if startedErr != nil {
				return Snapshot{}, fmt.Errorf("%w: parse semaphore waiter start times: %v", errInvalidSemaphoreScriptResponse, startedErr)
			}
			for index := range metadataTokens {
				if index < len(queueValues) && queueValues[index] != nil {
					queueIndex, parseErr := scriptInt(queueValues[index])
					if parseErr == nil && queueIndex >= 1 && queueIndex <= int64(len(s.config.QueuesByPriority)) {
						byQueue[s.config.QueuesByPriority[queueIndex-1]]++
					}
				}
				if index < len(startedValues) && startedValues[index] != nil {
					started, parseErr := scriptInt(startedValues[index])
					if parseErr == nil && started <= nowMillis && (oldestStarted == 0 || started < oldestStarted) {
						oldestStarted = started
					}
				}
			}
		}

		cursor = nextCursor
		if cursor == "0" {
			break
		}
	}

	var oldestWait time.Duration
	if oldestStarted != 0 {
		oldestWait = time.Duration(nowMillis-oldestStarted) * time.Millisecond
	}
	return Snapshot{
		Holders:          holders,
		Waiters:          waiters,
		WaitersByQueue:   byQueue,
		OldestWait:       oldestWait,
		CapacityExceeded: holders > int64(s.config.Capacity),
	}, nil
}

func withSemaphoreDefaults(config SemaphoreConfig) SemaphoreConfig {
	config.QueuesByPriority = append([]string(nil), config.QueuesByPriority...)
	if config.AcquireTimeout == 0 {
		config.AcquireTimeout = defaultAcquireTimeout
	}
	if config.PermitTTL == 0 {
		config.PermitTTL = defaultPermitTTL
	}
	if config.WaiterTTL == 0 {
		config.WaiterTTL = defaultWaiterTTL
	}
	if config.PollInitial == 0 {
		config.PollInitial = defaultPollInitial
	}
	if config.PollMax == 0 {
		config.PollMax = defaultPollMax
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultCleanupTimeout
	}
	return config
}

func validateSemaphoreConfig(client redis.UniversalClient, config SemaphoreConfig) error {
	if nilRedisClient(client) {
		return invalidConfig("Redis client must not be nil")
	}
	if strings.TrimSpace(config.Namespace) == "" {
		return invalidConfig("namespace must not be empty")
	}
	if config.Capacity <= 0 {
		return invalidConfig("capacity must be positive")
	}
	const maximumExactLuaInteger = uint64(1<<53 - 1)
	if uint64(config.Capacity) > maximumExactLuaInteger {
		return invalidConfig("capacity must not exceed %d", maximumExactLuaInteger)
	}
	if len(config.QueuesByPriority) == 0 || len(config.QueuesByPriority) > 64 {
		return invalidConfig("queues by priority must contain between 1 and 64 queues")
	}
	seen := make(map[string]struct{}, len(config.QueuesByPriority))
	for _, queue := range config.QueuesByPriority {
		if strings.TrimSpace(queue) == "" {
			return invalidConfig("queue names must not be empty")
		}
		if _, exists := seen[queue]; exists {
			return invalidConfig("queue %q is configured more than once", queue)
		}
		seen[queue] = struct{}{}
	}
	if config.AcquireTimeout < time.Millisecond {
		return invalidConfig("acquire timeout must be at least one millisecond")
	}
	if config.PermitTTL < time.Millisecond {
		return invalidConfig("permit TTL must be at least one millisecond")
	}
	if config.WaiterTTL < time.Millisecond {
		return invalidConfig("waiter TTL must be at least one millisecond")
	}
	if config.PollInitial < time.Millisecond {
		return invalidConfig("initial poll interval must be at least one millisecond")
	}
	if config.PollMax < time.Millisecond {
		return invalidConfig("maximum poll interval must be at least one millisecond")
	}
	if config.CleanupTimeout < time.Millisecond {
		return invalidConfig("cleanup timeout must be at least one millisecond")
	}
	for _, duration := range []struct {
		name  string
		value time.Duration
	}{
		{name: "acquire timeout", value: config.AcquireTimeout},
		{name: "permit TTL", value: config.PermitTTL},
		{name: "waiter TTL", value: config.WaiterTTL},
		{name: "initial poll interval", value: config.PollInitial},
		{name: "maximum poll interval", value: config.PollMax},
		{name: "cleanup timeout", value: config.CleanupTimeout},
	} {
		if duration.value%time.Millisecond != 0 {
			return invalidConfig("%s must be aligned to whole milliseconds", duration.name)
		}
	}
	if config.PollInitial > config.PollMax {
		return invalidConfig("initial poll interval must not exceed maximum poll interval")
	}
	if config.CleanupTimeout > config.PermitTTL/3 {
		return invalidConfig("permit TTL must be at least three times cleanup timeout")
	}
	if config.WaiterTTL <= config.PollMax {
		return invalidConfig("waiter TTL must exceed maximum poll interval")
	}
	return nil
}

func randomSemaphoreToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func scriptInt(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse Lua integer %q: %w", value, err)
		}
		return parsed, nil
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse Lua integer %q: %w", value, err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unexpected Lua integer %T: %v", value, value)
	}
}

func scriptValues(value interface{}) ([]interface{}, error) {
	switch value := value.(type) {
	case []interface{}:
		return value, nil
	case []string:
		values := make([]interface{}, len(value))
		for index := range value {
			values[index] = value[index]
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected Lua array %T: %v", value, value)
	}
}

func scriptString(value interface{}) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected Lua string %T: %v", value, value)
	}
}

// Initialization is a single primary-routed write script. This avoids a
// SETNX/GET split read when a Cluster client is configured to read replicas.
const initializeSemaphoreScript = `
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return ARGV[1]
end
return current
`

// Snapshot uses write-classified EVAL commands with an explicit same-slot key
// so Cluster clients route every diagnostic read to the slot's primary even
// when replica reads are enabled.
const snapshotLeasesScript = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local minimum = '(' .. now
local holders = redis.call('ZCOUNT', KEYS[2], minimum, '+inf')
local waiters = redis.call('ZCOUNT', KEYS[3], minimum, '+inf')
return {now, holders, waiters}
`

const snapshotWaitersScript = `
return redis.call('ZSCAN', KEYS[2], ARGV[1], 'COUNT', ARGV[2])
`

const snapshotMetadataScript = `
return {
  redis.call('HMGET', KEYS[2], unpack(ARGV)),
  redis.call('HMGET', KEYS[3], unpack(ARGV))
}
`

const tryAcquireScript = `
local configKey = KEYS[1]
local holdersKey = KEYS[2]
local waitersKey = KEYS[3]
local waiterStartedKey = KEYS[4]
local tokenQueueKey = KEYS[5]
local tokenRequestKey = KEYS[6]
local activeRequestKey = KEYS[7]
local sequenceKey = KEYS[8]
local firstQueueKey = 9
local queueCount = #KEYS - 8

local expectedConfig = ARGV[1]
local token = ARGV[2]
local requestID = ARGV[3]
local requestedQueue = tonumber(ARGV[4])
local capacity = tonumber(ARGV[5])
local waiterTTL = tonumber(ARGV[6])
local permitTTL = tonumber(ARGV[7])
local cleanupBatch = tonumber(ARGV[8])

if redis.call('GET', configKey) ~= expectedConfig then
  return {-3, 0, 0, 0, 0}
end

local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local prunedHolders = 0
local prunedWaiters = 0
local cleaned = 0

local function liveCount(key)
  return redis.call('ZCOUNT', key, '(' .. now, '+inf')
end

local function removeToken(candidate)
  local removedHolder = redis.call('ZREM', holdersKey, candidate)
  local removedWaiter = redis.call('ZREM', waitersKey, candidate)
  local queueIndex = tonumber(redis.call('HGET', tokenQueueKey, candidate))
  if queueIndex and queueIndex >= 1 and queueIndex <= queueCount then
    redis.call('ZREM', KEYS[firstQueueKey + queueIndex - 1], candidate)
  end
  local candidateRequest = redis.call('HGET', tokenRequestKey, candidate)
  if candidateRequest and redis.call('HGET', activeRequestKey, candidateRequest) == candidate then
    redis.call('HDEL', activeRequestKey, candidateRequest)
  end
  redis.call('HDEL', waiterStartedKey, candidate)
  redis.call('HDEL', tokenQueueKey, candidate)
  redis.call('HDEL', tokenRequestKey, candidate)
  return removedHolder, removedWaiter
end

local expiredHolders = redis.call('ZRANGEBYSCORE', holdersKey, '-inf', now, 'LIMIT', 0, cleanupBatch)
for _, candidate in ipairs(expiredHolders) do
  local removedHolder, removedWaiter = removeToken(candidate)
  prunedHolders = prunedHolders + removedHolder
  prunedWaiters = prunedWaiters + removedWaiter
  cleaned = cleaned + 1
end

if cleaned < cleanupBatch then
  local expiredWaiters = redis.call('ZRANGEBYSCORE', waitersKey, '-inf', now, 'LIMIT', 0, cleanupBatch - cleaned)
  for _, candidate in ipairs(expiredWaiters) do
    local removedHolder, removedWaiter = removeToken(candidate)
    prunedHolders = prunedHolders + removedHolder
    prunedWaiters = prunedWaiters + removedWaiter
    cleaned = cleaned + 1
  end
end

local mappedToken = redis.call('HGET', activeRequestKey, requestID)
if mappedToken and mappedToken ~= token then
  local holderExpiry = tonumber(redis.call('ZSCORE', holdersKey, mappedToken))
  local waiterExpiry = tonumber(redis.call('ZSCORE', waitersKey, mappedToken))
  if (holderExpiry and holderExpiry > now) or (waiterExpiry and waiterExpiry > now) then
    return {-1, liveCount(holdersKey), liveCount(waitersKey), prunedHolders, prunedWaiters}
  end
  local removedHolder, removedWaiter = removeToken(mappedToken)
  prunedHolders = prunedHolders + removedHolder
  prunedWaiters = prunedWaiters + removedWaiter
  mappedToken = false
end

local storedRequest = redis.call('HGET', tokenRequestKey, token)
if storedRequest and storedRequest ~= requestID then
  return {-2, liveCount(holdersKey), liveCount(waitersKey), prunedHolders, prunedWaiters}
end

local holderExpiry = tonumber(redis.call('ZSCORE', holdersKey, token))
if holderExpiry and holderExpiry > now and storedRequest == requestID and mappedToken == token then
  redis.call('ZADD', holdersKey, 'XX', now + permitTTL, token)
  return {1, liveCount(holdersKey), liveCount(waitersKey), prunedHolders, prunedWaiters}
end
if holderExpiry then
  local removedHolder, removedWaiter = removeToken(token)
  prunedHolders = prunedHolders + removedHolder
  prunedWaiters = prunedWaiters + removedWaiter
  storedRequest = false
  mappedToken = false
end

local waiterExpiry = tonumber(redis.call('ZSCORE', waitersKey, token))
local storedQueue = tonumber(redis.call('HGET', tokenQueueKey, token))
if waiterExpiry and waiterExpiry > now and storedRequest == requestID and mappedToken == token and storedQueue == requestedQueue then
  redis.call('ZADD', waitersKey, 'XX', now + waiterTTL, token)
else
  if waiterExpiry or storedRequest or mappedToken == token then
    local removedHolder, removedWaiter = removeToken(token)
    prunedHolders = prunedHolders + removedHolder
    prunedWaiters = prunedWaiters + removedWaiter
  end
  local sequence = redis.call('INCR', sequenceKey)
  redis.call('HSET', activeRequestKey, requestID, token)
  redis.call('HSET', tokenRequestKey, token, requestID)
  redis.call('HSET', tokenQueueKey, token, requestedQueue)
  redis.call('HSET', waiterStartedKey, token, now)
  redis.call('ZADD', waitersKey, now + waiterTTL, token)
  redis.call('ZADD', KEYS[firstQueueKey + requestedQueue - 1], sequence, token)
end

if redis.call('ZCARD', holdersKey) < capacity then
  local selected = false
  local cleanupBlocked = false
  for queueIndex = 1, queueCount do
    local queueKey = KEYS[firstQueueKey + queueIndex - 1]
    while true do
      local head = redis.call('ZRANGE', queueKey, 0, 0)[1]
      if not head then
        break
      end
      local expiry = tonumber(redis.call('ZSCORE', waitersKey, head))
      if expiry and expiry > now then
        selected = head
        break
      end
      if cleaned >= cleanupBatch then
        cleanupBlocked = true
        break
      end
      local removedFromQueue = redis.call('ZREM', queueKey, head)
      local removedHolder, removedWaiter = removeToken(head)
      prunedHolders = prunedHolders + removedHolder
      if removedWaiter == 0 and removedFromQueue > 0 then
        prunedWaiters = prunedWaiters + 1
      else
        prunedWaiters = prunedWaiters + removedWaiter
      end
      cleaned = cleaned + 1
    end
    if selected or cleanupBlocked then
      break
    end
  end

  if selected == token then
    redis.call('ZREM', waitersKey, token)
    redis.call('ZREM', KEYS[firstQueueKey + requestedQueue - 1], token)
    redis.call('HDEL', waiterStartedKey, token)
    redis.call('ZADD', holdersKey, now + permitTTL, token)
    return {1, liveCount(holdersKey), liveCount(waitersKey), prunedHolders, prunedWaiters}
  end
end

return {0, liveCount(holdersKey), liveCount(waitersKey), prunedHolders, prunedWaiters}
`

const renewSemaphoreScript = `
local token = ARGV[1]
local requestID = ARGV[2]
local permitTTL = tonumber(ARGV[3])
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)

if redis.call('HGET', KEYS[2], token) ~= requestID then
  return 0
end
if redis.call('HGET', KEYS[3], requestID) ~= token then
  return 0
end
local expiry = tonumber(redis.call('ZSCORE', KEYS[1], token))
if not expiry or expiry <= now then
  return 0
end
redis.call('ZADD', KEYS[1], 'XX', now + permitTTL, token)
return 1
`

// Cancellation is allowed to remove either a waiter or a holder because an
// admission reply may have been lost. The token/request pair makes that safe.
const cancelSemaphoreScript = `
local token = ARGV[1]
local requestID = ARGV[2]
if redis.call('HGET', KEYS[5], token) ~= requestID then
  return 0
end

local removed = redis.call('ZREM', KEYS[1], token)
removed = removed + redis.call('ZREM', KEYS[2], token)
local queueIndex = tonumber(redis.call('HGET', KEYS[4], token))
local queueCount = #KEYS - 6
if queueIndex and queueIndex >= 1 and queueIndex <= queueCount then
  redis.call('ZREM', KEYS[6 + queueIndex], token)
end
if redis.call('HGET', KEYS[6], requestID) == token then
  redis.call('HDEL', KEYS[6], requestID)
end
redis.call('HDEL', KEYS[3], token)
redis.call('HDEL', KEYS[4], token)
redis.call('HDEL', KEYS[5], token)
return removed
`

// Release deliberately has its own script so its behavior can evolve
// independently from cancellation while retaining the same token checks.
const releaseSemaphoreScript = `
local token = ARGV[1]
local requestID = ARGV[2]
if redis.call('HGET', KEYS[5], token) ~= requestID then
  return 0
end

local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local holderExpiry = tonumber(redis.call('ZSCORE', KEYS[1], token))
local matchedLiveHolder = 0
if holderExpiry and holderExpiry > now and redis.call('HGET', KEYS[6], requestID) == token then
  matchedLiveHolder = 1
end

redis.call('ZREM', KEYS[1], token)
redis.call('ZREM', KEYS[2], token)
local queueIndex = tonumber(redis.call('HGET', KEYS[4], token))
local queueCount = #KEYS - 6
if queueIndex and queueIndex >= 1 and queueIndex <= queueCount then
  redis.call('ZREM', KEYS[6 + queueIndex], token)
end
if redis.call('HGET', KEYS[6], requestID) == token then
  redis.call('HDEL', KEYS[6], requestID)
end
redis.call('HDEL', KEYS[3], token)
redis.call('HDEL', KEYS[4], token)
redis.call('HDEL', KEYS[5], token)
return matchedLiveHolder
`
