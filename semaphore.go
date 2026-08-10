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

var errInvalidSemaphoreState = errors.New("redisemaphore: invalid semaphore state")

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
	semaphore := &Semaphore{
		redis:       client,
		config:      config,
		keys:        keys,
		fingerprint: string(fingerprint),
	}
	if err := semaphore.initializeConfiguration(checkCtx); err != nil {
		return nil, err
	}

	queueIndex := make(map[string]int, len(config.QueuesByPriority))
	for index, queue := range config.QueuesByPriority {
		queueIndex[queue] = index + 1
	}
	config.Logger = newNonBlockingLogger(config.Logger)
	semaphore.config = config
	semaphore.queueIndex = queueIndex
	return semaphore, nil
}

func (s *Semaphore) initializeConfiguration(ctx context.Context) error {
	for {
		_, err := withOperationGate(ctx, s, []string{s.keys.config}, false, func(tx *redis.Tx, _ operationGate) (struct{}, error) {
			stored, getErr := tx.Get(ctx, s.keys.config).Result()
			switch {
			case errors.Is(getErr, redis.Nil):
				return struct{}{}, execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
					pipe.Set(ctx, s.keys.config, s.fingerprint, 0)
				})
			case getErr != nil:
				return struct{}{}, getErr
			case stored != s.fingerprint:
				if err := validateWatchedState(ctx, tx, s.keys.operationGate); err != nil {
					return struct{}{}, err
				}
				return struct{}{}, invalidConfig("namespace %q is already configured differently", s.config.Namespace)
			default:
				return struct{}{}, validateWatchedState(ctx, tx, s.keys.operationGate)
			}
		})
		if err == nil {
			return nil
		}
		if !transactionOutcomeUncertain(err) &&
			!errors.Is(err, errOperationGateBusy) &&
			!errors.Is(err, errOperationGateLost) &&
			!errors.Is(err, redis.TxFailedErr) {
			return fmt.Errorf("initialize semaphore configuration: %w", err)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("initialize semaphore configuration: %w", errors.Join(err, ctx.Err()))
		}
		timer := time.NewTimer(jitter(s.config.PollInitial))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return fmt.Errorf("initialize semaphore configuration: %w", errors.Join(err, ctx.Err()))
		case <-timer.C:
		}
	}
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

type acquireResult struct {
	confirmedAt   time.Time
	holders       int64
	waiters       int64
	prunedHolders int64
	prunedWaiters int64
}

func (s *Semaphore) acquire(ctx context.Context, request AcquireRequest, queueIndex int, token string) (acquireResult, error) {
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
					return acquireResult{}, ctxErr
				}
				return acquireResult{}, ErrAcquireTimeout
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
				return acquireResult{}, ErrDuplicateRequest
			case tryAcquireTokenConflict:
				return acquireResult{}, fmt.Errorf("semaphore ownership token conflict")
			case tryAcquireConfigMismatch:
				return acquireResult{}, invalidConfig("persisted configuration for namespace %q changed", s.config.Namespace)
			default:
				return acquireResult{}, fmt.Errorf("%w: unknown try-acquire status %d", errInvalidSemaphoreState, status)
			}
		} else {
			if heartbeatDue.IsZero() && !result.confirmedAt.IsZero() {
				// The transaction may have registered or refreshed this waiter even
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
				return acquireResult{}, err
			}
			// The transaction may have committed before a response was lost. Retrying
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
			return acquireResult{}, terminalErr
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

type acquireAttemptResult struct {
	result acquireResult
	status int64
}

func (s *Semaphore) tryAcquire(ctx context.Context, request AcquireRequest, queueIndex int, token string) (acquireResult, int64, error) {
	attemptStarted := time.Now()
	attempt, err := withOperationGate(ctx, s, s.stateKeys(), false, func(tx *redis.Tx, gate operationGate) (acquireAttemptResult, error) {
		stored, getErr := tx.Get(ctx, s.keys.config).Result()
		if getErr != nil && !errors.Is(getErr, redis.Nil) {
			return acquireAttemptResult{}, getErr
		}
		if errors.Is(getErr, redis.Nil) || stored != s.fingerprint {
			if err := validateWatchedState(ctx, tx, s.keys.operationGate); err != nil {
				return acquireAttemptResult{}, err
			}
			return acquireAttemptResult{status: tryAcquireConfigMismatch}, nil
		}

		initialNow, timeErr := tx.Time(ctx).Result()
		if timeErr != nil {
			return acquireAttemptResult{}, timeErr
		}
		initialNowMillis := initialNow.UnixMilli()
		maximum := strconv.FormatInt(initialNowMillis, 10)
		var expiredHolderCommand, expiredWaiterCommand *redis.StringSliceCmd
		_, readErr := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			expiredHolderCommand = pipe.ZRangeByScore(ctx, s.keys.holders, &redis.ZRangeBy{
				Min: "-inf", Max: maximum, Offset: 0, Count: cleanupBatchSize,
			})
			expiredWaiterCommand = pipe.ZRangeByScore(ctx, s.keys.waiters, &redis.ZRangeBy{
				Min: "-inf", Max: maximum, Offset: 0, Count: cleanupBatchSize,
			})
			return nil
		})
		if readErr != nil {
			return acquireAttemptResult{}, readErr
		}
		expiredHolders, readErr := expiredHolderCommand.Result()
		if readErr != nil {
			return acquireAttemptResult{}, readErr
		}
		expiredWaiters, readErr := expiredWaiterCommand.Result()
		if readErr != nil {
			return acquireAttemptResult{}, readErr
		}

		mappedToken, mapped, readErr := optionalHGet(ctx, tx, s.keys.activeRequest, request.RequestID)
		if readErr != nil {
			return acquireAttemptResult{}, readErr
		}
		stateTokens := make([]string, 0, len(expiredHolders)+len(expiredWaiters)+2)
		stateTokens = append(stateTokens, expiredHolders...)
		remainingCleanup := cleanupBatchSize - len(expiredHolders)
		if remainingCleanup > 0 {
			if len(expiredWaiters) > remainingCleanup {
				expiredWaiters = expiredWaiters[:remainingCleanup]
			}
			stateTokens = append(stateTokens, expiredWaiters...)
		} else {
			expiredWaiters = nil
		}
		if mapped {
			stateTokens = append(stateTokens, mappedToken)
		}
		stateTokens = append(stateTokens, token)
		states, readErr := loadTokenStates(ctx, tx, s.keys, stateTokens)
		if readErr != nil {
			return acquireAttemptResult{}, readErr
		}

		removals := make(map[string]*tokenRemoval)
		var prunedHolders, prunedWaiters int64
		cleaned := 0
		scheduleRemoval := func(state semaphoreTokenState, extraQueue int) {
			if _, exists := removals[state.token]; !exists {
				if state.hasHolder {
					prunedHolders++
				}
				if state.hasWaiter {
					prunedWaiters++
				}
			}
			addTokenRemoval(removals, state, extraQueue)
		}
		for _, candidate := range expiredHolders {
			scheduleRemoval(states[candidate], 0)
			cleaned++
		}
		for _, candidate := range expiredWaiters {
			scheduleRemoval(states[candidate], 0)
			cleaned++
		}

		leaseClockStarted := time.Now()
		currentNow, timeErr := tx.Time(ctx).Result()
		if timeErr != nil {
			return acquireAttemptResult{}, timeErr
		}
		remainingGate, ttlErr := tx.PTTL(ctx, s.keys.operationGate).Result()
		if ttlErr != nil {
			return acquireAttemptResult{}, ttlErr
		}
		if remainingGate <= 0 {
			return acquireAttemptResult{}, errOperationGateLost
		}
		nowMillis := currentNow.UnixMilli()
		safeUntil := nowMillis + remainingGate.Milliseconds()

		commitRemovals := func() error {
			if len(removals) == 0 {
				return validateWatchedState(ctx, tx, s.keys.operationGate)
			}
			return execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
				queueTokenRemovals(ctx, pipe, s.keys, removals)
			})
		}

		if mapped && mappedToken != token {
			mappedState := states[mappedToken]
			if (mappedState.hasHolder && mappedState.holderExpiry > nowMillis) ||
				(mappedState.hasWaiter && mappedState.waiterExpiry > nowMillis) {
				if err := commitRemovals(); err != nil {
					return acquireAttemptResult{}, err
				}
				return acquireAttemptResult{
					result: acquireResult{prunedHolders: prunedHolders, prunedWaiters: prunedWaiters},
					status: tryAcquireDuplicate,
				}, nil
			}
			scheduleRemoval(mappedState, 0)
			mapped = false
		}

		ownState := states[token]
		if ownState.hasRequest && ownState.requestID != request.RequestID {
			if err := commitRemovals(); err != nil {
				return acquireAttemptResult{}, err
			}
			return acquireAttemptResult{
				result: acquireResult{prunedHolders: prunedHolders, prunedWaiters: prunedWaiters},
				status: tryAcquireTokenConflict,
			}, nil
		}

		if ownState.hasHolder && ownState.holderExpiry > nowMillis {
			if ownState.holderExpiry <= safeUntil || !ownState.hasRequest || ownState.requestID != request.RequestID || !ownState.activeMatches {
				if err := commitRemovals(); err != nil {
					return acquireAttemptResult{}, err
				}
				return acquireAttemptResult{}, ErrLeaseLost
			}
			holders, countErr := tx.ZCount(ctx, s.keys.holders, "("+strconv.FormatInt(nowMillis, 10), "+inf").Result()
			if countErr != nil {
				return acquireAttemptResult{}, countErr
			}
			waiters, countErr := tx.ZCount(ctx, s.keys.waiters, "("+strconv.FormatInt(nowMillis, 10), "+inf").Result()
			if countErr != nil {
				return acquireAttemptResult{}, countErr
			}
			err := execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
				queueTokenRemovals(ctx, pipe, s.keys, removals)
				pipe.ZAddArgs(ctx, s.keys.holders, redis.ZAddArgs{
					XX:      true,
					Members: []redis.Z{{Score: float64(nowMillis + s.config.PermitTTL.Milliseconds()), Member: token}},
				})
			})
			if err != nil {
				return acquireAttemptResult{}, err
			}
			return acquireAttemptResult{
				result: acquireResult{confirmedAt: leaseClockStarted, holders: holders, waiters: waiters, prunedHolders: prunedHolders, prunedWaiters: prunedWaiters},
				status: tryAcquireGranted,
			}, nil
		}
		if ownState.hasHolder {
			scheduleRemoval(ownState, 0)
			ownState = semaphoreTokenState{token: token}
			mapped = false
		}

		waiterConsistent := ownState.hasWaiter && ownState.waiterExpiry > safeUntil &&
			ownState.hasRequest && ownState.requestID == request.RequestID &&
			ownState.activeMatches && ownState.hasQueue && ownState.queueIndex == queueIndex && mapped && mappedToken == token
		registerWaiter := !waiterConsistent
		if registerWaiter && (ownState.hasWaiter || ownState.hasRequest || (mapped && mappedToken == token)) {
			scheduleRemoval(ownState, 0)
		}

		holders, countErr := tx.ZCount(ctx, s.keys.holders, "("+strconv.FormatInt(nowMillis, 10), "+inf").Result()
		if countErr != nil {
			return acquireAttemptResult{}, countErr
		}
		waiters, countErr := tx.ZCount(ctx, s.keys.waiters, "("+strconv.FormatInt(nowMillis, 10), "+inf").Result()
		if countErr != nil {
			return acquireAttemptResult{}, countErr
		}
		storedHolders, countErr := tx.ZCard(ctx, s.keys.holders).Result()
		if countErr != nil {
			return acquireAttemptResult{}, countErr
		}
		availableStoredHolders := storedHolders
		for _, removal := range removals {
			if removal.state.hasHolder && availableStoredHolders > 0 {
				availableStoredHolders--
			}
		}

		var selected string
		if availableStoredHolders < int64(s.config.Capacity) {
			queueReadLimit := int64(len(removals) + (cleanupBatchSize - cleaned) + 1)
			queueCandidates, readErr := readQueueCandidates(ctx, tx, s.keys, queueReadLimit)
			if readErr != nil {
				return acquireAttemptResult{}, readErr
			}
			cleanupBlocked := false
			type staleQueueToken struct {
				token      string
				queueIndex int
			}
			staleQueueTokens := make([]staleQueueToken, 0)
			for index, candidates := range queueCandidates {
				for _, candidate := range candidates {
					if _, removed := removals[candidate.token]; removed {
						continue
					}
					if candidate.expiry <= nowMillis {
						if cleaned >= cleanupBatchSize {
							cleanupBlocked = true
							break
						}
						staleQueueTokens = append(staleQueueTokens, staleQueueToken{token: candidate.token, queueIndex: index + 1})
						cleaned++
						continue
					}
					if candidate.expiry <= safeUntil {
						cleanupBlocked = true
						break
					}
					selected = candidate.token
					break
				}
				if selected != "" || cleanupBlocked {
					break
				}
				if registerWaiter && index+1 == queueIndex {
					selected = token
					break
				}
			}
			if len(staleQueueTokens) != 0 {
				tokens := make([]string, len(staleQueueTokens))
				for index := range staleQueueTokens {
					tokens[index] = staleQueueTokens[index].token
				}
				staleStates, stateErr := loadTokenStates(ctx, tx, s.keys, tokens)
				if stateErr != nil {
					return acquireAttemptResult{}, stateErr
				}
				for _, stale := range staleQueueTokens {
					scheduleRemoval(staleStates[stale.token], stale.queueIndex)
					if !staleStates[stale.token].hasWaiter {
						prunedWaiters++
					}
				}
			}
		}

		for _, removal := range removals {
			if removal.state.holderExpiry > nowMillis && holders > 0 {
				holders--
			}
			if removal.state.hasHolder && storedHolders > 0 {
				storedHolders--
			}
			if removal.state.waiterExpiry > nowMillis && waiters > 0 {
				waiters--
			}
		}
		grant := storedHolders < int64(s.config.Capacity) && selected == token
		var nextSequence int64
		if registerWaiter {
			storedSequence, sequenceErr := tx.Get(ctx, s.keys.sequence).Result()
			if errors.Is(sequenceErr, redis.Nil) {
				nextSequence = 1
			} else if sequenceErr != nil {
				return acquireAttemptResult{}, sequenceErr
			} else {
				parsed, parseErr := strconv.ParseInt(storedSequence, 10, 64)
				if parseErr != nil || parsed < 0 || parsed >= 1<<53-1 {
					return acquireAttemptResult{}, fmt.Errorf("%w: invalid queue sequence %q", errInvalidSemaphoreState, storedSequence)
				}
				nextSequence = parsed + 1
			}
		}

		err := execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
			queueTokenRemovals(ctx, pipe, s.keys, removals)
			if registerWaiter {
				pipe.Set(ctx, s.keys.sequence, nextSequence, 0)
				pipe.HSet(ctx, s.keys.activeRequest, request.RequestID, token)
				pipe.HSet(ctx, s.keys.tokenRequest, token, request.RequestID)
				pipe.HSet(ctx, s.keys.tokenQueue, token, queueIndex)
				if !grant {
					pipe.HSet(ctx, s.keys.waiterStarted, token, nowMillis)
					pipe.ZAdd(ctx, s.keys.waiters, redis.Z{Score: float64(nowMillis + s.config.WaiterTTL.Milliseconds()), Member: token})
					pipe.ZAdd(ctx, s.keys.queues[queueIndex-1], redis.Z{Score: float64(nextSequence), Member: token})
				}
			} else if !grant {
				pipe.ZAddArgs(ctx, s.keys.waiters, redis.ZAddArgs{
					XX:      true,
					Members: []redis.Z{{Score: float64(nowMillis + s.config.WaiterTTL.Milliseconds()), Member: token}},
				})
			}
			if grant {
				pipe.ZRem(ctx, s.keys.waiters, token)
				pipe.ZRem(ctx, s.keys.queues[queueIndex-1], token)
				pipe.HDel(ctx, s.keys.waiterStarted, token)
				pipe.ZAdd(ctx, s.keys.holders, redis.Z{Score: float64(nowMillis + s.config.PermitTTL.Milliseconds()), Member: token})
			}
		})
		if err != nil {
			return acquireAttemptResult{}, err
		}
		if grant {
			holders++
			if !registerWaiter && waiters > 0 {
				waiters--
			}
		} else if registerWaiter {
			waiters++
		}
		status := tryAcquireWaiting
		if grant {
			status = tryAcquireGranted
		}
		return acquireAttemptResult{
			result: acquireResult{confirmedAt: leaseClockStarted, holders: holders, waiters: waiters, prunedHolders: prunedHolders, prunedWaiters: prunedWaiters},
			status: status,
		}, nil
	})
	if err != nil {
		return acquireResult{confirmedAt: attemptStarted}, 0, err
	}
	return attempt.result, attempt.status, nil
}

func (s *Semaphore) renew(ctx context.Context, token, requestID string) error {
	_, err := withOperationGate(ctx, s, []string{s.keys.holders, s.keys.waiters, s.keys.tokenQueue, s.keys.tokenRequest, s.keys.activeRequest}, true, func(tx *redis.Tx, _ operationGate) (struct{}, error) {
		states, readErr := loadTokenStates(ctx, tx, s.keys, []string{token})
		if readErr != nil {
			return struct{}{}, readErr
		}
		state := states[token]
		now, timeErr := tx.Time(ctx).Result()
		if timeErr != nil {
			return struct{}{}, timeErr
		}
		remaining, ttlErr := tx.PTTL(ctx, s.keys.operationGate).Result()
		if ttlErr != nil {
			return struct{}{}, ttlErr
		}
		if !state.hasRequest || state.requestID != requestID || !state.activeMatches ||
			state.holderExpiry <= now.UnixMilli()+remaining.Milliseconds() {
			if err := validateWatchedState(ctx, tx, s.keys.operationGate); err != nil {
				return struct{}{}, err
			}
			return struct{}{}, ErrLeaseLost
		}
		err := execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
			pipe.ZAddArgs(ctx, s.keys.holders, redis.ZAddArgs{
				XX:      true,
				Members: []redis.Z{{Score: float64(now.UnixMilli() + s.config.PermitTTL.Milliseconds()), Member: token}},
			})
		})
		return struct{}{}, err
	})
	return err
}

func (s *Semaphore) release(ctx context.Context, token, requestID string) (bool, error) {
	return s.cleanupToken(ctx, token, requestID, true)
}

func (s *Semaphore) cancel(ctx context.Context, token, requestID string) (bool, error) {
	return s.cleanupToken(ctx, token, requestID, false)
}

func (s *Semaphore) cleanupToken(ctx context.Context, token, requestID string, requireLiveHolder bool) (bool, error) {
	matched, err := withOperationGate(ctx, s, s.cleanupKeys(), true, func(tx *redis.Tx, gate operationGate) (bool, error) {
		states, readErr := loadTokenStates(ctx, tx, s.keys, []string{token})
		if readErr != nil {
			return false, readErr
		}
		state := states[token]
		if !state.hasRequest || state.requestID != requestID {
			if err := validateWatchedState(ctx, tx, s.keys.operationGate); err != nil {
				return false, err
			}
			return false, nil
		}
		matched := state.hasHolder || state.hasWaiter
		if requireLiveHolder {
			now, timeErr := tx.Time(ctx).Result()
			if timeErr != nil {
				return false, timeErr
			}
			matched = state.hasHolder &&
				state.holderExpiry > now.UnixMilli()+gate.ttl.Milliseconds() &&
				state.activeMatches
		}
		removals := map[string]*tokenRemoval{}
		addTokenRemoval(removals, state, 0)
		err := execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
			queueTokenRemovals(ctx, pipe, s.keys, removals)
		})
		return matched, err
	})
	if err != nil {
		return false, fmt.Errorf("clean up semaphore token: %w", err)
	}
	return matched, nil
}

func isDeterministicSemaphoreError(err error) bool {
	if transactionOutcomeUncertain(err) ||
		errors.Is(err, redis.TxFailedErr) ||
		errors.Is(err, errOperationGateBusy) ||
		errors.Is(err, errOperationGateLost) {
		return false
	}
	return errors.Is(err, errInvalidSemaphoreState) ||
		errors.Is(err, ErrLeaseLost) ||
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

func (s *Semaphore) stateKeys() []string {
	keys := make([]string, 0, 8+len(s.keys.queues))
	keys = append(keys,
		s.keys.config,
		s.keys.sequence,
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
// Waiter metadata is requested in scan batches so one call does not retain the
// entire waiter set in client memory.
func (s *Semaphore) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, invalidConfig("context must not be nil")
	}
	var snapshot Snapshot
	err := s.redis.Watch(ctx, func(tx *redis.Tx) error {
		now, err := tx.Time(ctx).Result()
		if err != nil {
			return err
		}
		nowMillis := now.UnixMilli()
		minimum := "(" + strconv.FormatInt(nowMillis, 10)
		var holderCommand, waiterCommand *redis.IntCmd
		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			holderCommand = pipe.ZCount(ctx, s.keys.holders, minimum, "+inf")
			waiterCommand = pipe.ZCount(ctx, s.keys.waiters, minimum, "+inf")
			return nil
		})
		if err != nil {
			return err
		}
		holders, err := holderCommand.Result()
		if err != nil {
			return err
		}
		waiters, err := waiterCommand.Result()
		if err != nil {
			return err
		}

		byQueue := make(map[string]int64, len(s.config.QueuesByPriority))
		for _, queue := range s.config.QueuesByPriority {
			byQueue[queue] = 0
		}
		var oldestStarted int64
		if waiters != 0 {
			const snapshotBatchSize = 512
			var cursor uint64
			for {
				entries, nextCursor, scanErr := tx.ZScan(ctx, s.keys.waiters, cursor, "", snapshotBatchSize).Result()
				if scanErr != nil {
					return scanErr
				}
				if len(entries)%2 != 0 {
					return fmt.Errorf("%w: odd semaphore waiter scan entry count %d", errInvalidSemaphoreState, len(entries))
				}
				liveTokens := make([]string, 0, len(entries)/2)
				for index := 0; index < len(entries); index += 2 {
					expiry, parseErr := strconv.ParseFloat(entries[index+1], 64)
					if parseErr != nil {
						return fmt.Errorf("%w: invalid waiter expiry %q", errInvalidSemaphoreState, entries[index+1])
					}
					if expiry > float64(nowMillis) {
						liveTokens = append(liveTokens, entries[index])
					}
				}

				for offset := 0; offset < len(liveTokens); offset += snapshotBatchSize {
					end := min(offset+snapshotBatchSize, len(liveTokens))
					batch := liveTokens[offset:end]
					var queueCommand, startedCommand *redis.SliceCmd
					_, metadataErr := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						queueCommand = pipe.HMGet(ctx, s.keys.tokenQueue, batch...)
						startedCommand = pipe.HMGet(ctx, s.keys.waiterStarted, batch...)
						return nil
					})
					if metadataErr != nil {
						return metadataErr
					}
					queues, metadataErr := queueCommand.Result()
					if metadataErr != nil {
						return metadataErr
					}
					startedValues, metadataErr := startedCommand.Result()
					if metadataErr != nil {
						return metadataErr
					}
					for index := range batch {
						queueText, present, parseErr := optionalRedisString(queues[index])
						if parseErr == nil && present {
							queueIndex, conversionErr := strconv.Atoi(queueText)
							if conversionErr == nil && queueIndex >= 1 && queueIndex <= len(s.config.QueuesByPriority) {
								byQueue[s.config.QueuesByPriority[queueIndex-1]]++
							}
						}
						startedText, present, parseErr := optionalRedisString(startedValues[index])
						if parseErr == nil && present {
							started, conversionErr := strconv.ParseInt(startedText, 10, 64)
							if conversionErr == nil && started <= nowMillis && (oldestStarted == 0 || started < oldestStarted) {
								oldestStarted = started
							}
						}
					}
				}
				cursor = nextCursor
				if cursor == 0 {
					break
				}
			}
		}

		var oldestWait time.Duration
		if oldestStarted != 0 {
			oldestWait = time.Duration(nowMillis-oldestStarted) * time.Millisecond
		}
		snapshot = Snapshot{
			Holders:          holders,
			Waiters:          waiters,
			WaitersByQueue:   byQueue,
			OldestWait:       oldestWait,
			CapacityExceeded: holders > int64(s.config.Capacity),
		}
		return nil
	}, s.keys.config)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read semaphore snapshot from primary: %w", err)
	}
	return snapshot, nil
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
