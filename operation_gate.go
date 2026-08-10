package redisemaphore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	errOperationGateBusy = errors.New("redisemaphore: operation gate is busy")
	errOperationGateLost = errors.New("redisemaphore: operation gate ownership lost")
)

// uncertainTransactionError prevents ClusterClient.Watch from invoking a
// callback a second time after MULTI/EXEC may already have committed. The
// package-level operation retry then reconciles the same ownership token.
// go-redis only retries direct I/O and timeout error types; this wrapper is
// deliberately neither while retaining the original error for callers.
type uncertainTransactionError struct {
	err error
}

func (e *uncertainTransactionError) Error() string {
	return fmt.Sprintf("redisemaphore: transaction outcome is uncertain: %v", e.err)
}

func (e *uncertainTransactionError) Unwrap() error {
	return e.err
}

func transactionOutcomeUncertain(err error) bool {
	var uncertain *uncertainTransactionError
	return errors.As(err, &uncertain)
}

// execWatchedTransaction is the only state-commit path. Redis replies and
// WATCH conflicts are definitive and may be handled normally. A transport
// failure returned from the MULTI/EXEC pipeline is ambiguous, so wrap it to
// stop ClusterClient.Watch from silently running the callback again.
func execWatchedTransaction(
	ctx context.Context,
	tx *redis.Tx,
	queue func(redis.Pipeliner),
) error {
	_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		queue(pipe)
		return nil
	})
	if err == nil || errors.Is(err, redis.TxFailedErr) || isRedisServerError(err) {
		return err
	}
	return &uncertainTransactionError{err: err}
}

// validateWatchedState forces EXEC even when an operation has no writes. WATCH
// only checks for intervening changes at EXEC time; returning directly from a
// callback would otherwise allow a stale definitive result.
func validateWatchedState(ctx context.Context, tx *redis.Tx, gateKey string) error {
	err := execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
		pipe.Get(ctx, gateKey)
	})
	if errors.Is(err, redis.Nil) {
		return errOperationGateLost
	}
	return err
}

// operationGateTTL bounds a Go-side critical section with an expiring Redis
// key. Half of CleanupTimeout leaves the cleanup retry loop another attempt;
// the lease bounds ensure a transaction that commits before gate expiry does
// not create an already-expired waiter or holder lease.
func operationGateTTL(config SemaphoreConfig) time.Duration {
	ttl := config.CleanupTimeout / 2
	if waiterBound := config.WaiterTTL / 2; ttl > waiterBound {
		ttl = waiterBound
	}
	if ttl < time.Millisecond {
		return time.Millisecond
	}
	return ttl
}

type operationGate struct {
	token string
	ttl   time.Duration
}

// acquireOperationGate gives ownership maintenance priority. Admission uses
// SET NX PX; renew/release/cancel use SET PX to preempt admission. Since every
// state transaction watches the gate, Redis ordering guarantees an admission
// either commits first or aborts after preemption. Every epoch has a fresh
// token, so stale cleanup cannot delete a replacement epoch.
func (s *Semaphore) acquireOperationGate(ctx context.Context, maintenance bool) (operationGate, error) {
	ttl := operationGateTTL(s.config)
	if err := ctx.Err(); err != nil {
		return operationGate{}, err
	}
	token, err := randomSemaphoreToken()
	if err != nil {
		return operationGate{}, fmt.Errorf("generate operation gate token: %w", err)
	}
	acquired := true
	var setErr error
	if maintenance {
		setErr = s.redis.Set(ctx, s.keys.operationGate, token, ttl).Err()
	} else {
		acquired, setErr = s.redis.SetNX(ctx, s.keys.operationGate, token, ttl).Result()
	}
	if setErr == nil && acquired {
		return operationGate{token: token, ttl: ttl}, nil
	}
	// go-redis may transparently retry SET after losing the first reply. A
	// resulting (false, nil) can therefore mean the first SET succeeded.
	owned, remaining, observeErr := s.observeOperationGate(ctx, token)
	if observeErr == nil && owned && remaining > 0 {
		return operationGate{token: token, ttl: ttl}, nil
	}
	if setErr != nil {
		if observeErr != nil {
			return operationGate{}, errors.Join(setErr, observeErr)
		}
		return operationGate{}, setErr
	}
	if observeErr != nil {
		return operationGate{}, observeErr
	}
	return operationGate{}, errOperationGateBusy
}

// observeOperationGate uses Watch solely to pin the read to the hash slot's
// primary. This matters when a ClusterClient has replica reads enabled.
func (s *Semaphore) observeOperationGate(ctx context.Context, token string) (bool, time.Duration, error) {
	var owned bool
	var remaining time.Duration
	err := s.redis.Watch(ctx, func(tx *redis.Tx) error {
		value, err := tx.Get(ctx, s.keys.operationGate).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return err
		}
		if value != token {
			return nil
		}
		remaining, err = tx.PTTL(ctx, s.keys.operationGate).Result()
		if err != nil {
			return err
		}
		owned = remaining > 0
		return nil
	}, s.keys.operationGate)
	return owned, remaining, err
}

// releaseOperationGate conditionally deletes only the epoch it owns. Missing
// or replaced ownership is already released. Ambiguous EXEC outcomes are
// reconciled by re-reading the same token until the short cleanup deadline.
func (s *Semaphore) releaseOperationGate(ctx context.Context, gate operationGate) error {
	backoff := s.config.PollInitial
	for {
		err := s.redis.Watch(ctx, func(tx *redis.Tx) error {
			value, getErr := tx.Get(ctx, s.keys.operationGate).Result()
			if errors.Is(getErr, redis.Nil) {
				return nil
			}
			if getErr != nil {
				return getErr
			}
			if value != gate.token {
				return nil
			}
			return execWatchedTransaction(ctx, tx, func(pipe redis.Pipeliner) {
				pipe.Del(ctx, s.keys.operationGate)
			})
		}, s.keys.operationGate)
		if err == nil {
			return nil
		}
		if !errors.Is(err, redis.TxFailedErr) &&
			!transactionOutcomeUncertain(err) &&
			!isRetryableRedisOperationError(err) {
			return err
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}

		timer := time.NewTimer(jitter(backoff))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		backoff = growBackoff(backoff, s.config.PollMax)
	}
}

// withOperationGate serializes package writers, watches every key the caller
// may touch, and verifies the random gate token inside the watched callback.
// The gate is not renewed concurrently: expiry invalidates WATCH, making a
// delayed stale EXEC abort. WATCH conflicts retry in a fresh bounded epoch.
func withOperationGate[T any](
	ctx context.Context,
	s *Semaphore,
	stateKeys []string,
	maintenance bool,
	operation func(*redis.Tx, operationGate) (T, error),
) (T, error) {
	var zero T
	backoff := s.config.PollInitial
	for {
		gate, err := s.acquireOperationGate(ctx, maintenance)
		if err != nil {
			return zero, err
		}

		watchKeys := make([]string, 0, 1+len(stateKeys))
		watchKeys = append(watchKeys, s.keys.operationGate)
		watchKeys = append(watchKeys, stateKeys...)
		var result T
		err = s.redis.Watch(ctx, func(tx *redis.Tx) error {
			value, getErr := tx.Get(ctx, s.keys.operationGate).Result()
			if errors.Is(getErr, redis.Nil) || (getErr == nil && value != gate.token) {
				return errOperationGateLost
			}
			if getErr != nil {
				return getErr
			}
			remaining, ttlErr := tx.PTTL(ctx, s.keys.operationGate).Result()
			if ttlErr != nil {
				return ttlErr
			}
			if remaining <= 0 {
				return errOperationGateLost
			}
			result, getErr = operation(tx, operationGate{token: gate.token, ttl: remaining})
			return getErr
		}, watchKeys...)

		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), gate.ttl)
		_ = s.releaseOperationGate(releaseCtx, gate)
		cancelRelease()
		// Gate cleanup never changes the state result. The gate is bounded and
		// token-safe, so a cleanup failure can only delay the next admission;
		// joining it here would corrupt exact EXEC-ambiguity attribution.

		if err == nil {
			return result, nil
		}
		if transactionOutcomeUncertain(err) {
			return zero, err
		}
		if !errors.Is(err, redis.TxFailedErr) &&
			!errors.Is(err, errOperationGateLost) {
			return zero, err
		}
		if ctx.Err() != nil {
			return zero, errors.Join(err, ctx.Err())
		}

		timer := time.NewTimer(jitter(backoff))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return zero, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		backoff = growBackoff(backoff, s.config.PollMax)
	}
}
