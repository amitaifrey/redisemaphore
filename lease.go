package redisemaphore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errLeaseConfirmationTooOld  = errors.New("redisemaphore: lease confirmation arrived after the safety cutoff")
	errLeaseConfirmationMissing = errors.New("redisemaphore: lease was not reconfirmed before the safety cutoff")
)

type leaseRunOptions struct {
	resource       string
	namespace      string
	queue          string
	requestID      string
	ttl            time.Duration
	pollInitial    time.Duration
	pollMax        time.Duration
	cleanupTimeout time.Duration
	observer       Observer
	confirmedAt    time.Time
	renew          func(context.Context) error
	release        func(context.Context) (bool, error)
	leaseLossEvent *atomic.Bool
	renewRunner    *renewAttemptRunner
}

func runWithLease(ctx context.Context, opts leaseRunOptions, fn func(context.Context) error) (err error) {
	opts.leaseLossEvent = &atomic.Bool{}
	opts.renewRunner = &renewAttemptRunner{renew: opts.renew}
	if ctxErr := ctx.Err(); ctxErr != nil {
		releaseErr := releaseRunLease(opts, true)
		return joinReleaseError(ctxErr, releaseErr)
	}

	confirmedAt, confirmErr := confirmLeaseBeforeCallback(ctx, opts)
	if confirmErr != nil {
		releaseErr := releaseRunLease(opts, true)
		return joinReleaseError(confirmErr, releaseErr)
	}
	opts.confirmedAt = confirmedAt

	leaseCtx, cancelLease := context.WithCancelCause(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		cancelLease(ctxErr)
		releaseErr := releaseRunLease(opts, true)
		return joinReleaseError(ctxErr, releaseErr)
	}

	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewDone := make(chan error, 1)
	go func() {
		renewDone <- renewLease(renewCtx, cancelLease, opts)
	}()

	var panicValue any
	func() {
		defer func() {
			panicValue = recover()
		}()
		err = fn(leaseCtx)
	}()

	// Cancel any child work before releasing the Redis ownership. If lease loss
	// already supplied a more specific cause, CancelCauseFunc preserves it.
	cancelLease(context.Canceled)
	stopRenew()
	renewErr := <-renewDone
	if renewErr != nil {
		err = errors.Join(err, renewErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = errors.Join(err, ctxErr)
	}

	releaseErr := releaseRunLease(opts, false)
	err = joinReleaseError(err, releaseErr)

	if panicValue != nil {
		panic(panicValue)
	}
	return err
}

func confirmLeaseBeforeCallback(ctx context.Context, opts leaseRunOptions) (time.Time, error) {
	confirmedAt := opts.confirmedAt
	if confirmedAt.IsZero() {
		// Compatibility for internal callers while they migrate to passing the
		// command-start timestamp. A real acquisition path should always set it.
		confirmedAt = time.Now()
	}
	if leaseConfirmationIsSafe(time.Now(), confirmedAt, opts.ttl) {
		return confirmedAt, nil
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}

	attemptCtx, cancelAttempt := context.WithTimeout(ctx, opts.cleanupTimeout)
	attemptStarted, err := opts.renewRunner.run(attemptCtx)
	cancelAttempt()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return time.Time{}, ctxErr
	}
	if err != nil {
		emitLeaseRenewError(opts, err)
		if !errors.Is(err, ErrLeaseLost) {
			emitRedisError(opts, err)
		}
		return time.Time{}, emitLeaseLost(opts, fmt.Errorf("reconfirm lease before callback: %w", err))
	}
	if !leaseConfirmationIsSafe(time.Now(), attemptStarted, opts.ttl) {
		emitLeaseRenewError(opts, errLeaseConfirmationTooOld)
		return time.Time{}, emitLeaseLost(opts, errLeaseConfirmationTooOld)
	}

	emitLeaseRenewed(opts)
	return attemptStarted, nil
}

func renewLease(ctx context.Context, cancelLease context.CancelCauseFunc, opts leaseRunOptions) error {
	lastConfirmed := opts.confirmedAt
	if lastConfirmed.IsZero() {
		lastConfirmed = time.Now()
	}
	renewEvery := opts.ttl / 3
	safetyWindow := opts.ttl / 3
	retryMaximum := opts.pollMax
	if retentionRetryMaximum := safetyWindow / 2; retryMaximum > retentionRetryMaximum {
		retryMaximum = retentionRetryMaximum
	}
	if retryMaximum <= 0 {
		retryMaximum = time.Millisecond
	}
	delay := durationUntil(lastConfirmed.Add(renewEvery))
	retryDelay := opts.pollInitial
	var terminalErr error
	var lastRenewErr error

	reportLeaseLost := func(cause error) {
		if terminalErr != nil {
			return
		}
		terminalErr = emitLeaseLost(opts, cause)
		cancelLease(terminalErr)
	}

	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return terminalErr
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return terminalErr
		}

		lossAt := lastConfirmed.Add(opts.ttl - safetyWindow)
		if terminalErr == nil && !time.Now().Before(lossAt) {
			cause := lastRenewErr
			if cause == nil {
				cause = errLeaseConfirmationMissing
			}
			reportLeaseLost(cause)
		}

		attemptTimeout := opts.cleanupTimeout
		if terminalErr == nil {
			untilLoss := time.Until(lossAt)
			if untilLoss <= 0 {
				cause := lastRenewErr
				if cause == nil {
					cause = errLeaseConfirmationMissing
				}
				reportLeaseLost(cause)
			} else if attemptTimeout > untilLoss {
				// Do not let one stuck Redis call consume the shutdown reserve.
				// The local watchdog must cancel the callback at lossAt even when
				// the client ignores context deadlines for socket I/O.
				attemptTimeout = untilLoss
			}
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
		attemptStarted, err := opts.renewRunner.run(attemptCtx)
		cancelAttempt()
		if ctx.Err() != nil {
			return terminalErr
		}

		if err == nil {
			lastConfirmed = attemptStarted
			lastRenewErr = nil
			retryDelay = opts.pollInitial
			if !leaseConfirmationIsSafe(time.Now(), attemptStarted, opts.ttl) {
				lastRenewErr = errLeaseConfirmationTooOld
				emitLeaseRenewError(opts, lastRenewErr)
				reportLeaseLost(lastRenewErr)
				delay = 0
				continue
			}

			emitLeaseRenewed(opts)
			delay = durationUntil(lastConfirmed.Add(renewEvery))
			continue
		}

		lastRenewErr = err
		emitLeaseRenewError(opts, err)
		if !errors.Is(err, ErrLeaseLost) {
			emitRedisError(opts, err)
		}

		if errors.Is(err, ErrLeaseLost) {
			reportLeaseLost(err)
			return terminalErr
		}
		if !isRetryableRedisOperationError(err) {
			reportLeaseLost(err)
		}

		lossAt = lastConfirmed.Add(opts.ttl - safetyWindow)
		if terminalErr == nil && !time.Now().Before(lossAt) {
			reportLeaseLost(err)
		}

		effectiveRetryDelay := retryDelay
		if effectiveRetryDelay > retryMaximum {
			effectiveRetryDelay = retryMaximum
		}
		delay = jitter(effectiveRetryDelay)
		if terminalErr == nil {
			untilLoss := time.Until(lossAt)
			if untilLoss <= 0 {
				reportLeaseLost(err)
			} else if delay > untilLoss {
				delay = untilLoss
			}
		}
		retryDelay = growBackoff(retryDelay, retryMaximum)
	}
}

func releaseRunLease(opts leaseRunOptions, allowMissing bool) error {
	releaseErr := retryTokenCleanup(
		context.Background(),
		opts.cleanupTimeout,
		opts.pollInitial,
		opts.pollMax,
		allowMissing,
		func(ctx context.Context) (bool, error) {
			matched, err := opts.release(ctx)
			if err != nil {
				emitRedisError(opts, err)
			}
			return matched, err
		},
	)
	if errors.Is(releaseErr, ErrLeaseLost) {
		emitLeaseLost(opts, releaseErr)
	}
	emitEvent(context.Background(), opts.observer, Event{
		Type:      EventRelease,
		Resource:  opts.resource,
		Namespace: opts.namespace,
		Queue:     opts.queue,
		RequestID: opts.requestID,
		Err:       releaseErr,
	})
	return releaseErr
}

// retryTokenCleanup retries ambiguous cleanup outcomes with the same ownership
// token. A missing token is success for acquisition cancellation. For normal
// release, the first definitive missing result means ownership was lost; after
// an ambiguous error it is an idempotent confirmation that an earlier attempt
// may already have committed.
func retryTokenCleanup(
	parent context.Context,
	timeout time.Duration,
	pollInitial time.Duration,
	pollMax time.Duration,
	allowMissing bool,
	cleanup func(context.Context) (bool, error),
) error {
	overallCtx, cancelOverall := context.WithTimeout(parent, timeout)
	defer cancelOverall()

	perAttemptLimit := timeout / 2
	if perAttemptLimit <= 0 {
		perAttemptLimit = timeout
	}
	backoff := pollInitial
	ambiguous := false
	var lastErr error
	runner := cleanupAttemptRunner{cleanup: cleanup}

	for {
		if overallErr := overallCtx.Err(); overallErr != nil {
			if lastErr != nil {
				return errors.Join(lastErr, overallErr)
			}
			return overallErr
		}

		attemptTimeout := perAttemptLimit
		if deadline, ok := overallCtx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				continue
			}
			if attemptTimeout > remaining {
				attemptTimeout = remaining
			}
		}
		attemptCtx, cancelAttempt := context.WithTimeout(overallCtx, attemptTimeout)
		matched, err, completed := runner.run(attemptCtx)
		cancelAttempt()
		if err == nil {
			if matched || allowMissing || ambiguous {
				return nil
			}
			return ErrLeaseLost
		}

		lastErr = err
		if !isRetryableRedisOperationError(err) {
			return err
		}
		// A completed retryable transport/context error may mean the command
		// committed but its acknowledgement was lost. An explicit Redis error
		// reply is definitive non-execution. Merely reaching our local wait
		// deadline establishes neither: the single in-flight command may later
		// return a definitive missing-token result, which must report lease loss.
		if completed && !isRedisServerError(err) {
			ambiguous = true
		}
		if overallErr := overallCtx.Err(); overallErr != nil {
			return errors.Join(lastErr, overallErr)
		}

		delay := jitter(backoff)
		if deadline, ok := overallCtx.Deadline(); ok {
			remaining := time.Until(deadline)
			reserve := remaining / 2
			if delay > reserve {
				delay = reserve
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-overallCtx.Done():
			stopTimer(timer)
			return errors.Join(lastErr, overallCtx.Err())
		case <-timer.C:
		}
		backoff = growBackoff(backoff, pollMax)
	}
}

type cleanupAttemptResult struct {
	matched bool
	err     error
}

type renewAttempt struct {
	started time.Time
	done    chan error
}

// renewAttemptRunner keeps at most one socket operation in flight. go-redis
// does not apply context deadlines to socket I/O unless callers opt in with
// ContextTimeoutEnabled, so callers independently stop waiting at their local
// deadline. A later retry first observes the existing attempt; it never grows
// an unbounded set of goroutines when a connection stays stuck. The original
// command-start time is retained so a late success cannot look newer than it
// really is.
type renewAttemptRunner struct {
	mu       sync.Mutex
	renew    func(context.Context) error
	inFlight *renewAttempt
}

func (r *renewAttemptRunner) run(ctx context.Context) (time.Time, error) {
	r.mu.Lock()
	attempt := r.inFlight
	if attempt == nil {
		attempt = &renewAttempt{started: time.Now(), done: make(chan error, 1)}
		r.inFlight = attempt
		go func() {
			attempt.done <- r.renew(ctx)
		}()
	}
	r.mu.Unlock()

	select {
	case err := <-attempt.done:
		r.mu.Lock()
		if r.inFlight == attempt {
			r.inFlight = nil
		}
		r.mu.Unlock()
		return attempt.started, err
	case <-ctx.Done():
		return attempt.started, ctx.Err()
	}
}

type cleanupAttempt struct {
	done chan cleanupAttemptResult
}

type cleanupAttemptRunner struct {
	mu       sync.Mutex
	cleanup  func(context.Context) (bool, error)
	inFlight *cleanupAttempt
}

func (r *cleanupAttemptRunner) run(ctx context.Context) (bool, error, bool) {
	r.mu.Lock()
	attempt := r.inFlight
	if attempt == nil {
		attempt = &cleanupAttempt{done: make(chan cleanupAttemptResult, 1)}
		r.inFlight = attempt
		go func() {
			matched, err := r.cleanup(ctx)
			attempt.done <- cleanupAttemptResult{matched: matched, err: err}
		}()
	}
	r.mu.Unlock()

	select {
	case result := <-attempt.done:
		r.mu.Lock()
		if r.inFlight == attempt {
			r.inFlight = nil
		}
		r.mu.Unlock()
		return result.matched, result.err, true
	case <-ctx.Done():
		return false, ctx.Err(), false
	}
}

func leaseConfirmationIsSafe(now, confirmedAt time.Time, ttl time.Duration) bool {
	return now.Before(confirmedAt.Add(ttl - ttl/3))
}

func durationUntil(deadline time.Time) time.Duration {
	delay := time.Until(deadline)
	if delay < 0 {
		return 0
	}
	return delay
}

func newLeaseLostError(cause error) error {
	if cause == nil {
		return ErrLeaseLost
	}
	if errors.Is(cause, ErrLeaseLost) {
		return cause
	}
	return errors.Join(ErrLeaseLost, cause)
}

func emitLeaseLost(opts leaseRunOptions, cause error) error {
	leaseErr := newLeaseLostError(cause)
	if opts.leaseLossEvent != nil && !opts.leaseLossEvent.CompareAndSwap(false, true) {
		return leaseErr
	}
	emitEvent(context.Background(), opts.observer, Event{
		Type:      EventLeaseLost,
		Resource:  opts.resource,
		Namespace: opts.namespace,
		Queue:     opts.queue,
		RequestID: opts.requestID,
		Err:       leaseErr,
	})
	return leaseErr
}

func joinReleaseError(err, releaseErr error) error {
	if releaseErr == nil {
		return err
	}
	return errors.Join(err, fmt.Errorf("release lease: %w", releaseErr))
}

func emitLeaseRenewed(opts leaseRunOptions) {
	emitEvent(context.Background(), opts.observer, Event{
		Type:      EventLeaseRenewed,
		Resource:  opts.resource,
		Namespace: opts.namespace,
		Queue:     opts.queue,
		RequestID: opts.requestID,
	})
}

func emitLeaseRenewError(opts leaseRunOptions, err error) {
	emitEvent(context.Background(), opts.observer, Event{
		Type:      EventLeaseRenewError,
		Resource:  opts.resource,
		Namespace: opts.namespace,
		Queue:     opts.queue,
		RequestID: opts.requestID,
		Err:       err,
	})
}

func emitRedisError(opts leaseRunOptions, err error) {
	emitEvent(context.Background(), opts.observer, Event{
		Type:      EventRedisError,
		Resource:  opts.resource,
		Namespace: opts.namespace,
		Queue:     opts.queue,
		RequestID: opts.requestID,
		Err:       err,
	})
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	next := current * 2
	if next < current || next > maximum {
		return maximum
	}
	return next
}

func jitter(maximum time.Duration) time.Duration {
	if maximum <= time.Millisecond {
		return maximum
	}
	value := time.Duration(rand.Int63n(int64(maximum)))
	if value < time.Millisecond {
		return time.Millisecond
	}
	return value
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
