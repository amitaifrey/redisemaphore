package redisemaphore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-errors/errors"
	"github.com/redis/go-redis/v9"
)

var ErrNoKeysLeft = errors.New("error: no keys left in queues")
var ErrDuplicateKey = errors.New("error: duplicate key")

type Semaphore interface {
	Acquire(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
	AcquireQueue(ctx context.Context, queue, key string) error
	ReleaseQueue(ctx context.Context, queue, key string) error
}

type SemaphoreOption interface {
	Apply(*semaphore)
}

type SemaphoreOptionFunc func(*semaphore)

func (f SemaphoreOptionFunc) Apply(s *semaphore) {
	f(s)
}

func WithSemaphoreMutexName(mutexName string) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.mutexName = mutexName
	})
}

func WithSemaphoreMutexExpiry(mutexExpiry time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.mutexExpiry = mutexExpiry
	})
}

func WithSemaphoreMutexTimeout(mutexTimeout time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.mutexTimeout = mutexTimeout
	})
}

func WithSemaphoreDeleteTimeout(deleteTimeout time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.deleteTimeout = deleteTimeout
	})
}

func WithSemaphorePollDur(pollDur time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.pollDur = pollDur
	})
}

func WithSemaphoreQueueKeysByPrio(queueKeysByPrio ...string) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *semaphore) {
		s.queueKeysByPrio = queueKeysByPrio
	})
}

type semaphore struct {
	redisClient     redis.UniversalClient
	mutex           TokenMutex
	name            string
	size            int
	mutexName       string
	mutexExpiry     time.Duration
	mutexTimeout    time.Duration
	deleteTimeout   time.Duration
	pollDur         time.Duration
	queueKeysByPrio []string
}

func NewSemaphore(redisClient redis.UniversalClient, name string, size int, opts ...SemaphoreOption) (Semaphore, error) {
	s := &semaphore{
		redisClient:     redisClient,
		name:            name,
		size:            size,
		mutexName:       fmt.Sprintf("%s-mutex", name),
		mutexExpiry:     1 * time.Minute,
		mutexTimeout:    10 * time.Minute,
		deleteTimeout:   10 * time.Minute,
		pollDur:         100 * time.Millisecond,
		queueKeysByPrio: []string{fmt.Sprintf("%s-queue", name)},
	}

	for _, o := range opts {
		o.Apply(s)
	}

	s.mutex = NewMutex(redisClient, s.mutexName, WithMutexExpiry(s.mutexExpiry), WithMutexTimeout(s.mutexTimeout), WithMutexPollDur(s.pollDur))

	return s, nil
}

func (this *semaphore) Acquire(ctx context.Context, key string) error {
	if len(this.queueKeysByPrio) != 1 {
		return fmt.Errorf("queue keys by prio must have exactly one element")
	}
	return this.AcquireQueue(ctx, this.queueKeysByPrio[0], key)
}

func (this *semaphore) AcquireQueue(ctx context.Context, queue, key string) (err error) {
	if !slices.Contains(this.queueKeysByPrio, queue) {
		return fmt.Errorf("queue %s is not in the list of queue keys by prio", queue)
	}

	registered := false
	acquired := false
	defer func() {
		if !registered || acquired {
			return
		}
		cleanupErr := this.withCleanupContext(ctx, func(ctx context.Context) error {
			return this.cleanupWaiter(ctx, key)
		})
		if err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()

	registered, err = this.registerWaiter(ctx, queue, key)
	if err != nil {
		return err
	}

	for {
		exists, err := this.keyExists(ctx, this.name, key)
		if err != nil {
			return errors.WrapPrefix(err, "failed to check if key exists", 0)
		}
		if exists {
			acquired = true
			return nil
		}

		if err := this.tryInsertNext(ctx, queue, key); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(this.pollDur):
			continue
		}
	}
}

func (this *semaphore) tryInsertNext(ctx context.Context, queue, key string) error {
	return this.withMutex(ctx, this.mutexTokenDescription("fill", queue, key), func(ctx context.Context) error {
		return this.fillAvailable(ctx)
	})
}

func (this *semaphore) registerWaiter(ctx context.Context, queue, key string) (registered bool, err error) {
	err = this.withMutex(ctx, this.mutexTokenDescription("register", queue, key), func(ctx context.Context) error {
		if err := this.cleanupExpiredHolders(ctx); err != nil {
			return err
		}

		exists, err := this.keyExists(ctx, this.name, key)
		if err != nil {
			return errors.WrapPrefix(err, "failed to check if key exists in semaphore", 0)
		}
		if exists {
			return ErrDuplicateKey
		}

		for _, queueKey := range this.queueKeysByPrio {
			exists, err := this.keyExists(ctx, queueKey, key)
			if err != nil {
				return errors.WrapPrefix(err, fmt.Sprintf("failed to check if key exists in queue: %s", queueKey), 0)
			}
			if exists {
				return ErrDuplicateKey
			}
		}

		addCmd := this.redisClient.ZAddArgs(ctx, queue, redis.ZAddArgs{
			NX: true,
			Members: []redis.Z{
				{Score: this.waiterScore(), Member: key},
			},
		})
		if addCmd.Err() != nil && addCmd.Err() != redis.Nil {
			return errors.WrapPrefix(addCmd.Err(), "failed to push key", 0)
		}
		if addCmd.Val() == 0 {
			return ErrDuplicateKey
		}

		registered = true
		return this.fillAvailable(ctx)
	})
	return registered, err
}

func (this *semaphore) withMutex(ctx context.Context, tokenDescription string, fn func(context.Context) error) (err error) {
	token, err := NewMutexToken(tokenDescription)
	if err != nil {
		return errors.WrapPrefix(err, "failed to create mutex token", 0)
	}

	err = this.mutex.AcquireWithToken(ctx, token)
	if err != nil {
		return errors.WrapPrefix(err, "failed to acquire mutex", 0)
	}
	defer func() {
		releaseErr := this.withCleanupContext(ctx, this.mutex.Release)
		if err == nil && releaseErr != nil {
			err = errors.WrapPrefix(releaseErr, "failed to release mutex", 0)
		}
	}()

	return fn(ctx)
}

func (this *semaphore) fillAvailable(ctx context.Context) error {
	toAdd, err := this.amountToAdd(ctx)
	if err != nil || toAdd <= 0 { // if there is no room err is nil so we can just return it
		return err
	}

	for toAdd > 0 {
		nextQueue, nextKey, err := this.getNextKey(ctx)
		if err == ErrNoKeysLeft {
			return nil
		}
		if err != nil {
			return errors.WrapPrefix(err, "failed to get next key", 0)
		}

		err = this.insertNext(ctx, nextQueue, nextKey)
		if err != nil {
			return errors.WrapPrefix(err, "failed to insert next key", 0)
		}

		toAdd--
	}

	return nil
}

func (this *semaphore) getNextKey(ctx context.Context) (string, string, error) {
	for _, queue := range this.queueKeysByPrio {
		lenCmd := this.redisClient.ZCard(ctx, queue)
		if lenCmd.Err() != nil {
			return "", "", errors.WrapPrefix(lenCmd.Err(), fmt.Sprintf("failed to get length of queue: %s", queue), 0)
		}
		if lenCmd.Val() == 0 { // guaranteed that at least one queue has a key
			continue
		}

		readKeyCmd := this.redisClient.ZRange(ctx, queue, 0, 0)
		if readKeyCmd.Err() != nil {
			return "", "", errors.WrapPrefix(readKeyCmd.Err(), fmt.Sprintf("failed to get key: %s", readKeyCmd), 0)
		}
		if len(readKeyCmd.Val()) == 1 {
			return queue, readKeyCmd.Val()[0], nil
		}
	}

	return "", "", ErrNoKeysLeft
}

func (this *semaphore) amountToAdd(ctx context.Context) (int, error) {
	if err := this.cleanupExpiredHolders(ctx); err != nil {
		return -1, err
	}

	zcard := this.redisClient.ZCard(ctx, this.name)
	if zcard.Err() != nil {
		return -1, errors.WrapPrefix(zcard.Err(), "failed to get length of semaphore", 0)
	}
	return this.size - int(zcard.Val()), nil
}

func (this *semaphore) cleanupExpiredHolders(ctx context.Context) error {
	r1 := this.redisClient.ZRemRangeByScore(ctx, this.name, "-inf", this.expiredHolderScore())
	if r1.Err() != nil {
		return errors.WrapPrefix(r1.Err(), "failed to clean up semaphore", 0)
	}
	return nil
}

func (this *semaphore) cleanupWaiter(ctx context.Context, key string) error {
	return this.withMutex(ctx, this.mutexTokenDescription("cleanup-waiter", "", key), func(ctx context.Context) error {
		return this.cleanupWaiterLocked(ctx, key)
	})
}

func (this *semaphore) cleanupWaiterLocked(ctx context.Context, key string) error {
	for _, queue := range this.queueKeysByPrio {
		if err := this.redisClient.ZRem(ctx, queue, key).Err(); err != nil && err != redis.Nil {
			return errors.WrapPrefix(err, "failed to remove key from queue", 0)
		}
	}
	if err := this.redisClient.ZRem(ctx, this.name, key).Err(); err != nil && err != redis.Nil {
		return errors.WrapPrefix(err, "failed to remove key from semaphore", 0)
	}
	return nil
}

func (this *semaphore) keyExists(ctx context.Context, set, key string) (bool, error) {
	rankCmd := this.redisClient.ZRank(ctx, set, key)
	if rankCmd.Err() == nil {
		return true, nil
	}
	if rankCmd.Err() == redis.Nil {
		return false, nil
	}
	return false, rankCmd.Err()
}

func (this *semaphore) withCleanupContext(ctx context.Context, fn func(context.Context) error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), this.mutexTimeout)
	defer cancel()
	return fn(cleanupCtx)
}

func (this *semaphore) mutexTokenDescription(action, queue, key string) string {
	return fmt.Sprintf("semaphore=%s action=%s queue=%s key=%s", this.name, action, queue, key)
}

func (this *semaphore) insertNext(ctx context.Context, queue, key string) error {
	r1 := this.redisClient.ZAdd(ctx, this.name, redis.Z{Score: this.holderScore(), Member: key})
	if r1.Err() != nil {
		return errors.WrapPrefix(r1.Err(), "failed to add key", 0)
	}

	r2 := this.redisClient.ZRem(ctx, queue, key)
	if r2.Err() != nil && r2.Err() != redis.Nil {
		return errors.WrapPrefix(r2.Err(), "failed to remove key", 0)
	}
	return nil
}

func (this *semaphore) holderScore() float64 {
	return float64(time.Now().UnixMicro())
}

func (this *semaphore) waiterScore() float64 {
	return float64(time.Now().UnixMicro())
}

func (this *semaphore) expiredHolderScore() string {
	return fmt.Sprintf("%d", time.Now().Add(-this.deleteTimeout).UnixMicro())
}

func (this *semaphore) Release(ctx context.Context, key string) error {
	if len(this.queueKeysByPrio) != 1 {
		return fmt.Errorf("queue keys by prio must have exactly one element")
	}
	return this.ReleaseQueue(ctx, this.queueKeysByPrio[0], key)
}

func (this *semaphore) ReleaseQueue(ctx context.Context, queue, key string) error {
	// non-existing members are ignored, so if this was cleaned up this won't return an error
	err := this.redisClient.ZRem(ctx, this.name, key).Err()
	if err == nil || err == redis.Nil {
		return nil
	}
	return errors.WrapPrefix(err, "failed to remove key", 0)
}
