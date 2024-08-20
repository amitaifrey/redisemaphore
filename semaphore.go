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
	mutex           Mutex
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

func (this *semaphore) AcquireQueue(ctx context.Context, queue, key string) error {
	if !slices.Contains(this.queueKeysByPrio, queue) {
		return fmt.Errorf("queue %s is not in the list of queue keys by prio", queue)
	}

	for {
		addCmd := this.redisClient.ZAddArgs(ctx, queue, redis.ZAddArgs{
			NX: true, // do not override score
			Members: []redis.Z{
				{Score: float64(time.Now().UnixNano()), Member: key},
			}})
		if addCmd.Err() != nil && addCmd.Err() != redis.Nil {
			return errors.WrapPrefix(addCmd.Err(), "failed to push key", 0)
		}

		semaphoreExists := this.redisClient.ZRank(ctx, this.name, key)
		if semaphoreExists.Err() == nil {
			break
		}
		if semaphoreExists.Err() != nil && semaphoreExists.Err() != redis.Nil {
			return errors.WrapPrefix(semaphoreExists.Err(), "failed to check if key exists", 0)
		}

		err := this.tryInsertNext(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(this.pollDur):
			continue
		}
	}

	remCmd := this.redisClient.ZRem(ctx, queue, key)
	if remCmd.Err() != nil && remCmd.Err() != redis.Nil {
		return errors.WrapPrefix(remCmd.Err(), "failed to remove key", 0)
	}

	return nil
}

func (this *semaphore) tryInsertNext(ctx context.Context) error {
	err := this.mutex.Acquire(ctx)
	if err != nil {
		return errors.WrapPrefix(err, "failed to acquire mutex", 0)
	}
	defer this.mutex.Release(ctx)

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

		readKeyCmd := this.redisClient.ZRangeByScore(ctx, queue, &redis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 0, Count: 1})
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
	zcard := this.redisClient.ZCard(ctx, this.name)
	if zcard.Err() != nil {
		return -1, errors.WrapPrefix(zcard.Err(), "failed to get length of semaphore", 0)
	}
	return this.size - int(zcard.Val()), nil
}

func (this *semaphore) insertNext(ctx context.Context, queue, key string) error {
	r1 := this.redisClient.ZRemRangeByScore(ctx, this.name, "-inf", fmt.Sprintf("%d", time.Now().Add(-this.deleteTimeout).UnixNano()))
	if r1.Err() != nil {
		return errors.WrapPrefix(r1.Err(), "failed to clean up semaphore", 0)
	}

	r2 := this.redisClient.ZAdd(ctx, this.name, redis.Z{Score: float64(time.Now().UnixNano()), Member: key})
	if r2.Err() != nil {
		return errors.WrapPrefix(r2.Err(), "failed to add key", 0)
	}

	r3 := this.redisClient.ZRem(ctx, queue, key)
	if r3.Err() != nil && r3.Err() != redis.Nil {
		return errors.WrapPrefix(r3.Err(), "failed to remove key", 0)
	}
	return nil
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
