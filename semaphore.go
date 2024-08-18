package redisemaphore

import (
	"context"
	"fmt"
	"time"

	"github.com/go-errors/errors"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

type Semaphore interface {
	Acquire(ctx context.Context, queue, key string) error
	Release(ctx context.Context, queue, key string) error
}

type semaphore struct {
	redisClient     *redis.Client
	mutex           *redsync.Mutex
	semaphoreKey    string
	size            int
	deleteTimeout   time.Duration
	pollDur         time.Duration
	queueKeysByPrio []string
}

func NewSemaphore(redisClient *redis.Client, mutexKey, semaphoreKey string, size int, mutexExpiry, deleteTimeout, pollDur time.Duration, queueKeysByPrio ...string) (Semaphore, error) {
	if len(queueKeysByPrio) == 0 {
		return nil, errors.New("queueKeysByPrio must be of length at least 1")
	}

	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	mutex := rs.NewMutex(mutexKey, redsync.WithExpiry(mutexExpiry))

	return &semaphore{
		redisClient:     redisClient,
		mutex:           mutex,
		semaphoreKey:    semaphoreKey,
		size:            size,
		deleteTimeout:   deleteTimeout,
		pollDur:         pollDur,
		queueKeysByPrio: queueKeysByPrio,
	}, nil
}

func (this *semaphore) Acquire(ctx context.Context, queue, key string) error {
	for {
		addCmd := this.redisClient.ZAddArgs(ctx, queue, redis.ZAddArgs{
			NX: true, // do not override score
			Members: []redis.Z{
				{Score: float64(time.Now().UnixNano()), Member: key},
			}})
		if addCmd.Err() != nil && addCmd.Err() != redis.Nil {
			return errors.WrapPrefix(addCmd.Err(), "failed to push key", 0)
		}

		semaphoreExists := this.redisClient.ZRank(ctx, this.semaphoreKey, key)
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

func (this *semaphore) Release(ctx context.Context, queue, key string) error {
	// non-existing members are ignored, so if this was cleaned up this won't return an error
	err := this.redisClient.ZRem(ctx, this.semaphoreKey, key).Err()
	if err == nil || err == redis.Nil {
		return nil
	}
	return errors.WrapPrefix(err, "failed to remove key", 0)
}

func (this *semaphore) tryInsertNext(ctx context.Context) error {
	err := this.mutex.TryLockContext(ctx)
	if err != nil {
		switch err.(type) {
		case *redsync.ErrTaken, *redsync.ErrNodeTaken:
			return nil
		default:
			return errors.WrapPrefix(err, "failed to acquire lock", 0)
		}
	}
	defer this.mutex.UnlockContext(ctx)

	hasRoom, err := this.hasRoom(ctx)
	if err != nil || !hasRoom { // if there is no room err is nil so we can just return it
		return err
	}

	nextQueue, nextKey, err := this.getNextKey(ctx)
	if err != nil {
		return errors.WrapPrefix(err, "failed to get next key", 0)
	}

	return this.insertNext(ctx, nextQueue, nextKey)
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

	return "", "", fmt.Errorf("no keys found")
}

func (this *semaphore) hasRoom(ctx context.Context) (bool, error) {
	zcard := this.redisClient.ZCard(ctx, this.semaphoreKey)
	if zcard.Err() != nil {
		return false, errors.WrapPrefix(zcard.Err(), "failed to get length of semaphore", 0)
	}
	return zcard.Val() < int64(this.size), nil
}

func (this *semaphore) insertNext(ctx context.Context, queue, key string) error {
	r1 := this.redisClient.ZRemRangeByScore(ctx, this.semaphoreKey, "-inf", fmt.Sprintf("%d", time.Now().Add(-this.deleteTimeout).UnixNano()))
	if r1.Err() != nil {
		return errors.WrapPrefix(r1.Err(), "failed to clean up semaphore", 0)
	}

	r2 := this.redisClient.ZAdd(ctx, this.semaphoreKey, redis.Z{Score: float64(time.Now().UnixNano()), Member: key})
	if r2.Err() != nil {
		return errors.WrapPrefix(r2.Err(), "failed to add key", 0)
	}

	r3 := this.redisClient.ZRem(ctx, queue, key)
	if r3.Err() != nil && r3.Err() != redis.Nil {
		return errors.WrapPrefix(r3.Err(), "failed to remove key", 0)
	}
	return nil
}
