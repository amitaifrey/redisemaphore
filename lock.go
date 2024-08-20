package redisemaphore

import (
	"context"
	"time"

	"github.com/go-errors/errors"
	"github.com/redis/go-redis/v9"
)

var ErrTimeout = errors.New("error: lock timeout")

type Mutex interface {
	Acquire(ctx context.Context) error
	Release(ctx context.Context) error
}

type MutexOption interface {
	Apply(*mutex)
}

type MutexOptionFunc func(*mutex)

func (f MutexOptionFunc) Apply(m *mutex) {
	f(m)
}

func WithMutexPollDur(pollDur time.Duration) MutexOption {
	return MutexOptionFunc(func(m *mutex) {
		m.pollDur = pollDur
	})
}

func WithMutexExpiry(expiry time.Duration) MutexOption {
	return MutexOptionFunc(func(m *mutex) {
		m.expiry = expiry
	})
}

func WithMutexTimeout(timeout time.Duration) MutexOption {
	return MutexOptionFunc(func(m *mutex) {
		m.timeout = timeout
	})
}

type mutex struct {
	redisClient redis.UniversalClient
	name        string
	expiry      time.Duration // when the lock expires so other can take it, for when the lock holder dies
	timeout     time.Duration // how long to wait for the lock to be released
	pollDur     time.Duration // how often to poll for the lock
}

func NewMutex(redisClient redis.UniversalClient, name string, opts ...MutexOption) Mutex {
	m := &mutex{
		redisClient: redisClient,
		name:        name,
		expiry:      1 * time.Minute,
		timeout:     10 * time.Minute,
		pollDur:     100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt.Apply(m)
	}

	return m
}

func (this *mutex) Acquire(ctx context.Context) error {
	timeout := time.After(this.timeout)
	for {
		r := this.redisClient.SetArgs(ctx, this.name, "1", redis.SetArgs{
			Mode: "NX",
			TTL:  this.expiry,
		})
		if r.Err() != nil && r.Err() != redis.Nil {
			return r.Err()
		}

		result, err := r.Result()
		if err != nil && err != redis.Nil {
			return err
		}
		if result == "OK" {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return ErrTimeout
		case <-time.After(this.pollDur):
			continue
		}
	}
}

func (this *mutex) Release(ctx context.Context) error {
	r := this.redisClient.Del(ctx, this.name)
	return r.Err()
}
