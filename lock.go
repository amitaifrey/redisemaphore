package redisemaphore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/go-errors/errors"
	"github.com/redis/go-redis/v9"
)

var ErrTimeout = errors.New("error: lock timeout")

var releaseMutexScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

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
	localLock   chan struct{}
	stateMu     sync.Mutex
	token       string
}

func NewMutex(redisClient redis.UniversalClient, name string, opts ...MutexOption) Mutex {
	m := &mutex{
		redisClient: redisClient,
		name:        name,
		expiry:      1 * time.Minute,
		timeout:     10 * time.Minute,
		pollDur:     100 * time.Millisecond,
		localLock:   make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt.Apply(m)
	}

	return m
}

func (this *mutex) Acquire(ctx context.Context) error {
	select {
	case this.localLock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	token, err := newMutexToken()
	if err != nil {
		<-this.localLock
		return err
	}

	timeout := time.After(this.timeout)
	for {
		r := this.redisClient.SetArgs(ctx, this.name, token, redis.SetArgs{
			Mode: "NX",
			TTL:  this.expiry,
		})
		if r.Err() != nil && r.Err() != redis.Nil {
			<-this.localLock
			return r.Err()
		}

		result, err := r.Result()
		if err != nil && err != redis.Nil {
			<-this.localLock
			return err
		}
		if result == "OK" {
			this.stateMu.Lock()
			this.token = token
			this.stateMu.Unlock()
			return nil
		}

		select {
		case <-ctx.Done():
			<-this.localLock
			return ctx.Err()
		case <-timeout:
			<-this.localLock
			return ErrTimeout
		case <-time.After(this.pollDur):
			continue
		}
	}
}

func (this *mutex) Release(ctx context.Context) error {
	this.stateMu.Lock()
	token := this.token
	if token == "" {
		this.stateMu.Unlock()
		return nil
	}
	this.token = ""
	this.stateMu.Unlock()

	defer func() {
		<-this.localLock
	}()

	return releaseMutexScript.Run(ctx, this.redisClient, []string{this.name}, token).Err()
}

func newMutexToken() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return hex.EncodeToString(token), nil
}
