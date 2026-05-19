package redisemaphore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/go-errors/errors"
	"github.com/redis/go-redis/v9"
)

var ErrInvalidConfig = errors.New("error: invalid config")
var ErrTimeout = errors.New("error: lock timeout")

var errEmptyMutexToken = errors.New("error: empty mutex token")

// Check and delete in one Redis operation so an expired owner cannot release a
// lock that a newer owner has already acquired.
var releaseMutexScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

type MutexOption interface {
	Apply(*Mutex)
}

type MutexOptionFunc func(*Mutex)

func (f MutexOptionFunc) Apply(m *Mutex) {
	f(m)
}

func WithMutexPollDur(pollDur time.Duration) MutexOption {
	return MutexOptionFunc(func(m *Mutex) {
		m.pollDur = pollDur
	})
}

func WithMutexExpiry(expiry time.Duration) MutexOption {
	return MutexOptionFunc(func(m *Mutex) {
		m.expiry = expiry
	})
}

func WithMutexTimeout(timeout time.Duration) MutexOption {
	return MutexOptionFunc(func(m *Mutex) {
		m.timeout = timeout
	})
}

type Mutex struct {
	redisClient redis.UniversalClient
	key         string
	expiry      time.Duration
	timeout     time.Duration
	pollDur     time.Duration

	// localLock gates one active Acquire/Release lifecycle for this instance.
	localLock chan struct{}
	// stateMu protects token so concurrent Release calls collapse to one script run.
	stateMu sync.Mutex
	// token is the Redis owner value for the active lock; Release has no token argument.
	token string
}

func NewMutex(redisClient redis.UniversalClient, namespace string, opts ...MutexOption) (*Mutex, error) {
	if namespace == "" {
		return nil, invalidConfig("mutex namespace must not be empty")
	}
	return newMutexWithKey(redisClient, redisMutexKey(namespace), opts...)
}

func newMutexWithKey(redisClient redis.UniversalClient, key string, opts ...MutexOption) (*Mutex, error) {
	m := &Mutex{
		redisClient: redisClient,
		key:         key,
		expiry:      time.Minute,
		timeout:     10 * time.Minute,
		pollDur:     100 * time.Millisecond,
		localLock:   make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt.Apply(m)
	}

	if err := m.validate(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Mutex) validate() error {
	if m.redisClient == nil {
		return invalidConfig("redis client must not be nil")
	}
	if m.key == "" {
		return invalidConfig("mutex key must not be empty")
	}
	if m.expiry <= 0 {
		return invalidConfig("mutex expiry must be positive")
	}
	if m.timeout <= 0 {
		return invalidConfig("mutex timeout must be positive")
	}
	if m.pollDur <= 0 {
		return invalidConfig("mutex poll duration must be positive")
	}
	return nil
}

func (m *Mutex) Acquire(ctx context.Context) error {
	return m.acquireWithDescription(ctx, "")
}

func (m *Mutex) acquireWithDescription(ctx context.Context, description string) error {
	token, err := newMutexToken(description)
	if err != nil {
		return err
	}
	return m.acquireWithToken(ctx, token)
}

func (m *Mutex) acquireWithToken(ctx context.Context, token string) error {
	if token == "" {
		return errEmptyMutexToken
	}

	select {
	case m.localLock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	timeout := time.After(m.timeout)
	for {
		r := m.redisClient.SetArgs(ctx, m.key, token, redis.SetArgs{
			Mode: "NX",
			TTL:  m.expiry,
		})
		if r.Err() != nil && r.Err() != redis.Nil {
			<-m.localLock
			return r.Err()
		}

		result, err := r.Result()
		if err != nil && err != redis.Nil {
			<-m.localLock
			return err
		}
		if result == "OK" {
			m.stateMu.Lock()
			m.token = token
			m.stateMu.Unlock()
			return nil
		}

		select {
		case <-ctx.Done():
			<-m.localLock
			return ctx.Err()
		case <-timeout:
			<-m.localLock
			return ErrTimeout
		case <-time.After(m.pollDur):
			continue
		}
	}
}

func (m *Mutex) Release(ctx context.Context) error {
	m.stateMu.Lock()
	token := m.token
	if token == "" {
		m.stateMu.Unlock()
		return nil
	}

	if err := releaseMutexScript.Run(ctx, m.redisClient, []string{m.key}, token).Err(); err != nil {
		m.stateMu.Unlock()
		return err
	}

	m.token = ""
	m.stateMu.Unlock()

	// Release the local lifecycle gate only after the Redis release succeeds.
	<-m.localLock
	return nil
}

// newMutexToken adds a random nonce to the optional description. The description
// is for Redis inspection; the nonce is the uniqueness/safety part.
func newMutexToken(description string) (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	if description == "" {
		return hex.EncodeToString(nonce), nil
	}
	return fmt.Sprintf("%s nonce=%s", description, hex.EncodeToString(nonce)), nil
}
