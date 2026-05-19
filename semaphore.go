package redisemaphore

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/go-errors/errors"
	"github.com/redis/go-redis/v9"
)

var errNoKeysLeft = errors.New("error: no keys left in queues")
var ErrDuplicateKey = errors.New("error: duplicate key")

var insertNextScript = redis.NewScript(`
if redis.call("zscore", KEYS[2], ARGV[2]) == false then
	return 0
end
local added = redis.call("zadd", KEYS[1], "NX", ARGV[1], ARGV[2])
if added == 0 then
	return -1
end
redis.call("zrem", KEYS[2], ARGV[2])
return 1
`)

type SemaphoreOption interface {
	Apply(*Semaphore)
}

type SemaphoreOptionFunc func(*Semaphore)

func (f SemaphoreOptionFunc) Apply(s *Semaphore) {
	f(s)
}

func WithSemaphoreMutexExpiry(mutexExpiry time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *Semaphore) {
		s.mutexExpiry = mutexExpiry
	})
}

func WithSemaphoreMutexTimeout(mutexTimeout time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *Semaphore) {
		s.mutexTimeout = mutexTimeout
	})
}

func WithSemaphorePermitTTL(permitTTL time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *Semaphore) {
		s.permitTTL = permitTTL
	})
}

func WithSemaphorePollDur(pollDur time.Duration) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *Semaphore) {
		s.pollDur = pollDur
	})
}

func WithSemaphoreQueuesByPriority(queueIDs ...string) SemaphoreOption {
	return SemaphoreOptionFunc(func(s *Semaphore) {
		s.queueIDsByPriority = queueIDs
	})
}

type Semaphore struct {
	redisClient        redis.UniversalClient
	mutex              *Mutex
	namespace          string
	size               int
	holderKey          string
	mutexKey           string
	mutexExpiry        time.Duration
	mutexTimeout       time.Duration
	permitTTL          time.Duration
	pollDur            time.Duration
	queueIDsByPriority []string
	queueKeysByID      map[string]string
	queueKeysByPrio    []string
}

func NewSemaphore(redisClient redis.UniversalClient, namespace string, size int, opts ...SemaphoreOption) (*Semaphore, error) {
	s := &Semaphore{
		redisClient:        redisClient,
		namespace:          namespace,
		size:               size,
		mutexExpiry:        time.Minute,
		mutexTimeout:       10 * time.Minute,
		permitTTL:          10 * time.Minute,
		pollDur:            100 * time.Millisecond,
		queueIDsByPriority: []string{"default"},
	}

	for _, opt := range opts {
		opt.Apply(s)
	}

	if err := s.validateAndBuildKeys(); err != nil {
		return nil, err
	}

	mutex, err := newMutexWithKey(
		redisClient,
		s.mutexKey,
		WithMutexExpiry(s.mutexExpiry),
		WithMutexTimeout(s.mutexTimeout),
		WithMutexPollDur(s.pollDur),
	)
	if err != nil {
		return nil, err
	}
	s.mutex = mutex

	return s, nil
}

func (s *Semaphore) validateAndBuildKeys() error {
	if isNilRedisClient(s.redisClient) {
		return invalidConfig("redis client must not be nil")
	}
	if s.namespace == "" {
		return invalidConfig("semaphore namespace must not be empty")
	}
	if s.size <= 0 {
		return invalidConfig("semaphore size must be positive")
	}
	if s.mutexExpiry <= 0 {
		return invalidConfig("semaphore mutex expiry must be positive")
	}
	if s.mutexTimeout <= 0 {
		return invalidConfig("semaphore mutex timeout must be positive")
	}
	if s.permitTTL <= 0 {
		return invalidConfig("semaphore permit TTL must be positive")
	}
	if s.pollDur <= 0 {
		return invalidConfig("semaphore poll duration must be positive")
	}
	if len(s.queueIDsByPriority) == 0 {
		return invalidConfig("at least one semaphore queue must be configured")
	}

	s.holderKey = redisHolderKey(s.namespace)
	s.mutexKey = redisMutexKey(s.namespace)
	s.queueKeysByID = make(map[string]string, len(s.queueIDsByPriority))
	s.queueKeysByPrio = make([]string, 0, len(s.queueIDsByPriority))

	seenQueueIDs := make(map[string]struct{}, len(s.queueIDsByPriority))
	seenRedisKeys := map[string]string{
		s.holderKey: "holders",
		s.mutexKey:  "mutex",
	}

	for _, queueID := range s.queueIDsByPriority {
		if queueID == "" {
			return invalidConfig("semaphore queue id must not be empty")
		}
		if _, exists := seenQueueIDs[queueID]; exists {
			return invalidConfig("duplicate semaphore queue id %q", queueID)
		}
		seenQueueIDs[queueID] = struct{}{}

		queueKey := redisQueueKey(s.namespace, queueID)
		if owner, exists := seenRedisKeys[queueKey]; exists {
			return invalidConfig("derived queue key for %q collides with %s key", queueID, owner)
		}

		seenRedisKeys[queueKey] = fmt.Sprintf("queue %q", queueID)
		s.queueKeysByID[queueID] = queueKey
		s.queueKeysByPrio = append(s.queueKeysByPrio, queueKey)
	}

	return nil
}

func (s *Semaphore) Acquire(ctx context.Context, key string) error {
	return s.AcquireQueue(ctx, s.queueIDsByPriority[0], key)
}

func (s *Semaphore) AcquireQueue(ctx context.Context, queueID, key string) (err error) {
	queueKey, err := s.queueKey(queueID)
	if err != nil {
		return err
	}
	if key == "" {
		return invalidConfig("semaphore key must not be empty")
	}

	registered := false
	acquired := false
	defer func() {
		if !registered || acquired {
			return
		}
		cleanupErr := s.withCleanupContext(ctx, func(ctx context.Context) error {
			return s.cleanupWaiter(ctx, key)
		})
		if err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()

	if err = s.registerWaiter(ctx, queueKey, key); err != nil {
		return err
	}
	registered = true

	for {
		exists, err := s.keyExists(ctx, s.holderKey, key)
		if err != nil {
			return errors.WrapPrefix(err, "failed to check if key exists", 0)
		}
		if exists {
			acquired = true
			return nil
		}

		admitted, err := s.tryInsertNext(ctx, queueID, queueKey, key)
		if err != nil {
			return err
		}
		if admitted {
			acquired = true
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pollDur):
			continue
		}
	}
}

func (s *Semaphore) tryInsertNext(ctx context.Context, queueID, queueKey, key string) (bool, error) {
	admitted := false
	err := s.withMutex(ctx, s.mutexTokenDescription("fill", queueID, key), func(ctx context.Context) error {
		toAdd, err := s.amountToAdd(ctx)
		if err != nil || toAdd <= 0 {
			return err
		}

		for toAdd > 0 {
			nextQueueKey, nextKey, err := s.getNextKey(ctx)
			if err == errNoKeysLeft {
				return nil
			}
			if err != nil {
				return errors.WrapPrefix(err, "failed to get next key", 0)
			}

			inserted, err := s.insertNext(ctx, nextQueueKey, nextKey)
			if err != nil {
				return errors.WrapPrefix(err, "failed to insert next key", 0)
			}
			if !inserted {
				continue
			}
			if nextQueueKey == queueKey && nextKey == key {
				admitted = true
			}

			toAdd--
		}

		return nil
	})
	return admitted, err
}

func (s *Semaphore) registerWaiter(ctx context.Context, queueKey, key string) error {
	return s.withMutex(ctx, s.mutexTokenDescription("register", queueKey, key), func(ctx context.Context) error {
		if err := s.cleanupExpiredHolders(ctx); err != nil {
			return err
		}

		exists, err := s.keyExists(ctx, s.holderKey, key)
		if err != nil {
			return errors.WrapPrefix(err, "failed to check if key exists in semaphore", 0)
		}
		if exists {
			return ErrDuplicateKey
		}

		for _, queueKey := range s.queueKeysByPrio {
			exists, err := s.keyExists(ctx, queueKey, key)
			if err != nil {
				return errors.WrapPrefix(err, fmt.Sprintf("failed to check if key exists in queue: %s", queueKey), 0)
			}
			if exists {
				return ErrDuplicateKey
			}
		}

		addCmd := s.redisClient.ZAddArgs(ctx, queueKey, redis.ZAddArgs{
			NX: true,
			Members: []redis.Z{
				{Score: float64(time.Now().UnixMicro()), Member: key},
			},
		})
		if addCmd.Err() != nil && addCmd.Err() != redis.Nil {
			return errors.WrapPrefix(addCmd.Err(), "failed to push key", 0)
		}
		if addCmd.Val() == 0 {
			return ErrDuplicateKey
		}

		return nil
	})
}

func (s *Semaphore) withMutex(ctx context.Context, tokenDescription string, fn func(context.Context) error) (err error) {
	err = s.mutex.acquireWithDescription(ctx, tokenDescription)
	if err != nil {
		return errors.WrapPrefix(err, "failed to acquire mutex", 0)
	}
	defer func() {
		releaseErr := s.withCleanupContext(ctx, s.mutex.Release)
		if releaseErr == nil {
			return
		}

		releaseErr = errors.WrapPrefix(releaseErr, "failed to release mutex", 0)
		if err == nil {
			err = releaseErr
			return
		}
		err = stderrors.Join(err, releaseErr)
	}()

	return fn(ctx)
}

func (s *Semaphore) getNextKey(ctx context.Context) (string, string, error) {
	for _, queueKey := range s.queueKeysByPrio {
		lenCmd := s.redisClient.ZCard(ctx, queueKey)
		if lenCmd.Err() != nil {
			return "", "", errors.WrapPrefix(lenCmd.Err(), fmt.Sprintf("failed to get length of queue: %s", queueKey), 0)
		}
		if lenCmd.Val() == 0 {
			continue
		}

		readKeyCmd := s.redisClient.ZRange(ctx, queueKey, 0, 0)
		if readKeyCmd.Err() != nil {
			return "", "", errors.WrapPrefix(readKeyCmd.Err(), fmt.Sprintf("failed to get key: %s", readKeyCmd), 0)
		}
		if len(readKeyCmd.Val()) == 1 {
			return queueKey, readKeyCmd.Val()[0], nil
		}
	}

	return "", "", errNoKeysLeft
}

func (s *Semaphore) amountToAdd(ctx context.Context) (int, error) {
	if err := s.cleanupExpiredHolders(ctx); err != nil {
		return -1, err
	}

	zcard := s.redisClient.ZCard(ctx, s.holderKey)
	if zcard.Err() != nil {
		return -1, errors.WrapPrefix(zcard.Err(), "failed to get length of semaphore", 0)
	}
	return s.size - int(zcard.Val()), nil
}

func (s *Semaphore) cleanupExpiredHolders(ctx context.Context) error {
	cutoff := fmt.Sprintf("%d", time.Now().Add(-s.permitTTL).UnixMicro())
	r1 := s.redisClient.ZRemRangeByScore(ctx, s.holderKey, "-inf", cutoff)
	if r1.Err() != nil {
		return errors.WrapPrefix(r1.Err(), "failed to clean up semaphore", 0)
	}
	return nil
}

func (s *Semaphore) cleanupWaiter(ctx context.Context, key string) error {
	return s.withMutex(ctx, s.mutexTokenDescription("cleanup-waiter", "", key), func(ctx context.Context) error {
		for _, queueKey := range s.queueKeysByPrio {
			if err := s.redisClient.ZRem(ctx, queueKey, key).Err(); err != nil && err != redis.Nil {
				return errors.WrapPrefix(err, "failed to remove key from queue", 0)
			}
		}
		if err := s.redisClient.ZRem(ctx, s.holderKey, key).Err(); err != nil && err != redis.Nil {
			return errors.WrapPrefix(err, "failed to remove key from semaphore", 0)
		}
		return nil
	})
}

func (s *Semaphore) keyExists(ctx context.Context, set, key string) (bool, error) {
	rankCmd := s.redisClient.ZRank(ctx, set, key)
	if rankCmd.Err() == nil {
		return true, nil
	}
	if rankCmd.Err() == redis.Nil {
		return false, nil
	}
	return false, rankCmd.Err()
}

func (s *Semaphore) withCleanupContext(ctx context.Context, fn func(context.Context) error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.mutexTimeout)
	defer cancel()
	return fn(cleanupCtx)
}

func (s *Semaphore) mutexTokenDescription(action, queueID, key string) string {
	return fmt.Sprintf("semaphore=%s action=%s queue=%s key=%s", s.namespace, action, queueID, key)
}

func (s *Semaphore) insertNext(ctx context.Context, queueKey, key string) (bool, error) {
	score := fmt.Sprintf("%d", time.Now().UnixMicro())
	result, err := insertNextScript.Run(ctx, s.redisClient, []string{s.holderKey, queueKey}, score, key).Int()
	if err != nil {
		return false, errors.WrapPrefix(err, "failed to move key from queue to semaphore", 0)
	}
	switch result {
	case 1:
		return true, nil
	case 0:
		return false, nil
	case -1:
		return false, ErrDuplicateKey
	default:
		return false, fmt.Errorf("unexpected insert result: %d", result)
	}
}

func (s *Semaphore) Release(ctx context.Context, key string) error {
	if key == "" {
		return invalidConfig("semaphore key must not be empty")
	}

	err := s.redisClient.ZRem(ctx, s.holderKey, key).Err()
	if err == nil || err == redis.Nil {
		return nil
	}
	return errors.WrapPrefix(err, "failed to remove key", 0)
}

func (s *Semaphore) queueKey(queueID string) (string, error) {
	if queueID == "" {
		return "", invalidConfig("semaphore queue id must not be empty")
	}
	queueKey, ok := s.queueKeysByID[queueID]
	if !ok {
		return "", invalidConfig("unknown semaphore queue %q", queueID)
	}
	return queueKey, nil
}
