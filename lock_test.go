package redisemaphore_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/amitaifrey/redisemaphore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("could not start miniredis: %v", err)
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{mr.Addr()},
	})
	return mr, client
}

func TestNewMutex(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")

	require.NoError(t, err)
	assert.NotNil(t, mutex, "mutex should not be nil")
}

func TestNewMutex_InvalidConfig(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	tests := []struct {
		name      string
		client    redis.UniversalClient
		namespace string
		opts      []redisemaphore.MutexOption
	}{
		{name: "nil client", namespace: "test-lock"},
		{name: "empty namespace", client: client},
		{name: "bad expiry", client: client, namespace: "test-lock", opts: []redisemaphore.MutexOption{redisemaphore.WithMutexExpiry(0)}},
		{name: "bad timeout", client: client, namespace: "test-lock", opts: []redisemaphore.MutexOption{redisemaphore.WithMutexTimeout(0)}},
		{name: "bad poll", client: client, namespace: "test-lock", opts: []redisemaphore.MutexOption{redisemaphore.WithMutexPollDur(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutex, err := redisemaphore.NewMutex(tt.client, tt.namespace, tt.opts...)
			require.Nil(t, mutex)
			require.ErrorIs(t, err, redisemaphore.ErrInvalidConfig)
		})
	}
}

func TestMutex_Acquire_Success(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())

	assert.NoError(t, err, "expected no error acquiring the lock")

	assert.True(t, mr.Exists(testMutexKey("test-lock")), "lock key should exist in redis")
}

func TestMutex_Acquire_Timeout(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	// Simulate the lock being held by another client
	mr.Set(testMutexKey("test-lock"), "1")

	mutex, err := redisemaphore.NewMutex(client, "test-lock", redisemaphore.WithMutexTimeout(100*time.Millisecond))
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())

	assert.Equal(t, redisemaphore.ErrTimeout, err, "expected timeout error acquiring the lock")
}

func TestMutex_Acquire_ContextCancel(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	// Simulate the lock being held by another client
	mr.Set(testMutexKey("test-lock"), "1")

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel) // Cancel context after 100ms

	err = mutex.Acquire(ctx)

	assert.ErrorIs(t, err, context.Canceled, "expected context canceled error")
}

func TestMutex_Release_Success(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	// First, acquire the lock
	err = mutex.Acquire(context.Background())
	assert.NoError(t, err, "expected no error acquiring the lock")

	// Then, release the lock
	err = mutex.Release(context.Background())

	assert.NoError(t, err, "expected no error releasing the lock")
	assert.False(t, mr.Exists(testMutexKey("test-lock")), "lock key should not exist in redis")
}

func TestMutex_Release_Error(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	// No lock to release
	err = mutex.Release(context.Background())

	assert.NoError(t, err, "expected no error releasing a non-existent lock")
}

func TestMutex_ExpiredOwnerDoesNotReleaseNewOwner(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutexA, err := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(100*time.Millisecond),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)
	require.NoError(t, err)
	mutexB, err := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(time.Minute),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)
	require.NoError(t, err)

	err = mutexA.Acquire(context.Background())
	require.NoError(t, err)

	mr.FastForward(101 * time.Millisecond)

	err = mutexB.Acquire(context.Background())
	require.NoError(t, err)

	err = mutexA.Release(context.Background())
	require.NoError(t, err)
	require.True(t, mr.Exists(testMutexKey("test-lock")), "stale owner should not delete the newer owner's lock")

	err = mutexB.Release(context.Background())
	require.NoError(t, err)
	require.False(t, mr.Exists(testMutexKey("test-lock")), "new owner should still be able to release its lock")
}

func TestMutex_ReleaseCanRetryAfterContextCancel(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())
	require.NoError(t, err)

	releaseCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = mutex.Release(releaseCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, mr.Exists(testMutexKey("test-lock")), "failed release should leave the lock retryable")

	err = mutex.Release(context.Background())
	require.NoError(t, err)
	require.False(t, mr.Exists(testMutexKey("test-lock")))
}

func TestMutex_SameInstanceAcquireHonorsContext(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "test-lock")
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = mutex.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = mutex.Release(context.Background())
	require.NoError(t, err)
}

func TestMutex_SameInstanceAcquireHonorsMutexTimeout(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexTimeout(50*time.Millisecond),
	)
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())
	require.NoError(t, err)

	err = mutex.Acquire(context.Background())
	require.ErrorIs(t, err, redisemaphore.ErrTimeout)

	err = mutex.Release(context.Background())
	require.NoError(t, err)
}

func testMutexKey(namespace string) string {
	return fmt.Sprintf("redisemaphore:mutex:{%s}:lock", url.PathEscape(namespace))
}

func testHolderKey(namespace string) string {
	return testRedisKey(namespace, "holders")
}

func testQueueKey(namespace, queueID string) string {
	return testRedisKey(namespace, "queue:"+url.PathEscape(queueID))
}

func testRedisKey(namespace, suffix string) string {
	return fmt.Sprintf("redisemaphore:{%s}:%s", url.PathEscape(namespace), suffix)
}
