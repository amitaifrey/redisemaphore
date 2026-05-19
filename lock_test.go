package redisemaphore_test

import (
	"context"
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

	mutex := redisemaphore.NewMutex(client, "test-lock")

	assert.NotNil(t, mutex, "mutex should not be nil")
}

func TestMutex_Acquire_Success(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	err := mutex.Acquire(context.Background())

	assert.NoError(t, err, "expected no error acquiring the lock")

	// Ensure the key is set in miniredis
	assert.True(t, mr.Exists("test-lock"), "lock key should exist in redis")
}

func TestMutex_Acquire_Timeout(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	// Simulate the lock being held by another client
	mr.Set("test-lock", "1")

	mutex := redisemaphore.NewMutex(client, "test-lock", redisemaphore.WithMutexTimeout(100*time.Millisecond))

	err := mutex.Acquire(context.Background())

	assert.Equal(t, redisemaphore.ErrTimeout, err, "expected timeout error acquiring the lock")
}

func TestMutex_Acquire_ContextCancel(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	// Simulate the lock being held by another client
	mr.Set("test-lock", "1")

	mutex := redisemaphore.NewMutex(client, "test-lock")

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel) // Cancel context after 100ms

	err := mutex.Acquire(ctx)

	assert.ErrorIs(t, err, context.Canceled, "expected context canceled error")
}

func TestMutex_Release_Success(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	// First, acquire the lock
	err := mutex.Acquire(context.Background())
	assert.NoError(t, err, "expected no error acquiring the lock")

	// Then, release the lock
	err = mutex.Release(context.Background())

	assert.NoError(t, err, "expected no error releasing the lock")
	assert.False(t, mr.Exists("test-lock"), "lock key should not exist in redis")
}

func TestMutex_Release_Error(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	// No lock to release
	err := mutex.Release(context.Background())

	assert.NoError(t, err, "expected no error releasing a non-existent lock")
}

func TestMutex_ExpiredOwnerDoesNotReleaseNewOwner(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutexA := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(100*time.Millisecond),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)
	mutexB := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(time.Minute),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)

	err := mutexA.Acquire(context.Background())
	require.NoError(t, err)

	mr.FastForward(101 * time.Millisecond)

	err = mutexB.Acquire(context.Background())
	require.NoError(t, err)

	err = mutexA.Release(context.Background())
	require.NoError(t, err)
	require.True(t, mr.Exists("test-lock"), "stale owner should not delete the newer owner's lock")

	err = mutexB.Release(context.Background())
	require.NoError(t, err)
	require.False(t, mr.Exists("test-lock"), "new owner should still be able to release its lock")
}

func TestMutex_AcquireWithTokenUsesCallerToken(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	err := mutex.AcquireWithToken(context.Background(), "queue=q key=k nonce=1")
	require.NoError(t, err)
	lockValue, err := mr.Get("test-lock")
	require.NoError(t, err)
	require.Equal(t, "queue=q key=k nonce=1", lockValue)

	err = mutex.Release(context.Background())
	require.NoError(t, err)
	require.False(t, mr.Exists("test-lock"))
}

func TestMutex_AcquireWithTokenRejectsEmptyToken(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	err := mutex.AcquireWithToken(context.Background(), "")
	require.Equal(t, redisemaphore.ErrEmptyMutexToken, err)
	require.False(t, mr.Exists("test-lock"))
}

func TestMutex_SameTrackingPrefixDifferentNonceDoesNotReleaseNewOwner(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutexA := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(100*time.Millisecond),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)
	mutexB := redisemaphore.NewMutex(
		client,
		"test-lock",
		redisemaphore.WithMutexExpiry(time.Minute),
		redisemaphore.WithMutexPollDur(10*time.Millisecond),
	)

	err := mutexA.AcquireWithToken(context.Background(), "queue=q key=k nonce=1")
	require.NoError(t, err)

	mr.FastForward(101 * time.Millisecond)

	err = mutexB.AcquireWithToken(context.Background(), "queue=q key=k nonce=2")
	require.NoError(t, err)

	err = mutexA.Release(context.Background())
	require.NoError(t, err)
	lockValue, err := mr.Get("test-lock")
	require.NoError(t, err)
	require.Equal(t, "queue=q key=k nonce=2", lockValue)

	err = mutexB.Release(context.Background())
	require.NoError(t, err)
	require.False(t, mr.Exists("test-lock"))
}

func TestMutex_SameInstanceAcquireHonorsContext(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex := redisemaphore.NewMutex(client, "test-lock")

	err := mutex.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = mutex.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = mutex.Release(context.Background())
	require.NoError(t, err)
}
