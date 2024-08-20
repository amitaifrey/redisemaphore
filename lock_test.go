package redisemaphore_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/amitaifrey/redisemaphore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
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
