package redisemaphore_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/amitaifrey/redisemaphore"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSemaphore(t *testing.T) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Connect to the mock Redis server
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Test with valid inputs
	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 3)
	assert.NoError(t, err)
	assert.NotNil(t, semaphore)
}

func TestSemaphore_Acquire(t *testing.T) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Connect to the mock Redis server
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 3)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test acquiring a key
	err = semaphore.Acquire(ctx, "semaphore-queue", "key1")
	require.NoError(t, err)

	// Check that the key was removed from the queue
	intCmd := client.ZCard(ctx, "semaphore-queue")
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	// Check if the key exists in the semaphore set
	intCmd = client.ZRank(ctx, "semaphore", "key1")
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	// cleanup
	err = semaphore.Release(ctx, "semaphore-queue", "key1")
	require.NoError(t, err)

	// Check if the key exists in the semaphore set
	intCmd = client.ZCard(ctx, "semaphore")
	require.NoError(t, intCmd.Err())
	assert.Equal(t, int64(0), intCmd.Val())
}

func TestSemaphore_AcquireOrder(t *testing.T) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Connect to the mock Redis server
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithQueueKeysByPrio("queue1", "queue2", "queue3"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acquireAndRelease := func(queue, key string, c chan string) {
		err := semaphore.Acquire(ctx, queue, key)
		require.NoError(t, err)
		c <- key

		err = semaphore.Release(ctx, queue, key)
		require.NoError(t, err)
	}

	// acquire so the semaphore won't start
	err = semaphore.Acquire(ctx, "queue1", "init")
	require.NoError(t, err)

	// Initialize channels and keys
	c := make(chan string, 9)
	keys1, keys2, keys3 := make([]string, 3), make([]string, 3), make([]string, 3)
	for i := 0; i < 3; i++ {
		keys1[i] = uuid.NewString()
		go acquireAndRelease("queue1", keys1[i], c)

		keys2[i] = uuid.NewString()
		go acquireAndRelease("queue2", keys2[i], c)

		keys3[i] = uuid.NewString()
		go acquireAndRelease("queue3", keys3[i], c)
	}

	for {
		if mr.Exists("queue1") && mr.Exists("queue2") && mr.Exists("queue3") {
			n1, err := mr.ZMembers("queue1")
			require.NoError(t, err)
			n2, err := mr.ZMembers("queue2")
			require.NoError(t, err)
			n3, err := mr.ZMembers("queue3")
			require.NoError(t, err)

			if len(n1)+len(n2)+len(n3) == 9 {
				break
			}
		}
		time.Sleep(time.Millisecond * 100)
	}

	// release the init key
	err = semaphore.Release(ctx, "queue1", "init")
	require.NoError(t, err)

	keys := make([]string, 9)
	for i := 0; i < 9; i++ {
		keys[i] = <-c
	}

	assert.ElementsMatch(t, keys[0:3], keys1)
	assert.ElementsMatch(t, keys[3:6], keys2)
	assert.ElementsMatch(t, keys[6:9], keys3)
}

func TestSemaphore_Concurrent(t *testing.T) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Connect to the mock Redis server
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 100, redisemaphore.WithQueueKeysByPrio("queue1", "queue2", "queue3"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := make(chan string, 200)

	for i := 0; i < 100; i++ {
		go func() {
			k := uuid.NewString()
			err := semaphore.Acquire(ctx, "queue1", k)
			require.NoError(t, err)
			c <- "before"
			time.Sleep(time.Second * 10)

			err = semaphore.Release(ctx, "queue1", k)
			require.NoError(t, err)
			c <- "after"
		}()
	}

	for i := 0; i < 100; i++ {
		out := <-c
		require.Equal(t, "before", out)
	}

	for i := 0; i < 100; i++ {
		out := <-c
		require.Equal(t, "after", out)
	}
}
