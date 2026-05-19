package redisemaphore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/amitaifrey/redisemaphore"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSemaphore(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 3)
	assert.NoError(t, err)
	assert.NotNil(t, semaphore)
}

func TestSemaphore_Acquire(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 3)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key1")
	require.NoError(t, err)

	intCmd := client.ZCard(ctx, "semaphore-queue")
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	intCmd = client.ZRank(ctx, "semaphore", "key1")
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	err = semaphore.Release(ctx, "key1")
	require.NoError(t, err)

	intCmd = client.ZCard(ctx, "semaphore")
	require.NoError(t, intCmd.Err())
	assert.Equal(t, int64(0), intCmd.Val())
}

func TestSemaphore_AcquireOrder(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
		redisemaphore.WithSemaphoreQueueKeysByPrio("queue1", "queue2", "queue3"),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = semaphore.AcquireQueue(ctx, "queue1", "init")
	require.NoError(t, err)

	acquired := make(chan string, 9)
	errs := make(chan error, 9)
	keys1, keys2, keys3 := make([]string, 3), make([]string, 3), make([]string, 3)
	for i := 0; i < 3; i++ {
		keys1[i] = uuid.NewString()
		go acquireAndRelease(ctx, semaphore, "queue1", keys1[i], acquired, errs)

		keys2[i] = uuid.NewString()
		go acquireAndRelease(ctx, semaphore, "queue2", keys2[i], acquired, errs)

		keys3[i] = uuid.NewString()
		go acquireAndRelease(ctx, semaphore, "queue3", keys3[i], acquired, errs)
	}

	waitForZCardSum(t, client, []string{"queue1", "queue2", "queue3"}, 9)

	err = semaphore.ReleaseQueue(context.Background(), "queue1", "init")
	require.NoError(t, err)

	keys := make([]string, 9)
	for i := 0; i < 9; i++ {
		keys[i] = receiveString(t, acquired)
	}
	for i := 0; i < 9; i++ {
		require.NoError(t, receiveError(t, errs))
	}

	assert.ElementsMatch(t, keys1, keys[0:3])
	assert.ElementsMatch(t, keys2, keys[3:6])
	assert.ElementsMatch(t, keys3, keys[6:9])
}

func TestSemaphore_Concurrent(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		100,
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
		redisemaphore.WithSemaphoreQueueKeysByPrio("queue1", "queue2", "queue3"),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := make(chan string, 200)
	errs := make(chan error, 100)
	releaseGate := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			k := uuid.NewString()
			if err := semaphore.AcquireQueue(ctx, "queue1", k); err != nil {
				errs <- err
				return
			}
			events <- "before"
			<-releaseGate

			if err := semaphore.ReleaseQueue(context.Background(), "queue1", k); err != nil {
				errs <- err
				return
			}
			events <- "after"
		}()
	}

	for i := 0; i < 100; i++ {
		require.Equal(t, "before", receiveStringOrError(t, events, errs))
	}
	close(releaseGate)
	for i := 0; i < 100; i++ {
		require.Equal(t, "after", receiveStringOrError(t, events, errs))
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestSemaphore_KeyExpiration(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	deleteTimeout := 2 * time.Second
	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithSemaphoreDeleteTimeout(deleteTimeout))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key1 := "key1"
	err = semaphore.Acquire(ctx, key1)
	require.NoError(t, err)

	intCmd := client.ZRank(ctx, "semaphore", key1)
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	key2 := "key2"
	err = semaphore.Acquire(ctx, key2)
	require.NoError(t, err)

	intCmd = client.ZRank(ctx, "semaphore", key2)
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	err = semaphore.Release(ctx, key2)
	require.NoError(t, err)
}

func TestSemaphore_HolderScoreUsesMicroseconds(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	before := float64(time.Now().Add(-time.Second).UnixMicro())
	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)
	after := float64(time.Now().Add(time.Second).UnixMicro())

	score := client.ZScore(context.Background(), "semaphore", "key")
	require.NoError(t, score.Err())
	require.GreaterOrEqual(t, score.Val(), before)
	require.LessOrEqual(t, score.Val(), after)
}

func TestSemaphore_WaiterScoreUsesMicroseconds(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "init")
	require.NoError(t, err)

	before := float64(time.Now().Add(-time.Second).UnixMicro())
	acquired := make(chan string, 1)
	errs := make(chan error, 1)
	go acquireAndRelease(ctx, semaphore, "semaphore-queue", "queued", acquired, errs)

	waitForZCard(t, client, "semaphore-queue", 1)
	after := float64(time.Now().Add(time.Second).UnixMicro())

	score := client.ZScore(context.Background(), "semaphore-queue", "queued")
	require.NoError(t, score.Err())
	require.GreaterOrEqual(t, score.Val(), before)
	require.LessOrEqual(t, score.Val(), after)

	err = semaphore.Release(context.Background(), "init")
	require.NoError(t, err)
	require.Equal(t, "queued", receiveString(t, acquired))
	require.NoError(t, receiveError(t, errs))
}

func TestSemaphore_DuplicateKeyWhileHeld(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithSemaphorePollDur(10*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)

	err = semaphore.Acquire(ctx, "key")
	require.Equal(t, redisemaphore.ErrDuplicateKey, err)

	queueSize := client.ZCard(context.Background(), "semaphore-queue")
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(0), queueSize.Val())

	holderSize := client.ZCard(context.Background(), "semaphore")
	require.NoError(t, holderSize.Err())
	require.Equal(t, int64(1), holderSize.Val())
}

func TestSemaphore_DuplicateKeyWhileQueued(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithSemaphorePollDur(10*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "init")
	require.NoError(t, err)

	acquired := make(chan string, 1)
	errs := make(chan error, 1)
	go acquireAndRelease(ctx, semaphore, "semaphore-queue", "queued-key", acquired, errs)

	waitForZCard(t, client, "semaphore-queue", 1)

	err = semaphore.Acquire(ctx, "queued-key")
	require.Equal(t, redisemaphore.ErrDuplicateKey, err)

	queueSize := client.ZCard(context.Background(), "semaphore-queue")
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(1), queueSize.Val())

	err = semaphore.Release(context.Background(), "init")
	require.NoError(t, err)

	require.Equal(t, "queued-key", receiveString(t, acquired))
	require.NoError(t, receiveError(t, errs))
}

func TestSemaphore_CanceledAcquireCleansWaiter(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithSemaphorePollDur(10*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "init")
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer waitCancel()

	err = semaphore.Acquire(waitCtx, "abandoned")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	queueSize := client.ZCard(context.Background(), "semaphore-queue")
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(0), queueSize.Val())

	rank := client.ZRank(context.Background(), "semaphore", "abandoned")
	require.Equal(t, redis.Nil, rank.Err())

	err = semaphore.Release(context.Background(), "init")
	require.NoError(t, err)

	freshCtx, freshCancel := context.WithTimeout(context.Background(), time.Second)
	defer freshCancel()

	err = semaphore.Acquire(freshCtx, "fresh")
	require.NoError(t, err)
	err = semaphore.Release(context.Background(), "fresh")
	require.NoError(t, err)
}

func TestSemaphore_SamePriorityOlderWaitersFirst(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
		redisemaphore.WithSemaphoreQueueKeysByPrio("queue"),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = semaphore.AcquireQueue(ctx, "queue", "init")
	require.NoError(t, err)

	acquired := make(chan string, 3)
	errs := make(chan error, 3)
	keys := []string{"c-key", "b-key", "a-key"}
	for i, key := range keys {
		go acquireAndRelease(ctx, semaphore, "queue", key, acquired, errs)
		waitForZCard(t, client, "queue", int64(i+1))
		time.Sleep(time.Millisecond)
	}

	err = semaphore.ReleaseQueue(context.Background(), "queue", "init")
	require.NoError(t, err)

	got := []string{receiveString(t, acquired), receiveString(t, acquired), receiveString(t, acquired)}
	for i := 0; i < 3; i++ {
		require.NoError(t, receiveError(t, errs))
	}

	require.Equal(t, keys, got)
}

func acquireAndRelease(ctx context.Context, semaphore redisemaphore.Semaphore, queue, key string, acquired chan<- string, errs chan<- error) {
	if err := semaphore.AcquireQueue(ctx, queue, key); err != nil {
		errs <- err
		return
	}
	acquired <- key
	errs <- semaphore.ReleaseQueue(context.Background(), queue, key)
}

func waitForZCard(t *testing.T, client redis.UniversalClient, key string, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		card := client.ZCard(context.Background(), key)
		return card.Err() == nil && card.Val() == want
	}, 5*time.Second, 10*time.Millisecond)
}

func waitForZCardSum(t *testing.T, client redis.UniversalClient, keys []string, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		var sum int64
		for _, key := range keys {
			card := client.ZCard(context.Background(), key)
			if card.Err() != nil {
				return false
			}
			sum += card.Val()
		}
		return sum == want
	}, 5*time.Second, 10*time.Millisecond)
}

func receiveString(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case val := <-ch:
		return val
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for string")
		return ""
	}
}

func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

func receiveStringOrError(t *testing.T, strings <-chan string, errs <-chan error) string {
	t.Helper()
	select {
	case val := <-strings:
		return val
	case err := <-errs:
		require.NoError(t, err)
		return ""
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
		return ""
	}
}
