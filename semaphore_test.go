package redisemaphore_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

func TestNewSemaphore_InvalidConfig(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	tests := []struct {
		name      string
		client    redis.UniversalClient
		namespace string
		size      int
		opts      []redisemaphore.SemaphoreOption
	}{
		{name: "nil client", namespace: "semaphore", size: 1},
		{name: "empty namespace", client: client, size: 1},
		{name: "bad size", client: client, namespace: "semaphore"},
		{name: "empty queues", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphoreQueuesByPriority()}},
		{name: "empty queue", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphoreQueuesByPriority("")}},
		{name: "duplicate queue", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphoreQueuesByPriority("queue", "queue")}},
		{name: "bad mutex expiry", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphoreMutexExpiry(0)}},
		{name: "bad mutex timeout", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphoreMutexTimeout(0)}},
		{name: "bad permit ttl", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphorePermitTTL(0)}},
		{name: "bad poll", client: client, namespace: "semaphore", size: 1, opts: []redisemaphore.SemaphoreOption{redisemaphore.WithSemaphorePollDur(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			semaphore, err := redisemaphore.NewSemaphore(tt.client, tt.namespace, tt.size, tt.opts...)
			require.Nil(t, semaphore)
			require.ErrorIs(t, err, redisemaphore.ErrInvalidConfig)
		})
	}
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

	intCmd := client.ZCard(ctx, testQueueKey("semaphore", "default"))
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	intCmd = client.ZRank(ctx, testHolderKey("semaphore"), "key1")
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	err = semaphore.Release(ctx, "key1")
	require.NoError(t, err)

	intCmd = client.ZCard(ctx, testHolderKey("semaphore"))
	require.NoError(t, intCmd.Err())
	assert.Equal(t, int64(0), intCmd.Val())
}

func TestSemaphore_ImmediateAdmissionDoesNotWaitForPoll(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphorePollDur(time.Hour),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)

	err = semaphore.Release(context.Background(), "key")
	require.NoError(t, err)
}

func TestSemaphore_StandaloneMutexDoesNotBlockSemaphore(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	mutex, err := redisemaphore.NewMutex(client, "same")
	require.NoError(t, err)
	err = mutex.Acquire(context.Background())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mutex.Release(context.Background()))
	}()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"same",
		1,
		redisemaphore.WithSemaphoreMutexTimeout(50*time.Millisecond),
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)
	err = semaphore.Release(context.Background(), "key")
	require.NoError(t, err)
}

func TestSemaphore_AcquireOrder(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
		redisemaphore.WithSemaphoreQueuesByPriority("queue1", "queue2", "queue3"),
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

	waitForZCardSum(t, client, []string{
		testQueueKey("semaphore", "queue1"),
		testQueueKey("semaphore", "queue2"),
		testQueueKey("semaphore", "queue3"),
	}, 9)

	err = semaphore.Release(context.Background(), "init")
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
		redisemaphore.WithSemaphoreQueuesByPriority("queue1", "queue2", "queue3"),
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

			if err := semaphore.Release(context.Background(), k); err != nil {
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

	permitTTL := time.Second
	start := time.Unix(1_700_000_000, 0)
	mr.SetTime(start)

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1, redisemaphore.WithSemaphorePermitTTL(permitTTL))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	key1 := "key1"
	err = semaphore.Acquire(ctx, key1)
	require.NoError(t, err)

	intCmd := client.ZRank(ctx, testHolderKey("semaphore"), key1)
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	mr.SetTime(start.Add(permitTTL + time.Microsecond))
	mr.FastForward(permitTTL + time.Microsecond)

	key2 := "key2"
	err = semaphore.Acquire(ctx, key2)
	require.NoError(t, err)

	intCmd = client.ZRank(ctx, testHolderKey("semaphore"), key2)
	require.NoError(t, intCmd.Err())
	require.Equal(t, int64(0), intCmd.Val())

	err = semaphore.Release(ctx, key2)
	require.NoError(t, err)
}

func TestSemaphore_HolderScoreUsesRedisTime(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	redisTime := time.Unix(1_700_000_000, 123_456_000)
	mr.SetTime(redisTime)

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)

	score := client.ZScore(context.Background(), testHolderKey("semaphore"), "key")
	require.NoError(t, score.Err())
	require.Equal(t, float64(redisTime.UnixMicro()), score.Val())
}

func TestSemaphore_WaiterScoreUsesRedisTime(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	holderTime := time.Unix(1_700_000_000, 0)
	waiterTime := time.Unix(1_700_000_010, 654_321_000)
	mr.SetTime(holderTime)

	semaphore, err := redisemaphore.NewSemaphore(client, "semaphore", 1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "init")
	require.NoError(t, err)

	mr.SetTime(waiterTime)

	acquired := make(chan string, 1)
	errs := make(chan error, 1)
	go acquireAndRelease(ctx, semaphore, "default", "queued", acquired, errs)

	waitForZCard(t, client, testQueueKey("semaphore", "default"), 1)

	score := client.ZScore(context.Background(), testQueueKey("semaphore", "default"), "queued")
	require.NoError(t, score.Err())
	require.Equal(t, float64(waiterTime.UnixMicro()), score.Val())

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

	queueSize := client.ZCard(context.Background(), testQueueKey("semaphore", "default"))
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(0), queueSize.Val())

	holderSize := client.ZCard(context.Background(), testHolderKey("semaphore"))
	require.NoError(t, holderSize.Err())
	require.Equal(t, int64(1), holderSize.Val())
}

func TestSemaphore_AcquireQueueRejectsUnknownQueue(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphoreQueuesByPriority("queue"),
	)
	require.NoError(t, err)

	err = semaphore.AcquireQueue(context.Background(), "unknown-queue", "key")
	require.ErrorIs(t, err, redisemaphore.ErrInvalidConfig)
}

func TestSemaphore_QueuesByPriorityCopiesInput(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	queues := []string{"queue"}
	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphoreQueuesByPriority(queues...),
	)
	require.NoError(t, err)

	queues[0] = "mutated"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.NoError(t, err)
	err = semaphore.Release(context.Background(), "key")
	require.NoError(t, err)
}

func TestSemaphore_ConfigMismatchRejectsAcquire(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	baseSize := 2
	baseOpts := []redisemaphore.SemaphoreOption{
		redisemaphore.WithSemaphoreQueuesByPriority("high", "low"),
		redisemaphore.WithSemaphorePermitTTL(30 * time.Second),
		redisemaphore.WithSemaphoreMutexExpiry(15 * time.Second),
	}

	tests := []struct {
		name string
		size int
		opts []redisemaphore.SemaphoreOption
	}{
		{
			name: "size",
			size: 3,
			opts: baseOpts,
		},
		{
			name: "queue order",
			size: baseSize,
			opts: []redisemaphore.SemaphoreOption{
				redisemaphore.WithSemaphoreQueuesByPriority("low", "high"),
				redisemaphore.WithSemaphorePermitTTL(30 * time.Second),
				redisemaphore.WithSemaphoreMutexExpiry(15 * time.Second),
			},
		},
		{
			name: "permit ttl",
			size: baseSize,
			opts: []redisemaphore.SemaphoreOption{
				redisemaphore.WithSemaphoreQueuesByPriority("high", "low"),
				redisemaphore.WithSemaphorePermitTTL(time.Minute),
				redisemaphore.WithSemaphoreMutexExpiry(15 * time.Second),
			},
		},
		{
			name: "mutex expiry",
			size: baseSize,
			opts: []redisemaphore.SemaphoreOption{
				redisemaphore.WithSemaphoreQueuesByPriority("high", "low"),
				redisemaphore.WithSemaphorePermitTTL(30 * time.Second),
				redisemaphore.WithSemaphoreMutexExpiry(time.Minute),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace := "config-" + strings.ReplaceAll(tt.name, " ", "-")
			base, err := redisemaphore.NewSemaphore(client, namespace, baseSize, baseOpts...)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err = base.AcquireQueue(ctx, "high", "seed")
			require.NoError(t, err)
			err = base.Release(context.Background(), "seed")
			require.NoError(t, err)

			other, err := redisemaphore.NewSemaphore(client, namespace, tt.size, tt.opts...)
			require.NoError(t, err)

			err = other.AcquireQueue(ctx, "high", "other")
			require.ErrorIs(t, err, redisemaphore.ErrInvalidConfig)
		})
	}
}

func TestSemaphore_ReleaseIgnoresConfigMismatch(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	base, err := redisemaphore.NewSemaphore(client, "config-release", 1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = base.Acquire(ctx, "held")
	require.NoError(t, err)

	other, err := redisemaphore.NewSemaphore(client, "config-release", 2)
	require.NoError(t, err)

	err = other.Release(context.Background(), "held")
	require.NoError(t, err)

	holderSize := client.ZCard(context.Background(), testHolderKey("config-release"))
	require.NoError(t, holderSize.Err())
	require.Equal(t, int64(0), holderSize.Val())
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
	go acquireAndRelease(ctx, semaphore, "default", "queued-key", acquired, errs)

	waitForZCard(t, client, testQueueKey("semaphore", "default"), 1)

	err = semaphore.Acquire(ctx, "queued-key")
	require.Equal(t, redisemaphore.ErrDuplicateKey, err)

	queueSize := client.ZCard(context.Background(), testQueueKey("semaphore", "default"))
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(1), queueSize.Val())

	err = semaphore.Release(context.Background(), "init")
	require.NoError(t, err)

	require.Equal(t, "queued-key", receiveString(t, acquired))
	require.NoError(t, receiveError(t, errs))
}

func TestSemaphore_QueueIDCannotCollideWithHolderKey(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
		redisemaphore.WithSemaphoreQueuesByPriority("holders"),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.AcquireQueue(ctx, "holders", "first")
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer waitCancel()

	err = semaphore.AcquireQueue(waitCtx, "holders", "second")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	holderSize := client.ZCard(context.Background(), testHolderKey("semaphore"))
	require.NoError(t, holderSize.Err())
	require.Equal(t, int64(1), holderSize.Val())

	err = semaphore.Release(context.Background(), "first")
	require.NoError(t, err)
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

	queueSize := client.ZCard(context.Background(), testQueueKey("semaphore", "default"))
	require.NoError(t, queueSize.Err())
	require.Equal(t, int64(0), queueSize.Val())

	rank := client.ZRank(context.Background(), testHolderKey("semaphore"), "abandoned")
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
		redisemaphore.WithSemaphoreQueuesByPriority("queue"),
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
		waitForZCard(t, client, testQueueKey("semaphore", "queue"), int64(i+1))
		time.Sleep(time.Millisecond)
	}

	err = semaphore.Release(context.Background(), "init")
	require.NoError(t, err)

	got := []string{receiveString(t, acquired), receiveString(t, acquired), receiveString(t, acquired)}
	for i := 0; i < 3; i++ {
		require.NoError(t, receiveError(t, errs))
	}

	require.Equal(t, keys, got)
}

func TestSemaphore_AcquireQueueReturnsReleaseErrorAfterAdmission(t *testing.T) {
	mr, client := setupRedis(t)
	defer mr.Close()

	hook := &failReleaseAfterPromotionHook{}
	client.AddHook(hook)

	semaphore, err := redisemaphore.NewSemaphore(
		client,
		"semaphore",
		1,
		redisemaphore.WithSemaphoreMutexTimeout(20*time.Millisecond),
		redisemaphore.WithSemaphorePollDur(10*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = semaphore.Acquire(ctx, "key")
	require.ErrorIs(t, err, errForcedMutexRelease)
	require.True(t, hook.armed.Load())
}

func acquireAndRelease(ctx context.Context, semaphore *redisemaphore.Semaphore, queue, key string, acquired chan<- string, errs chan<- error) {
	if err := semaphore.AcquireQueue(ctx, queue, key); err != nil {
		errs <- err
		return
	}
	acquired <- key
	errs <- semaphore.Release(context.Background(), key)
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

var errForcedMutexRelease = errors.New("forced mutex release failure")

type failReleaseAfterPromotionHook struct {
	armed atomic.Bool
}

func (h *failReleaseAfterPromotionHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *failReleaseAfterPromotionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.armed.Load() && scriptNumKeys(cmd) == 1 {
			return errForcedMutexRelease
		}

		err := next(ctx, cmd)
		if err == nil && scriptNumKeys(cmd) == 2 {
			h.armed.Store(true)
		}
		return err
	}
}

func (h *failReleaseAfterPromotionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func scriptNumKeys(cmd redis.Cmder) int {
	args := cmd.Args()
	if len(args) < 3 {
		return -1
	}

	name := strings.ToLower(fmt.Sprint(args[0]))
	if name != "eval" && name != "evalsha" {
		return -1
	}

	numKeys, err := strconv.Atoi(fmt.Sprint(args[2]))
	if err != nil {
		return -1
	}
	return numKeys
}
