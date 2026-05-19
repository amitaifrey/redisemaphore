package redisemaphore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisClusterSlot(t *testing.T) {
	require.Equal(t, 12739, redisClusterSlot("123456789"))
	require.Equal(t, redisClusterSlot("bar"), redisClusterSlot("foo{bar}zap"))
	require.Equal(t, redisClusterSlot("bar"), redisClusterSlot("{bar}"))
	require.NotEqual(t, redisClusterSlot("bar"), redisClusterSlot("foo{}{bar}"))
}

func TestDerivedSemaphoreKeysShareClusterSlot(t *testing.T) {
	namespace := "tenant{danger}:one"
	keys := []string{
		redisHolderKey(namespace),
		redisMutexKey(namespace),
		redisQueueKey(namespace, "high{danger}"),
		redisQueueKey(namespace, "low"),
	}

	slot := redisClusterSlot(keys[0])
	for _, key := range keys[1:] {
		require.Equal(t, slot, redisClusterSlot(key))
	}

	require.Contains(t, keys[0], "{tenant%7Bdanger%7D:one}")
	require.True(t, strings.Contains(keys[2], "queue:high%7Bdanger%7D"))
}
