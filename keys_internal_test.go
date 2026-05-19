package redisemaphore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDerivedSemaphoreKeysShareClusterHashTag(t *testing.T) {
	namespace := "tenant{danger}:one"
	keys := []string{
		redisHolderKey(namespace),
		redisMutexKey(namespace),
		redisQueueKey(namespace, "high{danger}"),
		redisQueueKey(namespace, "low"),
	}

	wantTag := "{tenant%7Bdanger%7D:one}"
	for _, key := range keys {
		require.Contains(t, key, wantTag)
	}

	require.True(t, strings.Contains(keys[2], "queue:high%7Bdanger%7D"))
	require.NotContains(t, keys[2], "{danger}")
}
