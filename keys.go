package redisemaphore

import (
	"fmt"
	"net/url"
)

const redisKeyPrefix = "redisemaphore"

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}

func redisMutexKey(namespace string) string {
	return redisKey(namespace, "mutex")
}

func redisHolderKey(namespace string) string {
	return redisKey(namespace, "holders")
}

func redisQueueKey(namespace, queueID string) string {
	return redisKey(namespace, "queue:"+escapeRedisKeyPart(queueID))
}

func redisKey(namespace, suffix string) string {
	// Redis Cluster hashes only the substring inside {...}. Keep every key for
	// a semaphore namespace on that same escaped tag so multi-key Lua scripts
	// can move waiters between queues and holders without CROSSSLOT errors.
	// Escaping keeps user-provided braces from changing that hash tag.
	return fmt.Sprintf("%s:{%s}:%s", redisKeyPrefix, escapeRedisKeyPart(namespace), suffix)
}

func escapeRedisKeyPart(part string) string {
	return url.PathEscape(part)
}
