package redisemaphore

import (
	"fmt"
	"net/url"
)

const redisKeyPrefix = "redisemaphore"

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}

func redisStandaloneMutexKey(namespace string) string {
	return fmt.Sprintf("%s:mutex:{%s}:lock", redisKeyPrefix, escapeRedisKeyPart(namespace))
}

func redisSemaphoreMutexKey(namespace string) string {
	return redisSemaphoreKey(namespace, "mutex")
}

func redisSemaphoreConfigKey(namespace string) string {
	return redisSemaphoreKey(namespace, "config")
}

func redisSemaphoreHolderKey(namespace string) string {
	return redisSemaphoreKey(namespace, "holders")
}

func redisSemaphoreQueueKey(namespace, queueID string) string {
	return redisSemaphoreKey(namespace, "queue:"+escapeRedisKeyPart(queueID))
}

func redisSemaphoreKey(namespace, suffix string) string {
	// Redis Cluster hashes only the substring inside {...}. Keep every key for
	// a semaphore namespace on that same escaped tag so multi-key Lua scripts
	// can move waiters between queues and holders without CROSSSLOT errors.
	// Escaping keeps user-provided braces from changing that hash tag.
	return fmt.Sprintf("%s:{%s}:%s", redisKeyPrefix, escapeRedisKeyPart(namespace), suffix)
}

func escapeRedisKeyPart(part string) string {
	return url.PathEscape(part)
}
